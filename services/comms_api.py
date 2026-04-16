#!/usr/bin/env python3
"""
IMM-OS Comms API — port 8005
Handles:
  - Messages (send with delay, inbox, thread, read-receipt)
  - Journals (role-gated: author + flight_surgeon)
  - Daily Briefings (ECLSS auto-populate, ack tracking)
  - Web Push subscriptions + dispatch
  - Video / Audio log management
"""
import os
import json
import asyncio
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Optional, List
from enum import Enum

import asyncpg
import httpx
from fastapi import FastAPI, HTTPException, UploadFile, File, Form, Query, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

# ── Logging (IST) ─────────────────────────────────────────────────
ist_tz = timezone(timedelta(hours=5, minutes=30))
logging.Formatter.converter = lambda *args: datetime.now(ist_tz).timetuple()
logging.basicConfig(level=logging.INFO,
    format="[IST %(asctime)s] [comms_api] %(message)s")
log = logging.getLogger(__name__)

app = FastAPI(title="IMM-OS Comms API", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_headers=["*"], allow_methods=["*"])

# ── Config ────────────────────────────────────────────────────────
PG = dict(
    user=os.getenv("POSTGRES_USER", "admin"),
    password=os.getenv("POSTGRES_PASSWORD", "changeme"),
    database=os.getenv("POSTGRES_DB", "imm_db"),
    host=os.getenv("POSTGRES_HOST", "postgres"),
)
TIME_SVC = os.getenv("TIME_SERVICE_URL", "http://time-service:8002")
ECLSS_SVC = os.getenv("ECLSS_API_URL", "http://eclss-api:8003")
MEDIA_DIR = os.getenv("MEDIA_DIR", "/app/media")
os.makedirs(MEDIA_DIR, exist_ok=True)

DELAY_MAP = {"none": 0, "moon": 1.28, "mars": 480}   # seconds (scaled for demo; real = 480 s / 8 min)

async def get_conn():
    return await asyncpg.connect(**PG)

# ── Helpers ───────────────────────────────────────────────────────

async def current_delay_seconds() -> float:
    """Fetch active comm delay from time-service."""
    try:
        async with httpx.AsyncClient(timeout=2) as client:
            r = await client.get(f"{TIME_SVC}/api/v1/time/delay")
            d = r.json()
            mode = d.get("mode", "none")
            if mode == "custom":
                return float(d.get("value", 0))
            return float(DELAY_MAP.get(mode, 0))
    except Exception:
        return 0.0

async def current_mission_day() -> int:
    try:
        async with httpx.AsyncClient(timeout=2) as client:
            r = await client.get(f"{TIME_SVC}/api/v1/time/now")
            d = r.json()
            ts = float(d.get("unix_ts", time.time()))
            return max(1, int((ts - 1710000000) / 86400))
    except Exception:
        return 1

async def eclss_snapshot() -> dict:
    """Fetch latest lighting state as a proxy for ECLSS live summary."""
    try:
        async with httpx.AsyncClient(timeout=2) as client:
            r = await client.get(f"{ECLSS_SVC}/api/v1/eclss/lighting")
            return {"lighting": r.json(), "fetched_at": datetime.now(ist_tz).isoformat()}
    except Exception:
        return {"error": "ECLSS unavailable"}

# ── Background delivery worker ────────────────────────────────────

async def delivery_worker():
    """Polls every 5 s; marks messages as delivered once deliver_at has passed."""
    while True:
        try:
            conn = await asyncpg.connect(**PG)
            now = datetime.now(timezone.utc)
            await conn.execute(
                "UPDATE messages SET delivered=TRUE WHERE delivered=FALSE AND deliver_at <= $1",
                now
            )
            await conn.close()
        except Exception as e:
            log.warning(f"Delivery worker error: {e}")
        await asyncio.sleep(5)

@app.on_event("startup")
async def startup():
    asyncio.create_task(delivery_worker())

# ══════════════════════════════════════════════════════════════════
#  MESSAGES
# ══════════════════════════════════════════════════════════════════

class MessageSend(BaseModel):
    sender_id: str
    recipient_group: str          # astro | mcc | all
    subject: Optional[str] = None
    body: str
    thread_id: Optional[int] = None

@app.post("/api/v1/comms/message", status_code=201)
async def send_message(msg: MessageSend):
    if msg.recipient_group not in ("astro", "mcc", "all"):
        raise HTTPException(422, "recipient_group must be astro, mcc, or all")

    # Zero delay for astro↔astro; time-service delay for astro→mcc
    if msg.sender_id.startswith("astro") and msg.recipient_group == "mcc":
        delay_s = await current_delay_seconds()
    else:
        delay_s = 0.0

    mission_day = await current_mission_day()
    deliver_at = datetime.now(timezone.utc) + timedelta(seconds=delay_s)

    conn = await get_conn()
    try:
        # Auto-create thread if not provided
        thread_id = msg.thread_id
        if not thread_id:
            row = await conn.fetchrow(
                "INSERT INTO threads (subject, created_by) VALUES ($1, $2) RETURNING id",
                msg.subject or "(no subject)", msg.sender_id
            )
            thread_id = row["id"]

        row = await conn.fetchrow(
            """
            INSERT INTO messages
                (thread_id, sender_id, recipient_group, subject, body,
                 delay_seconds, deliver_at, mission_day)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
            RETURNING id
            """,
            thread_id, msg.sender_id, msg.recipient_group,
            msg.subject, msg.body, delay_s, deliver_at, mission_day
        )
        log.info(f"Message {row['id']} queued; delay={delay_s}s deliver_at={deliver_at.isoformat()}")
        return {
            "message_id": row["id"],
            "thread_id": thread_id,
            "delay_seconds": delay_s,
            "deliver_at": deliver_at.isoformat(),
        }
    finally:
        await conn.close()

@app.get("/api/v1/comms/inbox/{user_id}")
async def get_inbox(user_id: str, group: str = Query("all")):
    conn = await get_conn()
    try:
        rows = await conn.fetch(
            """
            SELECT m.*, t.subject as thread_subject
            FROM messages m
            JOIN threads t ON m.thread_id = t.id
            WHERE m.delivered = TRUE
              AND (m.recipient_group = $1 OR m.recipient_group = 'all')
            ORDER BY m.mission_day DESC, m.deliver_at DESC
            LIMIT 100
            """,
            group
        )
        # Mark as read
        await conn.execute(
            """
            UPDATE messages
            SET read_by = array_append(read_by, $1)
            WHERE delivered=TRUE
              AND NOT ($1 = ANY(read_by))
              AND (recipient_group = $2 OR recipient_group = 'all')
            """,
            user_id, group
        )
        return [dict(r) for r in rows]
    finally:
        await conn.close()

@app.get("/api/v1/comms/thread/{thread_id}")
async def get_thread(thread_id: int):
    conn = await get_conn()
    try:
        rows = await conn.fetch(
            "SELECT * FROM messages WHERE thread_id=$1 ORDER BY sent_at",
            thread_id
        )
        return [dict(r) for r in rows]
    finally:
        await conn.close()

@app.get("/api/v1/comms/pending/{sender_id}")
async def pending_messages(sender_id: str):
    """Returns outbox items not yet delivered — for countdown display."""
    conn = await get_conn()
    try:
        rows = await conn.fetch(
            "SELECT id, subject, deliver_at, delay_seconds FROM messages WHERE sender_id=$1 AND delivered=FALSE ORDER BY sent_at",
            sender_id
        )
        now = datetime.now(timezone.utc)
        result = []
        for r in rows:
            remaining = max(0.0, (r["deliver_at"] - now).total_seconds())
            result.append({**dict(r), "remaining_seconds": round(remaining, 1)})
        return result
    finally:
        await conn.close()

# ══════════════════════════════════════════════════════════════════
#  JOURNAL
# ══════════════════════════════════════════════════════════════════

class JournalCreate(BaseModel):
    author_id: str
    title: Optional[str] = None
    body: Optional[str] = None
    media_type: str = "text"    # text | voice | video
    tags: List[str] = []

@app.post("/api/v1/journal/entry", status_code=201)
async def create_journal(entry: JournalCreate):
    mission_day = await current_mission_day()
    conn = await get_conn()
    try:
        row = await conn.fetchrow(
            """
            INSERT INTO journals (author_id, title, body, media_type, mission_day, tags)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING id
            """,
            entry.author_id, entry.title, entry.body,
            entry.media_type, mission_day, entry.tags
        )
        return {"journal_id": row["id"], "mission_day": mission_day}
    finally:
        await conn.close()

@app.get("/api/v1/journal/entries/{author_id}")
async def get_journals(author_id: str, requester_role: str = Query("crew")):
    """Role-gated: only author or flight_surgeon role can view entries."""
    conn = await get_conn()
    try:
        rows = await conn.fetch(
            "SELECT * FROM journals WHERE author_id=$1 ORDER BY mission_day DESC, created_at DESC",
            author_id
        )
        # In production, requester_role comes from Keycloak JWT claim
        if requester_role not in ("flight_surgeon", "author"):
            # Return redacted version for non-privileged crew
            return [{"id": r["id"], "mission_day": r["mission_day"],
                     "title": r["title"], "body": "[PRIVATE]"} for r in rows]
        return [dict(r) for r in rows]
    finally:
        await conn.close()

@app.get("/api/v1/journal/search")
async def search_journals(author_id: str, keyword: str, requester_role: str = Query("crew")):
    if requester_role not in ("flight_surgeon", "author"):
        raise HTTPException(403, "Access denied")
    conn = await get_conn()
    try:
        rows = await conn.fetch(
            "SELECT * FROM journals WHERE author_id=$1 AND body ILIKE $2 ORDER BY created_at DESC",
            author_id, f"%{keyword}%"
        )
        return [dict(r) for r in rows]
    finally:
        await conn.close()

@app.post("/api/v1/journal/upload/{journal_id}")
async def upload_journal_media(journal_id: int, file: UploadFile = File(...)):
    safe_name = f"journal_{journal_id}_{file.filename}"
    path = os.path.join(MEDIA_DIR, safe_name)
    with open(path, "wb") as f:
        f.write(await file.read())
    conn = await get_conn()
    try:
        await conn.execute("UPDATE journals SET media_path=$1 WHERE id=$2", path, journal_id)
    finally:
        await conn.close()
    return {"stored": safe_name}

# ══════════════════════════════════════════════════════════════════
#  BRIEFINGS
# ══════════════════════════════════════════════════════════════════

class BriefingCreate(BaseModel):
    created_by: str
    objectives: str
    eva_summary: Optional[str] = None
    assignments: List[dict] = []   # [{crew_id, task}]

@app.post("/api/v1/briefing/create", status_code=201)
async def create_briefing(req: BriefingCreate):
    mission_day = await current_mission_day()
    snap = await eclss_snapshot()
    conn = await get_conn()
    try:
        row = await conn.fetchrow(
            """
            INSERT INTO briefings
                (created_by, mission_day, objectives, eclss_snapshot, eva_summary, assignments)
            VALUES ($1,$2,$3,$4,$5,$6)
            RETURNING id
            """,
            req.created_by, mission_day, req.objectives,
            json.dumps(snap), req.eva_summary, json.dumps(req.assignments)
        )
        log.info(f"Briefing {row['id']} created for Mission Day {mission_day}")
        return {"briefing_id": row["id"], "mission_day": mission_day, "eclss_snapshot": snap}
    finally:
        await conn.close()

@app.get("/api/v1/briefing/{briefing_id}")
async def get_briefing(briefing_id: int):
    conn = await get_conn()
    try:
        row = await conn.fetchrow("SELECT * FROM briefings WHERE id=$1", briefing_id)
        if not row:
            raise HTTPException(404, "Briefing not found")
        acks = await conn.fetch(
            "SELECT crew_id, item_index, acked_at FROM briefing_acks WHERE briefing_id=$1",
            briefing_id
        )
        data = dict(row)
        data["acks"] = [dict(a) for a in acks]
        return data
    finally:
        await conn.close()

@app.get("/api/v1/briefing/latest/today")
async def latest_briefing():
    day = await current_mission_day()
    conn = await get_conn()
    try:
        row = await conn.fetchrow(
            "SELECT * FROM briefings WHERE mission_day=$1 ORDER BY created_at DESC LIMIT 1", day
        )
        if not row:
            raise HTTPException(404, "No briefing for today")
        return dict(row)
    finally:
        await conn.close()

class AckItem(BaseModel):
    crew_id: str
    item_index: int

@app.post("/api/v1/briefing/{briefing_id}/ack", status_code=201)
async def ack_briefing_item(briefing_id: int, req: AckItem):
    conn = await get_conn()
    try:
        await conn.execute(
            """
            INSERT INTO briefing_acks (briefing_id, crew_id, item_index)
            VALUES ($1,$2,$3)
            ON CONFLICT (briefing_id, crew_id, item_index) DO NOTHING
            """,
            briefing_id, req.crew_id, req.item_index
        )
        # Progress summary
        total = await conn.fetchval(
            "SELECT jsonb_array_length(assignments) FROM briefings WHERE id=$1", briefing_id
        ) or 0
        acked = await conn.fetchval(
            "SELECT COUNT(DISTINCT item_index) FROM briefing_acks WHERE briefing_id=$1", briefing_id
        )
        return {"acked": acked, "total": total, "pct": round(100 * acked / total) if total else 0}
    finally:
        await conn.close()

# ══════════════════════════════════════════════════════════════════
#  PUSH NOTIFICATIONS (Web Push)
# ══════════════════════════════════════════════════════════════════

class PushSubscribe(BaseModel):
    user_id: str
    endpoint: str
    p256dh: str
    auth: str

@app.post("/api/v1/push/subscribe", status_code=201)
async def subscribe_push(req: PushSubscribe):
    conn = await get_conn()
    try:
        await conn.execute(
            """
            INSERT INTO push_subscriptions (user_id, endpoint, p256dh, auth)
            VALUES ($1,$2,$3,$4)
            ON CONFLICT (endpoint) DO UPDATE SET user_id=$1, p256dh=$3, auth=$4
            """,
            req.user_id, req.endpoint, req.p256dh, req.auth
        )
        return {"subscribed": True}
    finally:
        await conn.close()

@app.post("/api/v1/push/send")
async def send_push_notification(user_id: str, title: str, body: str):
    """
    Dispatches a Web Push notification.
    In production, uses pywebpush with VAPID keys.
    Here we log the intent and return subscription count.
    """
    conn = await get_conn()
    try:
        subs = await conn.fetch(
            "SELECT endpoint FROM push_subscriptions WHERE user_id=$1", user_id
        )
        log.info(f"PUSH → {user_id}: [{title}] {body} ({len(subs)} endpoints)")
        # TODO: pywebpush.webpush(...) for each sub with VAPID credentials
        return {"dispatched_to": len(subs), "user_id": user_id}
    finally:
        await conn.close()

# ══════════════════════════════════════════════════════════════════
#  VIDEO / AUDIO LOGS
# ══════════════════════════════════════════════════════════════════

@app.post("/api/v1/videolog/upload", status_code=201)
async def upload_video_log(
    crew_id: str = Form(...),
    title: str = Form(""),
    keywords: str = Form(""),
    duration_seconds: float = Form(0),
    file: UploadFile = File(...)
):
    mission_day = await current_mission_day()
    safe_name = f"vlog_{crew_id}_{int(time.time())}_{file.filename}"
    path = os.path.join(MEDIA_DIR, safe_name)
    with open(path, "wb") as f:
        f.write(await file.read())

    kw_list = [k.strip() for k in keywords.split(",") if k.strip()]
    conn = await get_conn()
    try:
        row = await conn.fetchrow(
            """
            INSERT INTO video_logs
                (crew_id, title, media_path, mime_type, mission_day, duration_seconds, keywords)
            VALUES ($1,$2,$3,$4,$5,$6,$7)
            RETURNING id
            """,
            crew_id, title, path, file.content_type or "video/mp4",
            mission_day, duration_seconds, kw_list
        )
        return {"video_log_id": row["id"], "mission_day": mission_day, "path": safe_name}
    finally:
        await conn.close()

@app.get("/api/v1/videolog")
async def list_video_logs(crew_id: Optional[str] = None, mission_day: Optional[int] = None,
                          keyword: Optional[str] = None):
    conn = await get_conn()
    try:
        q = "SELECT id, crew_id, title, mission_day, duration_seconds, keywords, recorded_at FROM video_logs WHERE TRUE"
        params: list = []
        if crew_id:
            params.append(crew_id); q += f" AND crew_id=${len(params)}"
        if mission_day:
            params.append(mission_day); q += f" AND mission_day=${len(params)}"
        if keyword:
            params.append(keyword); q += f" AND ${ len(params)} = ANY(keywords)"
        q += " ORDER BY recorded_at DESC LIMIT 50"
        rows = await conn.fetch(q, *params)
        return [dict(r) for r in rows]
    finally:
        await conn.close()

@app.get("/api/v1/videolog/stream/{log_id}")
async def stream_video(log_id: int):
    conn = await get_conn()
    try:
        row = await conn.fetchrow("SELECT * FROM video_logs WHERE id=$1", log_id)
        if not row or not os.path.exists(row["media_path"]):
            raise HTTPException(404, "Video not found")
        def iterfile():
            with open(row["media_path"], "rb") as f:
                yield from f
        return StreamingResponse(iterfile(), media_type=row["mime_type"])
    finally:
        await conn.close()

@app.get("/health")
def health():
    return {"status": "ok"}

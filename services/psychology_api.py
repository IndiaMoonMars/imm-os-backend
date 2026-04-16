"""
IMM-OS Psychology & Sociodynamics API — Phase 10
Port: 8008  Prefix: /api/v1/psych
Features:
  - Sleep log (manual + actigraphy webhook: Garmin/Fitbit compatible)
  - Mood check-in (twice daily, 5-point scale) + IST notification scheduling
  - Psychological surveys: PANAS, IES-R, Loneliness Scale (auto-scored)
  - Sociogram peer ratings (privacy-enforced: crew never sees own ratings)
  - Sleep deprivation alert: <6h for ≥3 consecutive nights → flight surgeon push
  - 30-day trend endpoints for Flight Surgeon dashboard
"""
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List
import asyncpg, os, json, httpx
from datetime import datetime, date, timezone, timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler

app = FastAPI(title="IMM Psychology API", version="1.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

DB_URL = os.getenv("DATABASE_URL", "postgresql://imm_user:imm_pass@postgres:5432/imm_db")
COMMS_URL = os.getenv("COMMS_URL", "http://comms-api:8005")
IST = timezone(timedelta(hours=5, seconds=1800))

pool = None

# ─── Psych survey definitions ─────────────────────────────────────────────────
PANAS = {
    "name": "PANAS",
    "description": "Positive and Negative Affect Schedule — weekly affect measurement",
    "schedule_days": 7,
    "questions": [
        {"id": f"p{i}", "text": word, "scale_min": 1, "scale_max": 5, "subscale": sub}
        for i, (word, sub) in enumerate([
            ("Interested", "positive"), ("Distressed", "negative"), ("Excited", "positive"),
            ("Upset", "negative"), ("Strong", "positive"), ("Guilty", "negative"),
            ("Scared", "negative"), ("Hostile", "negative"), ("Enthusiastic", "positive"),
            ("Proud", "positive"), ("Irritable", "negative"), ("Alert", "positive"),
            ("Ashamed", "negative"), ("Inspired", "positive"), ("Nervous", "negative"),
            ("Determined", "positive"), ("Attentive", "positive"), ("Jittery", "negative"),
            ("Active", "positive"), ("Afraid", "negative"),
        ], 1)
    ],
    "scoring_rules": {"method": "panas_split", "flag_negative_above": 30}
}
IES_R = {
    "name": "IES_R",
    "description": "Impact of Event Scale-Revised — post-traumatic stress indicators",
    "schedule_days": 14,
    "questions": [
        {"id": f"e{i}", "text": t, "scale_min": 0, "scale_max": 4, "subscale": sub}
        for i, (t, sub) in enumerate([
            ("Any reminder brought back feelings about the mission stressors", "intrusion"),
            ("I had trouble staying asleep", "hyperarousal"),
            ("Other things kept making me think about it", "intrusion"),
            ("I felt irritable and angry", "hyperarousal"),
            ("I avoided letting myself get upset when I thought about it", "avoidance"),
            ("I thought about it when I didn't mean to", "intrusion"),
            ("I felt as if it hadn't happened or wasn't real", "avoidance"),
            ("I stayed away from reminders of it", "avoidance"),
            ("Pictures about it popped into my mind", "intrusion"),
            ("I was jumpy and easily startled", "hyperarousal"),
            ("I tried not to think about it", "avoidance"),
            ("I was aware that I still had a lot of feelings about it", "intrusion"),
            ("My feelings about it were kind of numb", "avoidance"),
            ("I found myself acting or feeling like I was back at that difficult time", "intrusion"),
            ("I had trouble falling asleep", "hyperarousal"),
            ("I had waves of strong feelings about it", "intrusion"),
            ("I tried to remove it from my memory", "avoidance"),
            ("I had trouble concentrating", "hyperarousal"),
            ("Reminders of it caused me to have physical reactions", "hyperarousal"),
            ("I had dreams about it", "intrusion"),
            ("I felt watchful and on guard", "hyperarousal"),
            ("I tried not to talk about it", "avoidance"),
        ], 1)
    ],
    "scoring_rules": {"method": "sum", "flag_above": 33}
}
LONELINESS = {
    "name": "LONELINESS",
    "description": "UCLA Loneliness Scale (10-item short form)",
    "schedule_days": 7,
    "questions": [
        {"id": f"l{i}", "text": t, "scale_min": 1, "scale_max": 4, "subscale": "loneliness"}
        for i, t in enumerate([
            "How often do you feel that you lack companionship?",
            "How often do you feel left out?",
            "How often do you feel isolated from others?",
            "How often do you feel that there are people you can talk to?",
            "How often do you feel that there are people you can turn to?",
            "How often do you feel alone?",
            "How often do you feel part of a group of friends?",
            "How often do you feel that you have a lot in common with others around you?",
            "How often do you feel outgoing and friendly?",
            "How often do you feel close to people?",
        ], 1)
    ],
    "scoring_rules": {"method": "sum", "flag_above": 20}
}

async def seed_psych_surveys():
    async with pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM psych_survey_templates")
        if count == 0:
            for tmpl in [PANAS, IES_R, LONELINESS]:
                await conn.execute(
                    "INSERT INTO psych_survey_templates(name,description,schedule_days,questions,scoring_rules) VALUES($1,$2,$3,$4,$5)",
                    tmpl["name"], tmpl["description"], tmpl["schedule_days"],
                    json.dumps(tmpl["questions"]), json.dumps(tmpl["scoring_rules"])
                )

@app.on_event("startup")
async def startup():
    global pool
    pool = await asyncpg.create_pool(DB_URL, min_size=2, max_size=10)
    await seed_psych_surveys()
    scheduler = AsyncIOScheduler(timezone="Asia/Kolkata")
    # Morning mood push at 07:00 IST
    scheduler.add_job(mood_push_notification, "cron", hour=7, minute=0, args=["morning"])
    # Evening mood push at 21:00 IST
    scheduler.add_job(mood_push_notification, "cron", hour=21, minute=0, args=["evening"])
    # Sleep deprivation check nightly at 06:30 IST
    scheduler.add_job(check_sleep_deprivation, "cron", hour=6, minute=30)
    # Weekly survey reminders (Mon 08:00)
    scheduler.add_job(send_survey_reminders, "cron", day_of_week="mon", hour=8, minute=0)
    scheduler.start()

def mission_day() -> int:
    epoch = datetime(2026, 1, 1, tzinfo=IST)
    return (datetime.now(IST) - epoch).days + 1

# ─── Scheduled jobs ───────────────────────────────────────────────────────────
async def mood_push_notification(period: str):
    """Broadcast push notification to all crew to complete mood check-in."""
    try:
        async with httpx.AsyncClient() as client:
            await client.post(f"{COMMS_URL}/api/v1/comms/message", json={
                "sender_id": "psych-system",
                "recipient_group": "astro",
                "subject": f"{'🌅 Morning' if period == 'morning' else '🌙 Evening'} Mood Check-In",
                "body": f"Please complete your {period} mood check-in in the IMM-OS Psych tab. This takes only 30 seconds."
            }, timeout=5)
    except Exception:
        pass

async def check_sleep_deprivation():
    """3 consecutive nights with <6h sleep → alert flight surgeon."""
    async with pool.acquire() as conn:
        crew = await conn.fetch("SELECT DISTINCT crew_id FROM sleep_log")
        for c in crew:
            cid = c["crew_id"]
            last3 = await conn.fetch(
                "SELECT duration_min FROM sleep_log WHERE crew_id=$1 ORDER BY mission_day DESC LIMIT 3", cid
            )
            if len(last3) == 3 and all(r["duration_min"] < 360 for r in last3):
                hours = [round(r["duration_min"]/60, 1) for r in last3]
                try:
                    async with httpx.AsyncClient() as client:
                        await client.post(f"{COMMS_URL}/api/v1/comms/message", json={
                            "sender_id": "psych-system",
                            "recipient_group": "mcc",
                            "subject": f"⚠️ Sleep Deprivation Alert — {cid}",
                            "body": f"Crew member {cid} has slept <6 hours for 3 consecutive nights.\n"
                                    f"Sleep durations: {hours[2]}h, {hours[1]}h, {hours[0]}h\n"
                                    f"Recommendation: Review duty schedule, prescribe forced rest period."
                        }, timeout=10)
                except Exception:
                    pass

async def send_survey_reminders():
    async with pool.acquire() as conn:
        templates = await conn.fetch("SELECT id, name, schedule_days FROM psych_survey_templates")
        crew = await conn.fetch("SELECT DISTINCT crew_id FROM mood_checkins")
        today_day = mission_day()
        for tmpl in templates:
            if today_day % tmpl["schedule_days"] == 0:
                try:
                    async with httpx.AsyncClient() as client:
                        await client.post(f"{COMMS_URL}/api/v1/comms/message", json={
                            "sender_id": "psych-system",
                            "recipient_group": "astro",
                            "subject": f"📋 Weekly Survey: {tmpl['name']}",
                            "body": f"Your {tmpl['name']} psychological survey is due today. Please complete it in the IMM-OS Psych tab."
                        }, timeout=5)
                except Exception:
                    pass

# ─── Pydantic Models ──────────────────────────────────────────────────────────
class SleepIn(BaseModel):
    crew_id: str
    sleep_onset: str          # ISO datetime
    wake_time: str            # ISO datetime
    quality_score: Optional[int] = None
    source: str = "manual"
    awakenings: int = 0
    rem_min: int = 0
    deep_min: int = 0
    hr_avg: Optional[int] = None

class MoodIn(BaseModel):
    crew_id: str
    period: str = "morning"   # morning / evening
    score: int                # 1–5
    note: Optional[str] = None

class SurveyResponseIn(BaseModel):
    crew_id: str
    template_id: int
    responses: dict

class SociogramIn(BaseModel):
    rater_id: str
    ratee_id: str
    comfort_score: int        # 1–5

# ─── HEALTH CHECK ─────────────────────────────────────────────────────────────
@app.get("/health")
async def health(): return {"status": "ok", "service": "psych-api"}

# ─── SLEEP LOG ────────────────────────────────────────────────────────────────
@app.post("/api/v1/psych/sleep")
async def log_sleep(body: SleepIn):
    onset = datetime.fromisoformat(body.sleep_onset)
    wake  = datetime.fromisoformat(body.wake_time)
    duration_min = max(0, int((wake - onset).total_seconds() / 60))
    async with pool.acquire() as conn:
        sid = await conn.fetchval(
            """INSERT INTO sleep_log(crew_id, sleep_onset, wake_time, duration_min, quality_score,
               source, awakenings, rem_min, deep_min, hr_avg, mission_day)
               VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING id""",
            body.crew_id, onset, wake, duration_min, body.quality_score,
            body.source, body.awakenings, body.rem_min, body.deep_min, body.hr_avg, mission_day()
        )
    return {"sleep_id": sid, "duration_min": duration_min, "duration_h": round(duration_min/60, 2)}

@app.get("/api/v1/psych/sleep/{crew_id}")
async def get_sleep(crew_id: str, days: int = 30):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM sleep_log WHERE crew_id=$1 ORDER BY mission_day DESC LIMIT $2",
            crew_id, days
        )
        avg_dur = await conn.fetchval(
            "SELECT AVG(duration_min) FROM sleep_log WHERE crew_id=$1 ORDER BY mission_day DESC LIMIT 7", crew_id
        )
    return {
        "entries": [dict(r) for r in rows],
        "7day_avg_hours": round(float(avg_dur or 0) / 60, 2)
    }

# Garmin/Fitbit webhook-compatible ingest
@app.post("/api/v1/psych/sleep/webhook")
async def sleep_webhook(payload: dict):
    """Accept Garmin Connect IQ or Fitbit sleep webhook. Parse and store."""
    # Fitbit format: payload["sleep"][0]
    # Garmin format: payload["sleeps"][0]
    try:
        if "sleep" in payload:
            s = payload["sleep"][0]
            onset = s.get("startTime")
            wake  = s.get("endTime")
            dur   = s.get("minutesAsleep", 0)
            crew_id = payload.get("crew_id", "unknown")
        elif "sleeps" in payload:
            s = payload["sleeps"][0]
            onset = s.get("startTimeInSeconds")
            wake  = s.get("endTimeInSeconds")
            dur   = int((int(wake or 0) - int(onset or 0)) / 60) if onset and wake else 0
            crew_id = payload.get("crew_id", "unknown")
        else:
            return {"status": "unrecognised_format"}
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO sleep_log(crew_id, duration_min, source, mission_day) VALUES($1,$2,$3,$4)",
                crew_id, dur, "webhook", mission_day()
            )
        return {"status": "stored", "duration_min": dur}
    except Exception as e:
        return {"status": "error", "detail": str(e)}

# ─── MOOD CHECK-IN ────────────────────────────────────────────────────────────
@app.post("/api/v1/psych/mood")
async def log_mood(body: MoodIn):
    if body.score < 1 or body.score > 5:
        raise HTTPException(400, "Score must be 1–5")
    async with pool.acquire() as conn:
        mid = await conn.fetchval(
            "INSERT INTO mood_checkins(crew_id, period, score, note, mission_day) VALUES($1,$2,$3,$4,$5) RETURNING id",
            body.crew_id, body.period, body.score, body.note, mission_day()
        )
    return {"checkin_id": mid, "mission_day": mission_day()}

@app.get("/api/v1/psych/mood/{crew_id}")
async def get_mood(crew_id: str, days: int = 30):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM mood_checkins WHERE crew_id=$1 ORDER BY mission_day DESC LIMIT $2",
            crew_id, days * 2
        )
        # 7-day average
        avg = await conn.fetchval(
            "SELECT AVG(score) FROM mood_checkins WHERE crew_id=$1 AND mission_day >= $2",
            crew_id, mission_day() - 7
        )
    return {
        "entries": [dict(r) for r in rows],
        "7day_avg": round(float(avg or 0), 2)
    }

@app.get("/api/v1/psych/mood/trend/{crew_id}")
async def mood_trend(crew_id: str, days: int = 30):
    """Daily average mood score for trend chart."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT mission_day, AVG(score) as avg_score
               FROM mood_checkins WHERE crew_id=$1 AND mission_day >= $2
               GROUP BY mission_day ORDER BY mission_day""",
            crew_id, mission_day() - days
        )
    return [{"day": r["mission_day"], "score": round(float(r["avg_score"]), 2)} for r in rows]

# ─── PSYCHOLOGICAL SURVEYS ────────────────────────────────────────────────────
@app.get("/api/v1/psych/surveys")
async def list_surveys():
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT id, name, description, schedule_days FROM psych_survey_templates")
    return [dict(r) for r in rows]

@app.get("/api/v1/psych/survey/{template_id}")
async def get_survey(template_id: int):
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM psych_survey_templates WHERE id=$1", template_id)
    if not row: raise HTTPException(404)
    return dict(row)

@app.post("/api/v1/psych/survey/submit")
async def submit_survey(body: SurveyResponseIn):
    async with pool.acquire() as conn:
        tmpl = await conn.fetchrow("SELECT * FROM psych_survey_templates WHERE id=$1", body.template_id)
        if not tmpl: raise HTTPException(404)
        rules = tmpl["scoring_rules"] if isinstance(tmpl["scoring_rules"], dict) else json.loads(tmpl["scoring_rules"])
        vals = list(body.responses.values())
        method = rules.get("method", "sum")
        if method == "panas_split":
            questions = tmpl["questions"] if isinstance(tmpl["questions"], list) else json.loads(tmpl["questions"])
            pos_ids = {q["id"] for q in questions if q.get("subscale") == "positive"}
            neg_ids = {q["id"] for q in questions if q.get("subscale") == "negative"}
            pos_score = sum(body.responses.get(qid, 0) for qid in pos_ids)
            neg_score = sum(body.responses.get(qid, 0) for qid in neg_ids)
            score = pos_score - neg_score
            subscores = {"positive": pos_score, "negative": neg_score}
        else:
            score = sum(vals)
            subscores = {}
        rid = await conn.fetchval(
            """INSERT INTO psych_survey_responses(crew_id, template_id, responses, total_score, subscores, mission_day)
               VALUES($1,$2,$3,$4,$5,$6) RETURNING id""",
            body.crew_id, body.template_id, json.dumps(body.responses),
            round(score, 2), json.dumps(subscores), mission_day()
        )
    flagged = rules.get("flag_above") and score > rules.get("flag_above", 9999)
    return {"response_id": rid, "total_score": round(score, 2), "subscores": subscores, "flagged": bool(flagged)}

@app.get("/api/v1/psych/survey/history/{crew_id}")
async def survey_history(crew_id: str):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT pr.*, pt.name FROM psych_survey_responses pr
               JOIN psych_survey_templates pt ON pr.template_id=pt.id
               WHERE pr.crew_id=$1 ORDER BY completed_at DESC LIMIT 30""", crew_id
        )
    return [dict(r) for r in rows]

# ─── SOCIOGRAM ────────────────────────────────────────────────────────────────
@app.post("/api/v1/psych/sociogram")
async def submit_rating(body: SociogramIn):
    if body.rater_id == body.ratee_id:
        raise HTTPException(400, "Cannot rate yourself")
    if body.comfort_score < 1 or body.comfort_score > 5:
        raise HTTPException(400, "Score must be 1–5")
    async with pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO sociogram_ratings(rater_id, ratee_id, comfort_score, mission_day)
               VALUES($1,$2,$3,$4)
               ON CONFLICT(rater_id, ratee_id, mission_day)
               DO UPDATE SET comfort_score=EXCLUDED.comfort_score""",
            body.rater_id, body.ratee_id, body.comfort_score, mission_day()
        )
    return {"status": "rating_stored"}

@app.get("/api/v1/psych/sociogram/my-ratings/{crew_id}")
async def my_outgoing_ratings(crew_id: str):
    """Crew can see ratings THEY GAVE, never ratings they received."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT ratee_id, comfort_score, mission_day FROM sociogram_ratings WHERE rater_id=$1 ORDER BY mission_day DESC",
            crew_id
        )
    return [dict(r) for r in rows]

@app.get("/api/v1/psych/sociogram/aggregate")
async def sociogram_aggregate(requester_id: str = Query(...)):
    """Flight surgeon only — see all dyad ratings as network graph data."""
    async with pool.acquire() as conn:
        role = await conn.fetchval("SELECT role FROM users WHERE username=$1", requester_id)
        if role != "flight_surgeon":
            raise HTTPException(403, "Flight surgeon access only")
        rows = await conn.fetch(
            """SELECT rater_id, ratee_id, AVG(comfort_score) as avg_score, COUNT(*) as n_ratings
               FROM sociogram_ratings GROUP BY rater_id, ratee_id"""
        )
    nodes = set()
    edges = []
    for r in rows:
        nodes.add(r["rater_id"]); nodes.add(r["ratee_id"])
        edges.append({"from": r["rater_id"], "to": r["ratee_id"],
                      "score": round(float(r["avg_score"]), 2), "n": r["n_ratings"]})
    return {"nodes": list(nodes), "edges": edges}

# ─── TRENDS (Flight Surgeon 30-day) ──────────────────────────────────────────
@app.get("/api/v1/psych/trends/{crew_id}")
async def crew_trends(crew_id: str, requester_id: str = Query(...)):
    async with pool.acquire() as conn:
        role = await conn.fetchval("SELECT role FROM users WHERE username=$1", requester_id)
        if role != "flight_surgeon" and requester_id != crew_id:
            raise HTTPException(403, "Access denied")
        start_day = mission_day() - 30
        mood = await conn.fetch(
            "SELECT mission_day, AVG(score) avg FROM mood_checkins WHERE crew_id=$1 AND mission_day>=$2 GROUP BY mission_day ORDER BY mission_day",
            crew_id, start_day
        )
        sleep = await conn.fetch(
            "SELECT mission_day, duration_min, quality_score FROM sleep_log WHERE crew_id=$1 AND mission_day>=$2 ORDER BY mission_day",
            crew_id, start_day
        )
        workload = await conn.fetch(
            "SELECT mission_day, total_score FROM questionnaire_responses WHERE crew_id=$1 AND mission_day>=$2 ORDER BY mission_day",
            crew_id, start_day
        )
    return {
        "crew_id": crew_id,
        "mood_trend": [{"day": r["mission_day"], "score": round(float(r["avg"]), 2)} for r in mood],
        "sleep_trend": [{"day": r["mission_day"], "hours": round(r["duration_min"]/60, 2), "quality": r["quality_score"]} for r in sleep],
        "workload_trend": [{"day": r["mission_day"], "score": float(r["total_score"])} for r in workload],
    }

@app.get("/api/v1/psych/trigger-sleep-alert")
async def manual_sleep_alert():
    await check_sleep_deprivation()
    return {"status": "sleep_alert_check_complete"}

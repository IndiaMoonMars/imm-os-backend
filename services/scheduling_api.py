#!/usr/bin/env python3
"""
IMM-OS Scheduling API — port 8006

Handles:
  - Project CRUD + hierarchy
  - Task CRUD with status workflow (PENDING → IN_PROGRESS → COMPLETE)
  - Subtask linking (parent_task_id)
  - Milestone / roadmap management
  - Procedure library + step-by-step execution engine
  - Carry-over summary for daily briefing integration
  - Background APScheduler job: 23:00 IST → PDF → MCC inbox via comms-api
"""
import asyncio
import json
import os
import time
import io
import logging
from datetime import datetime, timezone, timedelta, date
from typing import Optional, List

import asyncpg
import httpx
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ── Logging (IST) ─────────────────────────────────────────────────
ist_tz = timezone(timedelta(hours=5, minutes=30))
logging.Formatter.converter = lambda *args: datetime.now(ist_tz).timetuple()
logging.basicConfig(level=logging.INFO,
                    format="[IST %(asctime)s] [scheduling] %(message)s")
log = logging.getLogger(__name__)

app = FastAPI(title="IMM-OS Scheduling API", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_headers=["*"], allow_methods=["*"])

PG = dict(
    user=os.getenv("POSTGRES_USER", "admin"),
    password=os.getenv("POSTGRES_PASSWORD", "changeme"),
    database=os.getenv("POSTGRES_DB", "imm_db"),
    host=os.getenv("POSTGRES_HOST", "postgres"),
)
COMMS_URL = os.getenv("COMMS_API_URL", "http://comms-api:8005")
TIME_SVC   = os.getenv("TIME_SERVICE_URL", "http://time-service:8002")

async def get_conn(): return await asyncpg.connect(**PG)

async def current_mission_day() -> int:
    try:
        async with httpx.AsyncClient(timeout=2) as c:
            r = await c.get(f"{TIME_SVC}/api/v1/time/now")
            ts = float(r.json().get("unix_ts", time.time()))
            return max(1, int((ts - 1710000000) / 86400))
    except Exception:
        return 1

# ══════════════════════════════════════════════════════════════════
#  PROJECTS
# ══════════════════════════════════════════════════════════════════

class ProjectCreate(BaseModel):
    name: str
    description: Optional[str] = None
    owner_id: str
    start_date: Optional[date] = None
    end_date: Optional[date] = None

class StatusUpdate(BaseModel):
    status: str

@app.post("/api/v1/scheduling/projects", status_code=201)
async def create_project(p: ProjectCreate):
    mday = await current_mission_day()
    conn = await get_conn()
    try:
        row = await conn.fetchrow(
            "INSERT INTO projects (name, description, owner_id, start_date, end_date, mission_day_start)"
            " VALUES ($1,$2,$3,$4,$5,$6) RETURNING id",
            p.name, p.description, p.owner_id, p.start_date, p.end_date, mday)
        return {"project_id": row["id"]}
    finally: await conn.close()

@app.get("/api/v1/scheduling/projects")
async def list_projects(status: Optional[str] = None):
    conn = await get_conn()
    try:
        q = "SELECT * FROM projects"
        params = []
        if status:
            params.append(status)
            q += " WHERE status=$1"
        q += " ORDER BY created_at DESC"
        return [dict(r) for r in await conn.fetch(q, *params)]
    finally: await conn.close()

@app.patch("/api/v1/scheduling/projects/{pid}/status")
async def update_project_status(pid: int, upd: StatusUpdate):
    conn = await get_conn()
    try:
        await conn.execute("UPDATE projects SET status=$1 WHERE id=$2", upd.status, pid)
        return {"updated": True}
    finally: await conn.close()

# ══════════════════════════════════════════════════════════════════
#  TASKS
# ══════════════════════════════════════════════════════════════════

class TaskCreate(BaseModel):
    project_id: int
    title: str
    description: Optional[str] = None
    assignee_id: Optional[str] = None
    priority: str = "NORMAL"
    deadline: Optional[datetime] = None
    parent_task_id: Optional[int] = None

class TaskUpdate(BaseModel):
    status: Optional[str] = None
    assignee_id: Optional[str] = None
    deadline: Optional[datetime] = None
    priority: Optional[str] = None

@app.post("/api/v1/scheduling/tasks", status_code=201)
async def create_task(t: TaskCreate):
    mday = await current_mission_day()
    conn = await get_conn()
    try:
        row = await conn.fetchrow(
            "INSERT INTO tasks (project_id, parent_task_id, title, description, assignee_id,"
            " priority, deadline, mission_day) VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING id",
            t.project_id, t.parent_task_id, t.title, t.description,
            t.assignee_id, t.priority, t.deadline, mday)
        return {"task_id": row["id"]}
    finally: await conn.close()

@app.get("/api/v1/scheduling/tasks")
async def list_tasks(project_id: Optional[int] = None,
                     assignee_id: Optional[str] = None,
                     status: Optional[str] = None):
    conn = await get_conn()
    try:
        clauses, params = [], []
        if project_id: params.append(project_id); clauses.append(f"project_id=${len(params)}")
        if assignee_id: params.append(assignee_id); clauses.append(f"assignee_id=${len(params)}")
        if status: params.append(status); clauses.append(f"status=${len(params)}")
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        rows = await conn.fetch(
            f"SELECT t.*, p.name as project_name FROM tasks t"
            f" JOIN projects p ON t.project_id=p.id {where}"
            f" ORDER BY t.deadline NULLS LAST, t.priority DESC", *params)
        return [dict(r) for r in rows]
    finally: await conn.close()

@app.patch("/api/v1/scheduling/tasks/{tid}")
async def update_task(tid: int, upd: TaskUpdate):
    conn = await get_conn()
    try:
        if upd.status:
            completed_at = datetime.now(timezone.utc) if upd.status == "COMPLETE" else None
            await conn.execute(
                "UPDATE tasks SET status=$1, completed_at=$2 WHERE id=$3",
                upd.status, completed_at, tid)
        if upd.assignee_id:
            await conn.execute("UPDATE tasks SET assignee_id=$1 WHERE id=$2", upd.assignee_id, tid)
        if upd.deadline:
            await conn.execute("UPDATE tasks SET deadline=$1 WHERE id=$2", upd.deadline, tid)
        if upd.priority:
            await conn.execute("UPDATE tasks SET priority=$1 WHERE id=$2", upd.priority, tid)
        return {"updated": True}
    finally: await conn.close()

@app.get("/api/v1/scheduling/carryover")
async def carry_over_tasks():
    """Incomplete tasks from previous mission days — fed into daily briefings."""
    mday = await current_mission_day()
    conn = await get_conn()
    try:
        rows = await conn.fetch(
            "SELECT t.*, p.name as project_name FROM tasks t"
            " JOIN projects p ON t.project_id=p.id"
            " WHERE t.status NOT IN ('COMPLETE','CANCELLED')"
            "   AND t.mission_day < $1"
            " ORDER BY t.priority DESC, t.deadline NULLS LAST",
            mday)
        return [dict(r) for r in rows]
    finally: await conn.close()

# ══════════════════════════════════════════════════════════════════
#  MILESTONES / ROADMAP
# ══════════════════════════════════════════════════════════════════

class MilestoneCreate(BaseModel):
    project_id: int
    name: str
    target_date: date

@app.post("/api/v1/scheduling/milestones", status_code=201)
async def create_milestone(m: MilestoneCreate):
    conn = await get_conn()
    try:
        row = await conn.fetchrow(
            "INSERT INTO milestones (project_id, name, target_date) VALUES ($1,$2,$3) RETURNING id",
            m.project_id, m.name, m.target_date)
        return {"milestone_id": row["id"]}
    finally: await conn.close()

@app.get("/api/v1/scheduling/milestones")
async def list_milestones(project_id: Optional[int] = None):
    conn = await get_conn()
    try:
        q = ("SELECT m.*, p.name as project_name FROM milestones m"
             " JOIN projects p ON m.project_id=p.id")
        params = []
        if project_id: params.append(project_id); q += f" WHERE m.project_id=${len(params)}"
        q += " ORDER BY m.target_date"
        return [dict(r) for r in await conn.fetch(q, *params)]
    finally: await conn.close()

@app.patch("/api/v1/scheduling/milestones/{mid}/reach")
async def reach_milestone(mid: int):
    conn = await get_conn()
    try:
        await conn.execute(
            "UPDATE milestones SET reached=TRUE, reached_at=NOW() WHERE id=$1", mid)
        return {"reached": True}
    finally: await conn.close()

# ══════════════════════════════════════════════════════════════════
#  PROCEDURES
# ══════════════════════════════════════════════════════════════════

class ProcedureCreate(BaseModel):
    name: str
    category: Optional[str] = None
    description: Optional[str] = None
    version: str = "1.0"
    steps: List[dict]   # [{title, detail, caution?}]
    created_by: str

@app.post("/api/v1/scheduling/procedures", status_code=201)
async def create_procedure(proc: ProcedureCreate):
    conn = await get_conn()
    try:
        row = await conn.fetchrow(
            "INSERT INTO procedures (name, category, description, version, steps, created_by)"
            " VALUES ($1,$2,$3,$4,$5,$6) RETURNING id",
            proc.name, proc.category, proc.description,
            proc.version, json.dumps(proc.steps), proc.created_by)
        return {"procedure_id": row["id"]}
    finally: await conn.close()

@app.get("/api/v1/scheduling/procedures")
async def list_procedures(category: Optional[str] = None):
    conn = await get_conn()
    try:
        q = "SELECT id, name, category, description, version, created_by, created_at FROM procedures"
        params = []
        if category: params.append(category); q += f" WHERE category=${len(params)}"
        return [dict(r) for r in await conn.fetch(q, *params)]
    finally: await conn.close()

@app.get("/api/v1/scheduling/procedures/{pid}")
async def get_procedure(pid: int):
    conn = await get_conn()
    try:
        row = await conn.fetchrow("SELECT * FROM procedures WHERE id=$1", pid)
        if not row: raise HTTPException(404, "Procedure not found")
        return dict(row)
    finally: await conn.close()

# ── Procedure Runs (execution engine) ────────────────────────────

class RunCreate(BaseModel):
    procedure_id: int
    crew_id: str
    task_id: Optional[int] = None

@app.post("/api/v1/scheduling/procedures/run", status_code=201)
async def start_run(req: RunCreate):
    conn = await get_conn()
    try:
        row = await conn.fetchrow(
            "INSERT INTO procedure_runs (procedure_id, crew_id, task_id)"
            " VALUES ($1,$2,$3) RETURNING id",
            req.procedure_id, req.crew_id, req.task_id)
        return {"run_id": row["id"]}
    finally: await conn.close()

@app.get("/api/v1/scheduling/runs/{run_id}")
async def get_run(run_id: int):
    conn = await get_conn()
    try:
        run = await conn.fetchrow("SELECT * FROM procedure_runs WHERE id=$1", run_id)
        if not run: raise HTTPException(404, "Run not found")
        proc = await conn.fetchrow("SELECT * FROM procedures WHERE id=$1", run["procedure_id"])
        steps = json.loads(proc["steps"]) if isinstance(proc["steps"], str) else proc["steps"]
        timestamps = json.loads(run["step_timestamps"]) if isinstance(run["step_timestamps"], str) else run["step_timestamps"]
        total = len(steps)
        done = run["current_step"]
        return {
            **dict(run),
            "steps": steps,
            "step_timestamps": timestamps,
            "total_steps": total,
            "completed_steps": done,
            "progress_pct": round(100 * done / total) if total else 0,
            "procedure_name": proc["name"],
        }
    finally: await conn.close()

class StepComplete(BaseModel):
    crew_id: str

@app.post("/api/v1/scheduling/runs/{run_id}/step")
async def complete_step(run_id: int, req: StepComplete):
    conn = await get_conn()
    try:
        run = await conn.fetchrow("SELECT * FROM procedure_runs WHERE id=$1", run_id)
        if not run: raise HTTPException(404, "Run not found")
        if run["status"] != "IN_PROGRESS":
            raise HTTPException(400, f"Run is {run['status']}")
        proc = await conn.fetchrow(
            "SELECT jsonb_array_length(steps) as total FROM procedures WHERE id=$1",
            run["procedure_id"])
        total = proc["total"]
        new_step = run["current_step"] + 1
        ts = json.loads(run["step_timestamps"]) if isinstance(run["step_timestamps"], str) else (run["step_timestamps"] or {})
        ts[str(new_step - 1)] = datetime.now(timezone.utc).isoformat()
        status = "COMPLETE" if new_step >= total else "IN_PROGRESS"
        completed_at = datetime.now(timezone.utc) if status == "COMPLETE" else None
        await conn.execute(
            "UPDATE procedure_runs SET current_step=$1, step_timestamps=$2,"
            " status=$3, completed_at=$4 WHERE id=$5",
            new_step, json.dumps(ts), status, completed_at, run_id)
        return {
            "current_step": new_step,
            "total_steps": total,
            "progress_pct": round(100 * new_step / total),
            "status": status,
        }
    finally: await conn.close()

@app.post("/api/v1/scheduling/runs/{run_id}/abort")
async def abort_run(run_id: int):
    conn = await get_conn()
    try:
        await conn.execute(
            "UPDATE procedure_runs SET status='ABORTED' WHERE id=$1", run_id)
        return {"aborted": True}
    finally: await conn.close()

# ══════════════════════════════════════════════════════════════════
#  DAILY SUMMARY PDF WORKER
# ══════════════════════════════════════════════════════════════════

async def generate_daily_report():
    """Generates a plain-text summary (PDF content) and sends to MCC via comms-api."""
    log.info("Daily report job triggered.")
    mday = await current_mission_day()
    conn = await asyncpg.connect(**PG)
    try:
        complete = await conn.fetch(
            "SELECT t.title, t.assignee_id, t.completed_at, p.name as project"
            " FROM tasks t JOIN projects p ON t.project_id=p.id"
            " WHERE t.mission_day=$1 AND t.status='COMPLETE'", mday)
        pending = await conn.fetch(
            "SELECT t.title, t.assignee_id, t.priority, p.name as project"
            " FROM tasks t JOIN projects p ON t.project_id=p.id"
            " WHERE t.mission_day=$1 AND t.status NOT IN ('COMPLETE','CANCELLED')", mday)
    finally:
        await conn.close()

    lines = [
        f"IMM-OS DAILY SUMMARY REPORT",
        f"Mission Day: {mday}",
        f"Generated: {datetime.now(ist_tz).strftime('%Y-%m-%d %H:%M:%S IST')}",
        "=" * 60,
        f"\nCOMPLETED TASKS ({len(complete)})",
        "-" * 40,
    ]
    for t in complete:
        lines.append(f"  [✓] {t['title']} ({t['project']}) — {t['assignee_id']}")

    lines += [f"\nPENDING / CARRY-OVER TASKS ({len(pending)})", "-" * 40]
    for t in pending:
        lines.append(f"  [ ] [{t['priority']}] {t['title']} ({t['project']}) — {t['assignee_id']}")

    report_body = "\n".join(lines)

    # Dispatch to MCC inbox via comms-api
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(f"{COMMS_URL}/api/v1/comms/message", json={
                "sender_id": "imm-scheduler",
                "recipient_group": "mcc",
                "subject": f"Daily Summary Report — Mission Day {mday}",
                "body": report_body,
            })
        log.info(f"Daily report dispatched to MCC (Mission Day {mday}).")
    except Exception as e:
        log.error(f"Failed to send daily report: {e}")

scheduler = AsyncIOScheduler(timezone="Asia/Kolkata")

@app.on_event("startup")
async def startup():
    # Seed a demonstration CO2 calibration procedure if none exist
    conn = await asyncpg.connect(**PG)
    count = await conn.fetchval("SELECT COUNT(*) FROM procedures")
    if count == 0:
        steps = [
            {"title": "Power down SCD40 sensor",           "detail": "Disconnect 3.3 V supply; wait 10 s."},
            {"title": "Prepare calibration gas",            "detail": "Connect certified 400 ppm CO₂ reference cylinder."},
            {"title": "Flush sensor housing",               "detail": "Open valve; purge for 60 s at 200 mL/min."},
            {"title": "Power on SCD40",                     "detail": "Reconnect supply; boot time 1 s."},
            {"title": "Send forced recalibration command",  "detail": "Write 0x362F to SCD40 I²C register 0x5204."},
            {"title": "Read measured CO₂",                  "detail": "Query measurement; expected 390–410 ppm."},
            {"title": "Verify within tolerance",            "detail": "If |reading − 400| > 15 ppm repeat from step 3.", "caution": True},
            {"title": "Reconnect sensor to pipeline",       "detail": "Restore MQTT publisher; confirm topic active."},
            {"title": "Log calibration event",              "detail": "Record in maintenance log with timestamp."},
            {"title": "Restore normal ECLSS monitoring",    "detail": "Verify Grafana shows live SCD40 data."},
        ]
        await conn.execute(
            "INSERT INTO procedures (name, category, description, version, steps, created_by)"
            " VALUES ($1,$2,$3,$4,$5,$6)",
            "SCD40 CO₂ Sensor Calibration", "MAINTENANCE",
            "Full forced-recalibration procedure for SCD40 sensor using 400 ppm reference gas.",
            "1.0", json.dumps(steps), "MCC-Engineer")
        log.info("Seeded SCD40 calibration procedure.")
    await conn.close()

    # Schedule daily report at 23:00 IST
    scheduler.add_job(generate_daily_report, "cron", hour=23, minute=0,
                      id="daily_report", replace_existing=True)
    scheduler.start()
    log.info("APScheduler started — daily report scheduled at 23:00 IST.")

@app.on_event("shutdown")
async def shutdown():
    scheduler.shutdown()

@app.post("/api/v1/scheduling/report/trigger")
async def trigger_report_now(background_tasks: BackgroundTasks):
    """Manual trigger for testing (does not wait for 23:00)."""
    background_tasks.add_task(generate_daily_report)
    return {"triggered": True}

@app.get("/health")
def health():
    return {"status": "ok"}

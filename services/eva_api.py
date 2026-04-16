#!/usr/bin/env python3
"""
EVA API — FastAPI microservice on port 8004.
Handles:
  - EVA plan CRUD  → /api/v1/eva/plan
  - EVA tool inventory registration → /api/v1/eva/tools/register
  - RFID tool scan (checkout / checkin) → /api/v1/eva/tools/scan
  - Checklist status update → /api/v1/eva/plan/{id}/checklist
"""
import os
import logging
from datetime import datetime, timezone
from typing import List, Optional

import asyncpg
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s [eva_api] %(message)s")
log = logging.getLogger(__name__)

app = FastAPI(title="IMM-OS EVA API", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_headers=["*"], allow_methods=["*"])

# ── DB helpers ─────────────────────────────────────────────────────

async def get_conn():
    return await asyncpg.connect(
        user=os.getenv("POSTGRES_USER", "admin"),
        password=os.getenv("POSTGRES_PASSWORD", "changeme"),
        database=os.getenv("POSTGRES_DB", "imm_db"),
        host=os.getenv("POSTGRES_HOST", "postgres"),
    )

# ── Pydantic Models ───────────────────────────────────────────────

class EvaPlanCreate(BaseModel):
    crew_members: List[str]
    objectives: str
    duration_minutes: int
    tools_required: List[str] = []
    abort_criteria: Optional[str] = None
    checklist: List[dict] = []

class EvaPlanStatusUpdate(BaseModel):
    status: str                  # PLANNED | GO | IN_PROGRESS | COMPLETE | ABORTED

class ChecklistUpdate(BaseModel):
    checklist: List[dict]        # full updated JSON list

class ToolRegister(BaseModel):
    rfid_tag: str
    tool_name: str
    category: Optional[str] = None

class ToolScan(BaseModel):
    rfid_tag: str
    action: str                  # CHECKOUT | CHECKIN
    eva_plan_id: Optional[int] = None
    operator_id: Optional[str] = "unknown"

# ── EVA Plan routes ───────────────────────────────────────────────

@app.post("/api/v1/eva/plan", status_code=201)
async def create_eva_plan(plan: EvaPlanCreate):
    conn = await get_conn()
    try:
        import json as _json
        row = await conn.fetchrow(
            """
            INSERT INTO eva_plans
                (crew_members, objectives, duration_minutes,
                 tools_required, abort_criteria, checklist)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING id
            """,
            plan.crew_members, plan.objectives, plan.duration_minutes,
            plan.tools_required, plan.abort_criteria,
            _json.dumps(plan.checklist),
        )
        log.info(f"EVA plan created id={row['id']}")
        return {"eva_plan_id": row["id"]}
    finally:
        await conn.close()

@app.get("/api/v1/eva/plans")
async def list_eva_plans():
    conn = await get_conn()
    try:
        rows = await conn.fetch("SELECT * FROM eva_plans ORDER BY created_at DESC LIMIT 50")
        return [dict(r) for r in rows]
    finally:
        await conn.close()

@app.get("/api/v1/eva/plan/{plan_id}")
async def get_eva_plan(plan_id: int):
    conn = await get_conn()
    try:
        row = await conn.fetchrow("SELECT * FROM eva_plans WHERE id=$1", plan_id)
        if not row:
            raise HTTPException(status_code=404, detail="EVA plan not found")
        return dict(row)
    finally:
        await conn.close()

@app.patch("/api/v1/eva/plan/{plan_id}/status")
async def update_plan_status(plan_id: int, update: EvaPlanStatusUpdate):
    conn = await get_conn()
    try:
        go_ts = datetime.now(timezone.utc) if update.status == "GO" else None
        await conn.execute(
            "UPDATE eva_plans SET status=$1, go_at=$2 WHERE id=$3",
            update.status, go_ts, plan_id
        )
        return {"updated": True}
    finally:
        await conn.close()

@app.patch("/api/v1/eva/plan/{plan_id}/checklist")
async def update_checklist(plan_id: int, update: ChecklistUpdate):
    import json as _json
    conn = await get_conn()
    try:
        await conn.execute(
            "UPDATE eva_plans SET checklist=$1 WHERE id=$2",
            _json.dumps(update.checklist), plan_id
        )
        # If all items done → auto-set status
        if update.checklist and all(item.get("done") for item in update.checklist):
            await conn.execute(
                "UPDATE eva_plans SET status='GO' WHERE id=$1 AND status='PLANNED'",
                plan_id
            )
            log.info(f"EVA plan {plan_id} checklist complete → status GO")
        return {"updated": True}
    finally:
        await conn.close()

# ── Tool inventory routes ─────────────────────────────────────────

@app.post("/api/v1/eva/tools/register", status_code=201)
async def register_tool(tool: ToolRegister):
    conn = await get_conn()
    try:
        await conn.execute(
            """
            INSERT INTO tool_inventory (rfid_tag, tool_name, category)
            VALUES ($1, $2, $3)
            ON CONFLICT (rfid_tag) DO NOTHING
            """,
            tool.rfid_tag, tool.tool_name, tool.category
        )
        return {"registered": True}
    finally:
        await conn.close()

@app.get("/api/v1/eva/tools")
async def list_tools():
    conn = await get_conn()
    try:
        rows = await conn.fetch("SELECT * FROM tool_inventory ORDER BY tool_name")
        return [dict(r) for r in rows]
    finally:
        await conn.close()

@app.post("/api/v1/eva/tools/scan", status_code=201)
async def scan_tool(scan: ToolScan):
    if scan.action not in ("CHECKOUT", "CHECKIN"):
        raise HTTPException(status_code=422, detail="action must be CHECKOUT or CHECKIN")
    conn = await get_conn()
    try:
        # Verify tool exists
        tool = await conn.fetchrow(
            "SELECT * FROM tool_inventory WHERE rfid_tag=$1", scan.rfid_tag
        )
        if not tool:
            raise HTTPException(status_code=404, detail=f"Tool {scan.rfid_tag} not in inventory")

        # Log the scan event
        await conn.execute(
            """
            INSERT INTO tool_checkout (rfid_tag, eva_plan_id, operator_id, action)
            VALUES ($1, $2, $3, $4)
            """,
            scan.rfid_tag, scan.eva_plan_id, scan.operator_id, scan.action
        )

        # Update availability flag
        is_available = (scan.action == "CHECKIN")
        await conn.execute(
            "UPDATE tool_inventory SET is_available=$1, last_scan=NOW() WHERE rfid_tag=$2",
            is_available, scan.rfid_tag
        )
        log.info(f"Tool {scan.rfid_tag} → {scan.action} by {scan.operator_id}")
        return {"logged": True, "action": scan.action, "rfid_tag": scan.rfid_tag}
    finally:
        await conn.close()

# ── Health ────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok"}

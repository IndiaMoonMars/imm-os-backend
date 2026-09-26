#!/usr/bin/env python3
"""
IMM-OS Inventory API — Phase 11, port 8010.
Handles:
  - Inventory items (barcode-indexed; solids, liquids, gases) + stock adjustments
      /api/v1/inventory/items, /items/{id}, /items/{id}/adjust
      /api/v1/inventory/scan/{barcode}          fast lookup for barcode scanners
  - Tool checkout / checkin by barcode          /api/v1/inventory/checkout, /checkin, /checkouts
  - Incident reports with photo                 /api/v1/incidents, /incidents/{id}/photo
  - Repair log; parts used are deducted from stock atomically   /api/v1/repairs

Every stock change is written to inventory_transactions.
"""
import os
import logging
from datetime import datetime, timezone
from typing import List, Optional

import asyncpg
from fastapi import Depends, FastAPI, File, Form, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel, confloat, conint, constr, root_validator, validator

from services.auth import (
    COMMANDER, MCC_OPERATOR, User, crew_or_edge, current_user, require_roles,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [inventory_api] %(message)s")
log = logging.getLogger(__name__)

app = FastAPI(title="IMM-OS Inventory API", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_headers=["*"], allow_methods=["*"])

MEDIA_DIR = os.getenv("INCIDENT_MEDIA_DIR", "/app/media/incidents")
MAX_PHOTO_BYTES = 10 * 1024 * 1024

# Units allowed per physical state (mirrors the CHECK constraint in init.sql)
UNITS = {"solid": {"units", "kg", "g"}, "liquid": {"mL", "L"}, "gas": {"bar", "kPa"}}

pool: Optional[asyncpg.Pool] = None


@app.on_event("startup")
async def startup():
    global pool
    os.makedirs(MEDIA_DIR, exist_ok=True)
    pool = await asyncpg.create_pool(
        user=os.getenv("POSTGRES_USER", "admin"),
        password=os.getenv("POSTGRES_PASSWORD", "changeme"),
        database=os.getenv("POSTGRES_DB", "imm_db"),
        host=os.getenv("POSTGRES_HOST", "postgres"),
        min_size=1, max_size=10,
    )


@app.on_event("shutdown")
async def shutdown():
    if pool is not None:
        await pool.close()

# ── Models ────────────────────────────────────────────────────────

Barcode = constr(strip_whitespace=True, min_length=1, max_length=128)


class ItemIn(BaseModel):
    barcode: Barcode
    name: constr(strip_whitespace=True, min_length=1, max_length=255)
    category: Optional[str] = None
    physical_state: str
    unit: str
    quantity: confloat(ge=0) = 0
    min_quantity: confloat(ge=0) = 0
    location: Optional[str] = None
    is_tool: bool = False
    notes: Optional[str] = None

    @root_validator(skip_on_failure=True)
    def unit_matches_state(cls, v):
        state, unit = v.get("physical_state"), v.get("unit")
        if state not in UNITS:
            raise ValueError("physical_state must be solid, liquid or gas")
        if unit not in UNITS[state]:
            raise ValueError(f"unit for {state} must be one of {sorted(UNITS[state])}")
        return v


class ItemPatch(BaseModel):
    name: Optional[constr(strip_whitespace=True, min_length=1, max_length=255)] = None
    category: Optional[str] = None
    physical_state: Optional[str] = None
    unit: Optional[str] = None
    min_quantity: Optional[confloat(ge=0)] = None
    location: Optional[str] = None
    is_tool: Optional[bool] = None
    notes: Optional[str] = None

    @root_validator(skip_on_failure=True)
    def state_and_unit_together(cls, v):
        state, unit = v.get("physical_state"), v.get("unit")
        if (state is None) != (unit is None):
            raise ValueError("physical_state and unit must be changed together")
        if state is not None and (state not in UNITS or unit not in UNITS[state]):
            raise ValueError("unit does not match physical_state")
        return v


class Adjustment(BaseModel):
    delta: float
    reason: constr(strip_whitespace=True, min_length=1, max_length=255)

    @validator("delta")
    def non_zero(cls, v):
        if v == 0:
            raise ValueError("delta must not be 0")
        return v


class CheckoutIn(BaseModel):
    barcode: Barcode
    activity: constr(strip_whitespace=True, min_length=1, max_length=100)
    crew_id: Optional[str] = None   # required for scanner stations; defaults to the logged-in user


class CheckinIn(BaseModel):
    barcode: Barcode


class RepairPart(BaseModel):
    barcode: Optional[Barcode] = None
    item_id: Optional[int] = None
    quantity: confloat(gt=0)

    @root_validator(skip_on_failure=True)
    def one_reference(cls, v):
        if (v.get("barcode") is None) == (v.get("item_id") is None):
            raise ValueError("give exactly one of barcode or item_id")
        return v


class RepairIn(BaseModel):
    item_description: constr(strip_whitespace=True, min_length=1, max_length=255)
    item_id: Optional[int] = None
    incident_id: Optional[int] = None
    repair_minutes: conint(ge=0)
    signature: constr(strip_whitespace=True, min_length=1, max_length=255)
    notes: Optional[str] = None
    parts: List[RepairPart] = []

# ── Helpers ───────────────────────────────────────────────────────


def with_status(row) -> dict:
    d = dict(row)
    d["low_stock"] = d["quantity"] <= d["min_quantity"]
    return d


async def item_by_barcode(conn, barcode: str, lock: bool = False):
    sql = "SELECT * FROM inventory_items WHERE barcode=$1" + (" FOR UPDATE" if lock else "")
    row = await conn.fetchrow(sql, barcode)
    if row is None:
        raise HTTPException(404, f"No inventory item with barcode {barcode}")
    return row


def detect_image_type(head: bytes) -> Optional[tuple]:
    """Identify the photo by its magic bytes, not the client-declared type."""
    if head.startswith(b"\xff\xd8\xff"):
        return "image/jpeg", ".jpg"
    if head.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png", ".png"
    if head[:4] == b"RIFF" and head[8:12] == b"WEBP":
        return "image/webp", ".webp"
    return None

# ── Items ─────────────────────────────────────────────────────────


@app.get("/api/v1/inventory/items", dependencies=[Depends(current_user)])
async def list_items(q: Optional[str] = None, category: Optional[str] = None,
                     low_stock: bool = False, tools_only: bool = False):
    where, args = ["TRUE"], []
    if q:
        args.append(f"%{q}%")
        where.append(f"(name ILIKE ${len(args)} OR barcode ILIKE ${len(args)})")
    if category:
        args.append(category)
        where.append(f"category = ${len(args)}")
    if low_stock:
        where.append("quantity <= min_quantity")
    if tools_only:
        where.append("is_tool")
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"SELECT * FROM inventory_items WHERE {' AND '.join(where)} ORDER BY name LIMIT 500", *args)
        open_rows = await conn.fetch(
            "SELECT item_id, crew_id, activity, checked_out_at FROM tool_checkouts WHERE checked_in_at IS NULL")
    open_by_item = {r["item_id"]: dict(r) for r in open_rows}
    return [{**with_status(r), "checked_out": open_by_item.get(r["id"])} for r in rows]


@app.post("/api/v1/inventory/items", status_code=201)
async def create_item(item: ItemIn, user: User = Depends(current_user)):
    async with pool.acquire() as conn:
        async with conn.transaction():
            try:
                row = await conn.fetchrow(
                    """INSERT INTO inventory_items
                       (barcode, name, category, physical_state, unit, quantity, min_quantity,
                        location, is_tool, notes)
                       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING *""",
                    item.barcode, item.name, item.category, item.physical_state, item.unit,
                    item.quantity, item.min_quantity, item.location, item.is_tool, item.notes)
            except asyncpg.UniqueViolationError:
                raise HTTPException(409, f"Barcode {item.barcode} is already registered")
            if item.quantity:
                await conn.execute(
                    """INSERT INTO inventory_transactions (item_id, delta, quantity_after, reason, actor)
                       VALUES ($1,$2,$3,'initial stock',$4)""",
                    row["id"], item.quantity, item.quantity, user.username)
    log.info(f"Item {item.barcode} '{item.name}' added by {user.username}")
    return with_status(row)


@app.get("/api/v1/inventory/items/{item_id}", dependencies=[Depends(current_user)])
async def get_item(item_id: int):
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM inventory_items WHERE id=$1", item_id)
        if row is None:
            raise HTTPException(404, "Item not found")
        history = await conn.fetch(
            "SELECT delta, quantity_after, reason, actor, created_at FROM inventory_transactions "
            "WHERE item_id=$1 ORDER BY created_at DESC LIMIT 50", item_id)
    return {**with_status(row), "history": [dict(h) for h in history]}


@app.patch("/api/v1/inventory/items/{item_id}", dependencies=[Depends(current_user)])
async def update_item(item_id: int, patch: ItemPatch):
    fields = patch.dict(exclude_unset=True)
    if not fields:
        raise HTTPException(422, "Nothing to update")
    cols = list(fields)  # validated model field names only, never user-supplied keys
    sets = ", ".join(f"{c}=${i + 2}" for i, c in enumerate(cols))
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"UPDATE inventory_items SET {sets}, updated_at=NOW() WHERE id=$1 RETURNING *",
            item_id, *[fields[c] for c in cols])
    if row is None:
        raise HTTPException(404, "Item not found")
    return with_status(row)


@app.delete("/api/v1/inventory/items/{item_id}", status_code=204,
            dependencies=[Depends(require_roles(COMMANDER, MCC_OPERATOR))])
async def delete_item(item_id: int):
    async with pool.acquire() as conn:
        try:
            deleted = await conn.execute("DELETE FROM inventory_items WHERE id=$1", item_id)
        except asyncpg.ForeignKeyViolationError:
            raise HTTPException(409, "Item is referenced by repair records; set quantity to 0 instead")
    if deleted.endswith(" 0"):
        raise HTTPException(404, "Item not found")


@app.post("/api/v1/inventory/items/{item_id}/adjust")
async def adjust_stock(item_id: int, adj: Adjustment, user: User = Depends(current_user)):
    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow("SELECT * FROM inventory_items WHERE id=$1 FOR UPDATE", item_id)
            if row is None:
                raise HTTPException(404, "Item not found")
            new_qty = row["quantity"] + adj.delta
            if new_qty < 0:
                raise HTTPException(409, f"Only {row['quantity']} {row['unit']} in stock")
            row = await conn.fetchrow(
                "UPDATE inventory_items SET quantity=$2, updated_at=NOW() WHERE id=$1 RETURNING *",
                item_id, new_qty)
            await conn.execute(
                """INSERT INTO inventory_transactions (item_id, delta, quantity_after, reason, actor)
                   VALUES ($1,$2,$3,$4,$5)""",
                item_id, adj.delta, new_qty, adj.reason, user.username)
    return with_status(row)


@app.get("/api/v1/inventory/scan/{barcode}", dependencies=[Depends(crew_or_edge)])
async def scan(barcode: str):
    """Barcode lookup for scanners and the crew UI (indexed; single round-trip)."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """SELECT i.*, c.crew_id AS out_crew_id, c.activity AS out_activity,
                      c.checked_out_at AS out_at
               FROM inventory_items i
               LEFT JOIN tool_checkouts c ON c.item_id = i.id AND c.checked_in_at IS NULL
               WHERE i.barcode=$1""", barcode.strip())
    if row is None:
        raise HTTPException(404, f"No inventory item with barcode {barcode}")
    d = with_status(row)
    out = {k: d.pop(k) for k in ("out_crew_id", "out_activity", "out_at")}
    d["checked_out"] = ({"crew_id": out["out_crew_id"], "activity": out["out_activity"],
                         "checked_out_at": out["out_at"]} if out["out_crew_id"] else None)
    return d

# ── Tool checkout ─────────────────────────────────────────────────


@app.post("/api/v1/inventory/checkout", status_code=201)
async def checkout_tool(req: CheckoutIn, user: User = Depends(crew_or_edge)):
    human = bool(user.roles & {"crew", "commander", "flight_surgeon", "mcc_operator"})
    if human:
        crew_id = req.crew_id or user.username
        if not user.is_self(crew_id) and not user.has_any(COMMANDER, MCC_OPERATOR):
            raise HTTPException(403, "Only the commander or MCC can check tools out to someone else")
    else:  # scanner station / service: the station says who is taking the tool
        if not req.crew_id:
            raise HTTPException(422, "crew_id is required for scanner stations")
        crew_id = req.crew_id
    async with pool.acquire() as conn:
        item = await item_by_barcode(conn, req.barcode)
        if not item["is_tool"]:
            raise HTTPException(409, f"{item['name']} is not a tool")
        # Single statement, no explicit transaction: after a unique violation the
        # connection stays usable for the holder lookup below
        try:
            row = await conn.fetchrow(
                """INSERT INTO tool_checkouts (item_id, crew_id, activity, checked_out_by)
                   VALUES ($1,$2,$3,$4) RETURNING *""",
                item["id"], crew_id, req.activity, user.username)
        except asyncpg.UniqueViolationError:
            holder = await conn.fetchval(
                "SELECT crew_id FROM tool_checkouts WHERE item_id=$1 AND checked_in_at IS NULL", item["id"])
            raise HTTPException(409, f"{item['name']} is already checked out to {holder}")
    log.info(f"CHECKOUT {req.barcode} ({item['name']}) -> {crew_id} for {req.activity}")
    return {**dict(row), "item_name": item["name"], "barcode": item["barcode"]}


@app.post("/api/v1/inventory/checkin")
async def checkin_tool(req: CheckinIn, user: User = Depends(crew_or_edge)):
    async with pool.acquire() as conn:
        item = await item_by_barcode(conn, req.barcode)
        row = await conn.fetchrow(
            """UPDATE tool_checkouts
               SET checked_in_at=NOW(), checked_in_by=$2,
                   duration_seconds=EXTRACT(EPOCH FROM NOW() - checked_out_at)
               WHERE item_id=$1 AND checked_in_at IS NULL RETURNING *""",
            item["id"], user.username)
    if row is None:
        raise HTTPException(409, f"{item['name']} is not checked out")
    log.info(f"CHECKIN {req.barcode} ({item['name']}) after {row['duration_seconds']:.0f}s")
    return {**dict(row), "item_name": item["name"], "barcode": item["barcode"]}


@app.get("/api/v1/inventory/checkouts", dependencies=[Depends(current_user)])
async def list_checkouts(open_only: bool = False, crew_id: Optional[str] = None,
                         limit: int = Query(100, ge=1, le=500)):
    where, args = ["TRUE"], []
    if open_only:
        where.append("c.checked_in_at IS NULL")
    if crew_id:
        args.append(crew_id)
        where.append(f"c.crew_id = ${len(args)}")
    args.append(limit)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""SELECT c.*, i.name AS item_name, i.barcode FROM tool_checkouts c
                JOIN inventory_items i ON i.id = c.item_id
                WHERE {' AND '.join(where)} ORDER BY c.checked_out_at DESC LIMIT ${len(args)}""", *args)
    return [dict(r) for r in rows]

# ── Incidents ─────────────────────────────────────────────────────


@app.post("/api/v1/incidents", status_code=201)
async def report_incident(
    zone: str = Form(..., min_length=1, max_length=50),
    severity: int = Form(..., ge=1, le=5),
    description: str = Form(..., min_length=1),
    immediate_action: Optional[str] = Form(None),
    occurred_at: Optional[datetime] = Form(None),
    photo: Optional[UploadFile] = File(None),
    user: User = Depends(current_user),
):
    data, kind = None, None
    if photo is not None and photo.filename:
        data = await photo.read(MAX_PHOTO_BYTES + 1)
        if len(data) > MAX_PHOTO_BYTES:
            raise HTTPException(413, "Photo larger than 10 MB")
        kind = detect_image_type(data[:16])
        if kind is None:
            raise HTTPException(415, "Photo must be a JPEG, PNG or WebP image")
    when = occurred_at or datetime.now(timezone.utc)
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                """INSERT INTO incidents (occurred_at, zone, severity, description, immediate_action, reported_by)
                   VALUES ($1,$2,$3,$4,$5,$6) RETURNING *""",
                when, zone, severity, description, immediate_action, user.username)
            if data is not None:
                # Server-generated name: nothing from the client reaches the path
                path = os.path.join(MEDIA_DIR, f"incident_{row['id']}{kind[1]}")
                with open(path, "wb") as f:
                    f.write(data)
                row = await conn.fetchrow(
                    "UPDATE incidents SET photo_path=$2, photo_mime=$3 WHERE id=$1 RETURNING *",
                    row["id"], path, kind[0])
    log.info(f"Incident {row['id']} (sev {severity}, {zone}) reported by {user.username}")
    return incident_out(row)


def incident_out(row) -> dict:
    d = dict(row)
    d["has_photo"] = bool(d.pop("photo_path"))
    d.pop("photo_mime", None)
    return d


@app.get("/api/v1/incidents", dependencies=[Depends(current_user)])
async def list_incidents(min_severity: int = Query(1, ge=1, le=5), limit: int = Query(100, ge=1, le=500)):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM incidents WHERE severity >= $1 ORDER BY occurred_at DESC LIMIT $2",
            min_severity, limit)
    return [incident_out(r) for r in rows]


@app.get("/api/v1/incidents/{incident_id}", dependencies=[Depends(current_user)])
async def get_incident(incident_id: int):
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM incidents WHERE id=$1", incident_id)
        if row is None:
            raise HTTPException(404, "Incident not found")
        repairs = await conn.fetch("SELECT * FROM repairs WHERE incident_id=$1 ORDER BY created_at", incident_id)
    return {**incident_out(row), "repairs": [dict(r) for r in repairs]}


@app.get("/api/v1/incidents/{incident_id}/photo", dependencies=[Depends(current_user)])
async def incident_photo(incident_id: int):
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT photo_path, photo_mime FROM incidents WHERE id=$1", incident_id)
    if row is None or not row["photo_path"] or not os.path.exists(row["photo_path"]):
        raise HTTPException(404, "No photo for this incident")
    return FileResponse(row["photo_path"], media_type=row["photo_mime"])

# ── Repairs ───────────────────────────────────────────────────────


@app.post("/api/v1/repairs", status_code=201)
async def log_repair(req: RepairIn, user: User = Depends(current_user)):
    async with pool.acquire() as conn:
        async with conn.transaction():
            # Resolve parts to item ids and total the quantity per item
            needed = {}
            for part in req.parts:
                if part.item_id is not None:
                    item_id = part.item_id
                else:
                    item_id = (await item_by_barcode(conn, part.barcode))["id"]
                needed[item_id] = needed.get(item_id, 0) + part.quantity
            # Lock in id order (no deadlocks between concurrent repairs) and check stock
            stock = {}
            for item_id in sorted(needed):
                row = await conn.fetchrow("SELECT * FROM inventory_items WHERE id=$1 FOR UPDATE", item_id)
                if row is None:
                    raise HTTPException(404, f"Part item {item_id} not found")
                stock[item_id] = row
            short = [f"{stock[i]['name']}: need {q}, have {stock[i]['quantity']} {stock[i]['unit']}"
                     for i, q in needed.items() if stock[i]["quantity"] < q]
            if short:
                raise HTTPException(409, "Not enough stock — " + "; ".join(short))

            repair = await conn.fetchrow(
                """INSERT INTO repairs (item_id, item_description, incident_id, repair_minutes,
                                        technician, signature, notes)
                   VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING *""",
                req.item_id, req.item_description, req.incident_id, req.repair_minutes,
                user.username, req.signature, req.notes)
            used = []
            for item_id, qty in needed.items():
                after = stock[item_id]["quantity"] - qty
                await conn.execute("INSERT INTO repair_parts (repair_id, item_id, quantity) VALUES ($1,$2,$3)",
                                   repair["id"], item_id, qty)
                await conn.execute("UPDATE inventory_items SET quantity=$2, updated_at=NOW() WHERE id=$1",
                                   item_id, after)
                await conn.execute(
                    """INSERT INTO inventory_transactions (item_id, delta, quantity_after, reason, repair_id, actor)
                       VALUES ($1,$2,$3,$4,$5,$6)""",
                    item_id, -qty, after, f"repair #{repair['id']}", repair["id"], user.username)
                used.append({"item_id": item_id, "name": stock[item_id]["name"],
                             "quantity": qty, "remaining": after})
    log.info(f"Repair {repair['id']} by {user.username}: {req.item_description}, {len(used)} part types")
    return {**dict(repair), "parts": used}


@app.get("/api/v1/repairs", dependencies=[Depends(current_user)])
async def list_repairs(limit: int = Query(100, ge=1, le=500)):
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM repairs ORDER BY created_at DESC LIMIT $1", limit)
        parts = await conn.fetch(
            """SELECT rp.repair_id, rp.quantity, i.name, i.barcode, i.unit FROM repair_parts rp
               JOIN inventory_items i ON i.id = rp.item_id WHERE rp.repair_id = ANY($1::bigint[])""",
            [r["id"] for r in rows])
    by_repair = {}
    for p in parts:
        by_repair.setdefault(p["repair_id"], []).append(dict(p))
    return [{**dict(r), "parts": by_repair.get(r["id"], [])} for r in rows]


@app.get("/health")
def health():
    return {"status": "ok", "service": "inventory-api"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8010)

"""
IMM-OS mission record API (/api/mission, in the backend service).

    GET    /api/mission                    the mission and its clock (the top bar polls this)
    GET    /api/mission/clock              the same for edge nodes (edge_device role): the Pi's SD recorder
    POST   /api/mission/start              start a mission: name, T0 (IST, default now), sols, crew
    PATCH  /api/mission                    rename, correct T0, change the number of sols
    POST   /api/mission/end                end the mission now
    GET    /api/mission/overview           sol cards: state, data coverage, dose
    GET    /api/mission/sol/{n}            one sol: min / mean / max per measurement, coverage, events
    GET    /api/mission/timeline           one measurement over the whole mission (10-min bands)
    GET    /api/mission/overlay            one measurement, each sol against hours into the sol
    GET    /api/mission/health             sensor × sol coverage grid
    GET    /api/mission/dose               cumulative radiation dose
    POST   /api/mission/events             a note in the mission log
    POST   /api/mission/download-token     short-lived token for the download links below
    GET    /api/mission/download/sol/{n}   ZIP: per-sensor CSVs (every reading, IST and UTC) + summary
    GET    /api/mission/download/mission   CSV: 1-minute mean / min / max of every sensor, every sol
    GET    /api/mission/report/{n}         printable sol report (print → PDF)

Statistics use good-quality readings only (warm-up and other suspect readings are stored but
left out); coverage counts every reading. Real sensors only unless ?sim=1.

The archiver writes each sol, 10 minutes after it ends, to MISSION_ARCHIVE_DIR/<mission>/sol-NN/
(the same ZIP contents, plus rollup.json and SHA256SUMS) and notes it in the mission log.
Point MISSION_ARCHIVE_DIR at an external drive to keep a copy off the MCC disk.
"""
import asyncio
import csv
import hashlib
import hmac
import io
import json
import logging
import os
import re
import shutil
import tempfile
import time
import zipfile
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from fastapi.responses import FileResponse, HTMLResponse
from pydantic import BaseModel, Field
from starlette.background import BackgroundTask

from services import mission as M
from services.auth import COMMANDER, MCC_OPERATOR, User, authenticated, crew_or_edge, current_user, require_roles
from services.health.streams import DEFAULT_PERIOD_S
from services.telemetry_api import PIPELINE_BUCKET, _get_client

log = logging.getLogger("mission")

ARCHIVE_DIR = os.getenv("MISSION_ARCHIVE_DIR", "/archive")
LIVE_TTL_S = 60             # the current sol's numbers are recomputed at most once a minute
SETTLE_S = 600              # a sol is final (cached, archived) 10 min after it ends: queued readings arrive
DL_TOKEN_TTL_S = 120

router = APIRouter(prefix="/api/mission", tags=["Mission"], dependencies=[Depends(current_user)])
downloads = APIRouter(prefix="/api/mission", tags=["Mission"])
planner = require_roles(COMMANDER, MCC_OPERATOR)


# ── Postgres: missions and the mission log ─────────────────────────

SCHEMA = """
CREATE TABLE IF NOT EXISTS missions (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    start_at TIMESTAMPTZ NOT NULL,
    sols INTEGER NOT NULL DEFAULT 7,
    crew INTEGER,
    notes TEXT NOT NULL DEFAULT '',
    created_by TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS mission_events (
    id BIGSERIAL PRIMARY KEY,
    mission_id BIGINT REFERENCES missions(id) ON DELETE CASCADE,
    at TIMESTAMPTZ NOT NULL DEFAULT now(),
    kind TEXT NOT NULL,
    message TEXT NOT NULL,
    actor TEXT,
    details JSONB NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS mission_events_at ON mission_events (mission_id, at);
CREATE TABLE IF NOT EXISTS mission_sols (
    mission_id BIGINT REFERENCES missions(id) ON DELETE CASCADE,
    sol INTEGER NOT NULL,
    archived_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    archive_path TEXT,
    readings BIGINT,
    summary JSONB NOT NULL DEFAULT '{}',
    PRIMARY KEY (mission_id, sol)
);
"""


def _ts(epoch: Optional[float]):
    return datetime.fromtimestamp(epoch, timezone.utc) if epoch is not None else None


def _mission(row) -> Optional[M.Mission]:
    if row is None:
        return None
    return M.Mission(id=row["id"], name=row["name"], start=row["start_at"].timestamp(), sols=row["sols"],
                     crew=row["crew"], notes=row["notes"] or "", created_by=row["created_by"],
                     ended_at=row["ended_at"].timestamp() if row["ended_at"] else None)


class MissionStore:
    def __init__(self):
        self.pool = None
        self.lock = asyncio.Lock()

    async def _pool(self):
        async with self.lock:
            if self.pool is None:
                import asyncpg
                try:
                    pool = await asyncpg.create_pool(
                        user=os.getenv("POSTGRES_USER", "admin"), password=os.getenv("POSTGRES_PASSWORD", "changeme"),
                        database=os.getenv("POSTGRES_DB", "imm_db"), host=os.getenv("POSTGRES_HOST", "postgres"),
                        port=int(os.getenv("POSTGRES_PORT", "5432")), min_size=1, max_size=4, timeout=5)
                    async with pool.acquire() as conn:
                        async with conn.transaction():
                            await conn.execute("SELECT pg_advisory_xact_lock(7471002)")
                            await conn.execute(SCHEMA)
                    self.pool = pool
                except Exception as exc:
                    raise HTTPException(503, f"mission database unavailable: {exc}")
            return self.pool

    async def current(self) -> Optional[M.Mission]:
        """The newest mission (running, upcoming or finished)."""
        pool = await self._pool()
        return _mission(await pool.fetchrow("SELECT * FROM missions ORDER BY id DESC LIMIT 1"))

    async def create(self, name, start, sols, crew, notes, actor) -> M.Mission:
        pool = await self._pool()
        row = await pool.fetchrow(
            "INSERT INTO missions (name, start_at, sols, crew, notes, created_by) VALUES ($1,$2,$3,$4,$5,$6) RETURNING *",
            name, _ts(start), sols, crew, notes or "", actor)
        return _mission(row)

    async def update(self, mid: int, **fields) -> M.Mission:
        pool = await self._pool()
        cols = {"name": "name", "start": "start_at", "sols": "sols", "crew": "crew", "notes": "notes",
                "ended_at": "ended_at"}
        sets, args = [], [mid]
        for k, v in fields.items():
            args.append(_ts(v) if k in ("start", "ended_at") else v)
            sets.append(f"{cols[k]} = ${len(args)}")
        row = await pool.fetchrow(f"UPDATE missions SET {', '.join(sets)} WHERE id = $1 RETURNING *", *args)
        return _mission(row)

    async def event(self, mid: int, kind: str, message: str, actor: Optional[str] = None,
                    at: Optional[float] = None, details: Optional[dict] = None) -> None:
        pool = await self._pool()
        await pool.execute("INSERT INTO mission_events (mission_id, at, kind, message, actor, details) "
                           "VALUES ($1, COALESCE($2, now()), $3, $4, $5, $6)",
                           mid, _ts(at), kind, message, actor, json.dumps(details or {}))

    async def events(self, mid: int, start: float, end: float) -> List[dict]:
        """The mission log and the alarm history between start and end, newest first."""
        pool = await self._pool()
        rows = [{"at": r["at"].timestamp(), "kind": r["kind"], "message": r["message"], "actor": r["actor"]}
                for r in await pool.fetch("SELECT at, kind, message, actor FROM mission_events "
                                          "WHERE mission_id = $1 AND at >= $2 AND at < $3", mid, _ts(start), _ts(end))]
        try:
            rows += [{"at": r["at"].timestamp(), "kind": f"alarm.{r['event']}", "severity": r["severity"],
                      "message": r["message"], "actor": r["actor"]}
                     for r in await pool.fetch(
                         "SELECT at, event, severity, message, actor FROM alarm_events WHERE at >= $1 AND at < $2 "
                         "AND event IN ('raised', 'escalated', 'cleared', 'acked') ORDER BY at DESC LIMIT 300",
                         _ts(start), _ts(end))]
        except Exception:             # health monitor never ran here: no alarm tables yet
            pass
        return sorted(rows, key=lambda r: -r["at"])

    async def archived(self, mid: int) -> Dict[int, dict]:
        pool = await self._pool()
        return {r["sol"]: {"archived_at": r["archived_at"].timestamp(), "path": r["archive_path"],
                           "readings": r["readings"]}
                for r in await pool.fetch("SELECT * FROM mission_sols WHERE mission_id = $1", mid)}

    async def mark_archived(self, mid: int, sol: int, path: str, readings: int, summary: dict) -> None:
        pool = await self._pool()
        await pool.execute("INSERT INTO mission_sols (mission_id, sol, archive_path, readings, summary) "
                           "VALUES ($1,$2,$3,$4,$5) ON CONFLICT (mission_id, sol) DO UPDATE SET "
                           "archived_at = now(), archive_path = $3, readings = $4, summary = $5",
                           mid, sol, path, readings, json.dumps(summary, default=str))


store = MissionStore()


async def mission_or_404() -> M.Mission:
    m = await store.current()
    if m is None:
        raise HTTPException(404, "no mission yet: start one on the Mission page")
    return m


# ── InfluxDB: 1-minute rollups of a sol ─────────────────────────────

def _iso(t: float) -> str:
    return datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


_EPOCH_CACHE: Dict[str, float] = {}


def _epoch(ts: str) -> float:
    """InfluxDB RFC3339 (…T22:47:23.042Z, up to 9 fraction digits) → epoch seconds."""
    base, _, frac = ts.rstrip("Z").partition(".")
    t = _EPOCH_CACHE.get(base)
    if t is None:
        if len(_EPOCH_CACHE) > 200000:
            _EPOCH_CACHE.clear()
        t = _EPOCH_CACHE[base] = datetime.strptime(base, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc).timestamp()
    return t + (float("0." + frac) if frac else 0.0)


_CONVERT = {"double": float, "long": int, "unsignedLong": int, "boolean": lambda x: x == "true"}


def flux_rows(flux: str):
    """Rows of a Flux query as dicts, from InfluxDB's annotated CSV: several times faster than
    the client's record objects for the tens of thousands of rows a sol has. Numbers are
    converted; times stay RFC3339 strings (see _epoch); empty cells are None."""
    with _get_client() as client:
        raw = client.query_api().query_raw(flux)
        text = raw.data.decode("utf-8") if hasattr(raw, "data") else raw.read().decode("utf-8")
    types, defaults, header, conv = None, None, None, None
    for line in csv.reader(io.StringIO(text)):
        if not line or not any(line):
            header = None
            continue
        if line[0].startswith("#"):
            if line[0] == "#datatype":
                types, header = line, None
            elif line[0] == "#default":                    # e.g. the yield name for the "result" column
                defaults = line
            continue
        if header is None:
            header = line
            conv = [_CONVERT.get(t) for t in (types or [])] + [None] * len(line)
            dflt = (defaults or []) + [""] * len(line)
            continue
        row = {}
        for i, (h, v) in enumerate(zip(header, line)):
            if not h:
                continue
            if v == "":
                v = dflt[i]
            row[h] = None if v == "" else (conv[i](v) if conv[i] else v)
        yield row


def _flux_rollup(start: float, end: float, real_only: bool) -> str:
    """1-minute aggregates straight from storage (range → filter → aggregateWindow is pushed down
    into InfluxDB's storage engine: fast even for a sol of 1 Hz readings). Series are merged in Python."""
    sim = 'r.simulated == "false" and ' if real_only else ""
    pairs = " or ".join(f'(r._measurement == "{s}" and r.metric == "{m}")' for s, m in M.VALUE_PAIRS)
    presence = " or ".join(f'(r._measurement == "{s}" and r.metric == "{m}")' for s, m in M.PRESENCE_METRIC.items())
    warm = " or ".join(f'r._measurement == "{s}"' for s in M.WARMING_SENSORS)
    rng = f'from(bucket: "{PIPELINE_BUCKET}") |> range(start: {_iso(start)}, stop: {_iso(end)})'
    out = []
    for fn in ("mean", "min", "max", "count"):
        out.append(f'{rng} |> filter(fn: (r) => {sim}r._field == "value" and ({pairs})) '
                   f'|> aggregateWindow(every: 1m, fn: {fn}, createEmpty: false, timeSrc: "_start") |> yield(name: "{fn}")')
    out.append(f'{rng} |> filter(fn: (r) => {sim}r._field == "value" and ({presence})) '
               f'|> aggregateWindow(every: 1m, fn: count, createEmpty: false, timeSrc: "_start") |> yield(name: "presence")')
    out.append(f'{rng} |> filter(fn: (r) => {sim}r._field == "value" and r.metric == "warming" and ({warm})) '
               f'|> aggregateWindow(every: 1m, fn: max, createEmpty: false, timeSrc: "_start") |> yield(name: "warming")')
    return "\n".join(out)


def query_rollup(start: float, end: float, real_only: bool = True) -> Tuple[M.Values, M.Counts]:
    """Blocking: one sol (or part of it) → (values, counts). Minutes in which a sensor was warming
    up are left out of its values (its readings are stored, flagged suspect)."""
    vrows, crows, warming = [], [], set()
    for v in flux_rows(_flux_rollup(start, end, real_only)):
        row = {"result": v.get("result"), "sensor": v.get("_measurement"), "metric": v.get("metric"),
               "node_id": v.get("node_id") or "-", "zone": v.get("zone") or "-",
               "t": _epoch(v["_time"]), "value": v.get("_value"),
               # the whole tag set: one sensor can be several series (e.g. the processor's daily tags)
               "series": tuple(sorted((k, str(x)) for k, x in v.items()
                                      if not k.startswith("_") and k not in ("result", "table")))}
        if row["result"] == "presence":
            crows.append(row)
        elif row["result"] == "warming":
            if row["value"]:
                warming.add((row["sensor"], int(row["t"])))
        else:
            vrows.append(row)
    return M.values_from_series_rows(vrows, warming), M.counts_from_rows(crows)


def _slug(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")[:40] or "mission"


def archive_dir(m: M.Mission) -> str:
    return os.path.join(ARCHIVE_DIR, f"{_slug(m.name)}-{m.id}")


_cache: Dict[tuple, Tuple[float, M.Values, M.Counts]] = {}


async def sol_rollup(m: M.Mission, n: int, real_only: bool = True, now: Optional[float] = None
                     ) -> Tuple[M.Values, M.Counts]:
    now = now or time.time()
    s, e = M.sol_bounds(m, n)
    if now < s:
        return {}, {}
    key = (m.id, m.start, n, real_only)
    hit = _cache.get(key)
    final = now >= e + SETTLE_S
    if hit and (hit[0] >= e + SETTLE_S or now - hit[0] < LIVE_TTL_S):
        return hit[1], hit[2]
    if final and real_only and hit is None:        # archived earlier (maybe before a restart)
        path = os.path.join(archive_dir(m), f"sol-{n:02d}", "rollup.json")
        if os.path.exists(path):
            with open(path) as f:
                values, counts = M.rollup_from_json(json.load(f))
            _cache[key] = (now, values, counts)
            return values, counts
    end = min(e, now, m.ended_at or e)
    try:
        values, counts = await asyncio.to_thread(query_rollup, s, end, real_only)
    except Exception as exc:
        if hit:
            return hit[1], hit[2]
        raise HTTPException(503, f"InfluxDB unavailable: {exc}")
    _cache[key] = (now, values, counts)
    return values, counts


def started_sols(m: M.Mission, now: float) -> List[int]:
    last = M.sol_of(m, min(now, m.ended_at or now))
    return list(range(1, min(last, m.sols) + 1))


async def all_values(m: M.Mission, real_only: bool, now: float) -> Dict[int, M.Values]:
    out = {}
    for n in started_sols(m, now):
        out[n] = (await sol_rollup(m, n, real_only, now))[0]
    return out


def periods() -> Dict[str, float]:
    return dict(DEFAULT_PERIOD_S)


# ── endpoints ──────────────────────────────────────────────────────

class StartBody(BaseModel):
    name: str = Field(..., min_length=1, max_length=80)
    start_ist: Optional[str] = Field(None, description="T0 in IST, e.g. 2026-10-01 06:00 (default: now)")
    sols: int = Field(7, ge=1, le=60)
    crew: Optional[int] = Field(None, ge=0, le=50)
    notes: str = Field("", max_length=2000)


class PatchBody(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=80)
    start_ist: Optional[str] = None
    sols: Optional[int] = Field(None, ge=1, le=60)
    crew: Optional[int] = Field(None, ge=0, le=50)
    notes: Optional[str] = Field(None, max_length=2000)


class NoteBody(BaseModel):
    message: str = Field(..., min_length=1, max_length=500)


def _start_from(text: Optional[str], now: float) -> float:
    if not text:
        return now
    try:
        return M.parse_ist(text)
    except ValueError:
        raise HTTPException(422, "start_ist: use YYYY-MM-DD HH:MM (IST)")


@router.get("")
async def get_mission():
    now = time.time()
    m = await store.current()
    return {"now": now, "mission": m.to_json() if m else None, "clock": M.clock(m, now),
            "sols": M.sol_states(m, now) if m else []}


@downloads.get("/clock")
async def mission_clock(user: User = Depends(crew_or_edge)):
    """The mission and its clock for edge nodes too (the Pi's SD recorder files readings by sol)."""
    now = time.time()
    m = await store.current()
    return {"now": now, "mission": m.to_json() if m else None, "clock": M.clock(m, now)}


@router.post("/start")
async def start_mission(body: StartBody, user: User = Depends(planner)):
    now = time.time()
    m = await store.current()
    if m and M.clock(m, now)["phase"] in ("pre", "active"):
        raise HTTPException(409, f"mission '{m.name}' is not over: end it first")
    start = _start_from(body.start_ist, now)
    m = await store.create(body.name.strip(), start, body.sols, body.crew, body.notes, user.username)
    await store.event(m.id, "mission", f"Mission {m.name} created: Sol 1 starts {M.ist(m.start)}", user.username)
    return {"mission": m.to_json(), "clock": M.clock(m, now)}


@router.patch("")
async def edit_mission(body: PatchBody, user: User = Depends(planner)):
    m = await mission_or_404()
    fields = {k: v for k, v in body.dict().items() if v is not None and k != "start_ist"}
    if body.start_ist:
        fields["start"] = _start_from(body.start_ist, time.time())
    if not fields:
        raise HTTPException(422, "nothing to change")
    new = await store.update(m.id, **fields)
    what = ", ".join(f"{k} → {M.ist(v) if k == 'start' else v}" for k, v in fields.items())
    await store.event(m.id, "mission", f"Mission edited: {what}", user.username)
    _cache.clear()
    return {"mission": new.to_json(), "clock": M.clock(new, time.time())}


@router.post("/end")
async def end_mission(user: User = Depends(planner)):
    m = await mission_or_404()
    now = time.time()
    if M.clock(m, now)["phase"] not in ("pre", "active"):
        raise HTTPException(409, "the mission is already over")
    new = await store.update(m.id, ended_at=now)
    await store.event(m.id, "mission", f"Mission ended by {user.username} on Sol {M.sol_of(m, now)}", user.username)
    _cache.clear()
    return {"mission": new.to_json(), "clock": M.clock(new, now)}


@router.post("/events")
async def add_note(body: NoteBody, user: User = Depends(current_user)):
    m = await mission_or_404()
    await store.event(m.id, "note", body.message.strip(), user.username)
    return {"ok": True}


@router.get("/overview")
async def overview(sim: bool = False):
    now = time.time()
    m = await mission_or_404()
    archived = await store.archived(m.id)
    cards = M.sol_states(m, now)
    per_sol, cov = {}, {}
    for c in cards:
        if c["state"] == "upcoming":
            continue
        values, counts = await sol_rollup(m, c["sol"], not sim, now)
        per_sol[c["sol"]] = values
        s = M.summary(m, c["sol"], values, counts, now, periods())
        cov[c["sol"]] = s["coverage"]
        c.update(readings=s["readings"], dose_usv=s["dose_usv"], archived=c["sol"] in archived)
    _, overall = M.mission_coverage(cov)
    for c in cards:
        if c["sol"] in overall:
            c["coverage_pct"] = overall[c["sol"]]
    dose = M.dose_series(m, per_sol)
    return {"mission": m.to_json(), "clock": M.clock(m, now), "sols": cards, "dose": dose,
            "archive_dir": archive_dir(m)}


@router.get("/sol/{n}")
async def sol_detail(n: int, sim: bool = False):
    now = time.time()
    m = await mission_or_404()
    if not 1 <= n <= m.sols:
        raise HTTPException(404, f"no Sol {n} in this mission")
    values, counts = await sol_rollup(m, n, not sim, now)
    s = M.summary(m, n, values, counts, now, periods())
    s["events"] = await store.events(m.id, s["start"] - (3600 if n == 1 else 0), s["end"])
    series = {}
    for key, *_rest in M.MEASUREMENTS:
        src = s["measurements"][key]["source"]
        if src:
            series[key] = [[p[0], p[1]] for p in M.buckets(values[tuple(src.split("."))], s["start"], 600)]
    s["series"] = series
    return s


def _measurement(key: str) -> str:
    if key not in M.MEASUREMENT_KEYS:
        raise HTTPException(422, f"measurement is one of {', '.join(M.MEASUREMENT_KEYS)}")
    return key


@router.get("/timeline")
async def get_timeline(measurement: str = "co2", sim: bool = False):
    now = time.time()
    m = await mission_or_404()
    return {**M.timeline(m, await all_values(m, not sim, now), _measurement(measurement)),
            "now_h": (min(now, m.end) - m.start) / 3600, "sols": m.sols}


@router.get("/overlay")
async def get_overlay(measurement: str = "temperature", sim: bool = False):
    now = time.time()
    m = await mission_or_404()
    return M.overlay(m, await all_values(m, not sim, now), _measurement(measurement))


@router.get("/health")
async def get_health(sim: bool = False):
    now = time.time()
    m = await mission_or_404()
    cov = {}
    for n in started_sols(m, now):
        values, counts = await sol_rollup(m, n, not sim, now)
        cov[n] = M.summary(m, n, values, counts, now, periods())["coverage"]
    rows, overall = M.mission_coverage(cov)
    return {"sols": m.sols, "current": M.clock(m, now).get("sol"), "rows": rows, "overall": overall}


@router.get("/dose")
async def get_dose(sim: bool = False):
    now = time.time()
    m = await mission_or_404()
    return M.dose_series(m, await all_values(m, not sim, now))


# ── downloads (a browser link can't carry the login header: a signed short-lived token) ──

def _secret() -> bytes:
    return (os.getenv("IMM_SERVICE_TOKEN") or "imm-dev-download-secret").encode()


def make_token(username: str, now: Optional[float] = None) -> str:
    exp = int((now or time.time()) + DL_TOKEN_TTL_S)
    sig = hmac.new(_secret(), f"{exp}:{username}".encode(), hashlib.sha256).hexdigest()[:40]
    return f"{exp}.{sig}.{username}"


def check_token(token: str, now: Optional[float] = None) -> Optional[str]:
    try:
        exp, sig, username = token.split(".", 2)
        good = hmac.new(_secret(), f"{exp}:{username}".encode(), hashlib.sha256).hexdigest()[:40]
        if hmac.compare_digest(sig, good) and int(exp) >= (now or time.time()):
            return username
    except ValueError:
        pass
    return None


def download_user(dl: Optional[str] = Query(None), authorization: Optional[str] = Header(None),
                  x_imm_service_token: Optional[str] = Header(None)) -> str:
    if dl:
        who = check_token(dl)
        if not who:
            raise HTTPException(401, "download link expired: download again from the Mission page")
        return who
    return current_user(authenticated(authorization, x_imm_service_token)).username


@router.post("/download-token")
async def download_token(user: User = Depends(current_user)):
    return {"token": make_token(user.username), "expires_in": DL_TOKEN_TTL_S}


def _q_by_time(sensor: str, start: float, end: float, real_only: bool) -> Dict[tuple, str]:
    metric = M.PRESENCE_METRIC.get(sensor)
    if not metric:
        return {}
    sim = 'r.simulated == "false" and ' if real_only else ""
    flux = (f'from(bucket: "{PIPELINE_BUCKET}") |> range(start: {_iso(start)}, stop: {_iso(end)}) '
            f'|> filter(fn: (r) => {sim}r._measurement == "{sensor}" and r.metric == "{metric}" and r._field == "q")')
    return {(r["_time"], r.get("node_id"), r.get("zone")): r.get("_value") for r in flux_rows(flux)}


def write_sol_csvs(m: M.Mission, n: int, target: str, real_only: bool = True) -> Dict[str, int]:
    """Blocking: every reading of the sol, one CSV per sensor (wide: a column per metric). → rows per file."""
    s, e = M.sol_bounds(m, n)
    end = min(e, time.time(), m.ended_at or e)
    os.makedirs(target, exist_ok=True)
    sim = 'r.simulated == "false" and ' if real_only else ""
    written: Dict[str, int] = {}
    with _get_client() as client:
        sensors = [r.get_value() for t in client.query_api().query(
            f'import "influxdata/influxdb/schema"\nschema.measurements(bucket: "{PIPELINE_BUCKET}", '
            f'start: {_iso(s)}, stop: {_iso(end)})') for r in t.records]
    for sensor in sorted(x for x in sensors if x != "anomaly"):
        q = _q_by_time(sensor, s, end, real_only)
        flux = (f'from(bucket: "{PIPELINE_BUCKET}") |> range(start: {_iso(s)}, stop: {_iso(end)}) '
                f'|> filter(fn: (r) => {sim}r._measurement == "{sensor}" and r._field == "value") '
                f'|> keep(columns: ["_time", "_value", "metric", "node_id", "zone"]) '
                f'|> group(columns: ["node_id", "zone"]) '
                f'|> pivot(rowKey: ["_time"], columnKey: ["metric"], valueColumn: "_value")')
        rows = list(flux_rows(flux))
        if not rows:
            continue
        metrics = sorted({k for r in rows for k in r if not k.startswith("_") and k not in
                          ("result", "table", "node_id", "zone")})
        path = os.path.join(target, f"{sensor}.csv")
        with open(path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["time_ist", "time_utc", "sol", "node_id", "zone", *metrics, "quality"])
            for r in rows:
                ts = _epoch(r["_time"])
                w.writerow([M.ist(ts, "%Y-%m-%d %H:%M:%S.%f")[:-3],
                            datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
                            n, r.get("node_id"), r.get("zone"),
                            *["" if r.get(k) is None else r.get(k) for k in metrics],
                            q.get((r["_time"], r.get("node_id"), r.get("zone")), "")])
        written[f"{sensor}.csv"] = len(rows)
    return written


def _sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _readme(m: M.Mission, n: int) -> str:
    s, e = M.sol_bounds(m, n)
    return (f"{m.name} · Sol {n}\n{M.ist(s)} → {M.ist(e)}\n\n"
            "One CSV per sensor: time in IST and UTC, the node and zone, one column per metric, and the\n"
            "reading's quality (good / suspect / bad; suspect includes warm-up). summary.json holds the sol's\n"
            "statistics (good readings only), data coverage and the mission log; SHA256SUMS the checksums.\n")


async def build_sol(m: M.Mission, n: int, target: str, real_only: bool = True) -> dict:
    """The sol's files in target/: CSVs, summary.json, rollup.json, README.txt, SHA256SUMS."""
    now = time.time()
    files = await asyncio.to_thread(write_sol_csvs, m, n, os.path.join(target, "readings"), real_only)
    values, counts = await sol_rollup(m, n, real_only, now)
    summary = M.summary(m, n, values, counts, now, periods())
    summary["mission"] = m.to_json()
    try:
        summary["events"] = await store.events(m.id, summary["start"], summary["end"])
    except HTTPException:
        summary["events"] = []
    with open(os.path.join(target, "summary.json"), "w") as f:
        json.dump(summary, f, indent=1, default=str)
    with open(os.path.join(target, "rollup.json"), "w") as f:
        json.dump(M.rollup_to_json(values, counts), f)
    with open(os.path.join(target, "README.txt"), "w") as f:
        f.write(_readme(m, n))
    sums = []
    for root, _, names in os.walk(target):
        for name in sorted(names):
            if name != "SHA256SUMS":
                p = os.path.join(root, name)
                sums.append(f"{_sha256(p)}  {os.path.relpath(p, target)}")
    with open(os.path.join(target, "SHA256SUMS"), "w") as f:
        f.write("\n".join(sorted(sums, key=lambda x: x.split('  ', 1)[1])) + "\n")
    return {"files": files, "readings": sum(files.values()), "summary": summary}


def _zip_dir(src: str, zip_path: str, prefix: str) -> None:
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, names in os.walk(src):
            for name in sorted(names):
                p = os.path.join(root, name)
                z.write(p, os.path.join(prefix, os.path.relpath(p, src)))


@downloads.get("/download/sol/{n}")
async def download_sol(n: int, sim: bool = False, who: str = Depends(download_user)):
    m = await mission_or_404()
    if not 1 <= n <= m.sols or time.time() < M.sol_bounds(m, n)[0]:
        raise HTTPException(404, f"Sol {n} has no data yet")
    name = f"{_slug(m.name)}-sol-{n:02d}"
    tmp = tempfile.mkdtemp(prefix="imm-sol-")
    archived = os.path.join(archive_dir(m), f"sol-{n:02d}")
    src = archived if os.path.isdir(archived) and not sim else os.path.join(tmp, name)
    if src != archived:
        await build_sol(m, n, src, not sim)
    zip_path = os.path.join(tmp, f"{name}.zip")
    await asyncio.to_thread(_zip_dir, src, zip_path, name)
    log.info("Sol %d downloaded by %s", n, who)
    return FileResponse(zip_path, filename=f"{name}.zip", media_type="application/zip",
                        background=BackgroundTask(shutil.rmtree, tmp, True))


def write_mission_csv(m: M.Mission, path: str, real_only: bool = True) -> int:
    """Blocking: 1-minute mean / min / max / count of every metric of every sensor, all sols."""
    now = time.time()
    sim = 'r.simulated == "false" and ' if real_only else ""
    n_rows = 0
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["sol", "time_ist", "time_utc", "node_id", "zone", "sensor", "metric", "mean", "min", "max", "readings"])
        for n in started_sols(m, now):
            s, e = M.sol_bounds(m, n)
            end = min(e, now, m.ended_at or e)
            rng = (f'from(bucket: "{PIPELINE_BUCKET}") |> range(start: {_iso(s)}, stop: {_iso(end)}) '
                   f'|> filter(fn: (r) => {sim}r._field == "value" and r._measurement != "anomaly")')
            # range → filter → aggregateWindow runs inside InfluxDB's storage engine; the series one
            # sensor is split into (daily tags) are merged below
            flux = "\n".join(f'{rng} |> aggregateWindow(every: 1m, fn: {fn}, createEmpty: false, timeSrc: "_start") '
                              f'|> yield(name: "{fn}")' for fn in ("mean", "min", "max", "count"))
            per_series: Dict[tuple, dict] = {}
            for v in flux_rows(flux):
                series = tuple(sorted((k, str(x)) for k, x in v.items() if not k.startswith("_") and k not in ("result", "table")))
                per_series.setdefault((v["_time"], v.get("node_id"), v.get("zone"), v.get("_measurement"),
                                       v.get("metric"), series), {})[v.get("result")] = v.get("_value")
            cells: Dict[tuple, list] = {}
            for (t, node, zone, sensor, metric, _), c in per_series.items():
                cnt = c.get("count") or 0
                if not cnt or c.get("mean") is None:
                    continue
                cell = cells.get((t, node, zone, sensor, metric))
                if cell is None:
                    cells[(t, node, zone, sensor, metric)] = [c["mean"], c.get("min"), c.get("max"), cnt]
                else:
                    total = cell[3] + cnt
                    cell[0] = (cell[0] * cell[3] + c["mean"] * cnt) / total
                    cell[1] = min(x for x in (cell[1], c.get("min")) if x is not None)
                    cell[2] = max(x for x in (cell[2], c.get("max")) if x is not None)
                    cell[3] = total
            for (t, node, zone, sensor, metric), (mean, lo, hi, cnt) in sorted(cells.items(), key=lambda kv: (kv[0][0], kv[0][3], kv[0][4])):
                te = _epoch(t)
                w.writerow([n, M.ist(te, "%Y-%m-%d %H:%M"), datetime.fromtimestamp(te, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                            node, zone, sensor, metric, mean, lo, hi, cnt])
                n_rows += 1
    return n_rows


@downloads.get("/download/mission")
async def download_mission(sim: bool = False, who: str = Depends(download_user)):
    m = await mission_or_404()
    tmp = tempfile.mkdtemp(prefix="imm-mission-")
    name = f"{_slug(m.name)}-all-sols-1min.csv"
    path = os.path.join(tmp, name)
    try:
        await asyncio.to_thread(write_mission_csv, m, path, not sim)
    except Exception as exc:
        shutil.rmtree(tmp, True)
        raise HTTPException(503, f"InfluxDB unavailable: {exc}")
    log.info("whole mission downloaded by %s", who)
    return FileResponse(path, filename=name, media_type="text/csv", background=BackgroundTask(shutil.rmtree, tmp, True))


# ── printable sol report ───────────────────────────────────────────

def _fmt(v, dp):
    return "–" if v is None else f"{v:.{dp}f}"


def report_html(m: M.Mission, s: dict) -> str:
    esc = lambda x: (str(x).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))  # noqa: E731
    rows = "".join(
        f"<tr><td>{esc(x['label'])}</td><td>{_fmt(x.get('mean'), x['dp'])}</td>"
        f"<td>{_fmt(x.get('min'), x['dp'])}<small>{M.ist(x.get('min_t'), ' %H:%M') or ''}</small></td>"
        f"<td>{_fmt(x.get('max'), x['dp'])}<small>{M.ist(x.get('max_t'), ' %H:%M') or ''}</small></td>"
        f"<td>{esc(x['unit'])}</td><td class=m>{esc(x.get('source') or 'no data')}</td></tr>"
        for x in s["measurements"].values() if x.get("source"))
    cov = "".join(f"<tr><td>{esc(c['sensor'])}</td><td class=m>{esc(c['node_id'])} · {esc(c['zone'])}</td>"
                  f"<td>{_fmt(c['pct'], 1)} %</td><td>{c['readings']:,}</td></tr>" for c in s["coverage"])
    ev = "".join(f"<tr><td class=m>{M.ist(e['at'], '%d %b %H:%M:%S')}</td><td>{esc(e.get('severity') or e['kind'])}</td>"
                 f"<td>{esc(e['message'])}</td><td>{esc(e.get('actor') or '')}</td></tr>" for e in s["events"][:200])
    dose = "–" if s["dose_usv"] is None else f"{s['dose_usv']:.2f} µSv"
    return f"""<!doctype html><html><head><meta charset=utf-8><title>{esc(m.name)} · Sol {s['sol']} report</title>
<style>body{{font:13px/1.45 system-ui,sans-serif;color:#111;margin:28px;max-width:980px}}h1{{font-size:22px;margin:0}}
h2{{font-size:14px;letter-spacing:.08em;text-transform:uppercase;margin:22px 0 6px;border-bottom:2px solid #111}}
table{{border-collapse:collapse;width:100%}}td,th{{border-bottom:1px solid #ddd;padding:4px 6px;text-align:left}}
th{{font-size:11px;text-transform:uppercase;color:#555}}small{{color:#666;margin-left:6px}}.m{{font-family:monospace;font-size:12px}}
.k{{display:inline-block;margin-right:28px}}.k b{{display:block;font-size:18px}}@media print{{button{{display:none}}}}</style></head><body>
<button onclick="print()" style="float:right">Print / save as PDF</button>
<h1>{esc(m.name)} · Sol {s['sol']}{' (in progress)' if s['live'] else ''}</h1>
<div>{esc(s['start_ist'])} → {esc(s['end_ist'])}</div>
<p><span class=k>Readings<b>{s['readings']:,}</b></span><span class=k>Data coverage<b>{_fmt(s['coverage_pct'], 1)} %</b></span>
<span class=k>Radiation dose<b>{dose}</b></span><span class=k>Report made<b>{M.ist(time.time(), '%d %b %H:%M IST')}</b></span></p>
<h2>Habitat (good readings only)</h2><table><tr><th>Measurement</th><th>Mean</th><th>Min (IST)</th><th>Max (IST)</th><th>Unit</th><th>Source</th></tr>{rows}</table>
<h2>Data coverage</h2><table><tr><th>Sensor</th><th>Node · zone</th><th>Coverage</th><th>Readings</th></tr>{cov}</table>
<h2>Mission log and alarms</h2><table><tr><th>IST</th><th>Kind</th><th>Event</th><th>By</th></tr>{ev or '<tr><td colspan=4>none</td></tr>'}</table>
</body></html>"""


@downloads.get("/report/{n}", response_class=HTMLResponse)
async def sol_report(n: int, sim: bool = False, who: str = Depends(download_user)):
    m = await mission_or_404()
    if not 1 <= n <= m.sols:
        raise HTTPException(404, f"no Sol {n} in this mission")
    now = time.time()
    values, counts = await sol_rollup(m, n, not sim, now)
    s = M.summary(m, n, values, counts, now, periods())
    s["events"] = await store.events(m.id, s["start"], s["end"])
    return HTMLResponse(report_html(m, s))


# ── the archiver ───────────────────────────────────────────────────

async def archive_due(now: Optional[float] = None) -> List[int]:
    """Archive every finished sol not archived yet. → the sols archived now."""
    now = now or time.time()
    m = await store.current()
    if m is None:
        return []
    done = await store.archived(m.id)
    out = []
    for n in range(1, m.sols + 1):
        s, e = M.sol_bounds(m, n)
        end = min(e, m.ended_at or e)
        if n in done or s >= (m.ended_at or e) or now < end + SETTLE_S:
            continue
        final = os.path.join(archive_dir(m), f"sol-{n:02d}")
        partial = final + ".partial"
        shutil.rmtree(partial, True)
        result = await build_sol(m, n, partial)
        shutil.rmtree(final, True)
        os.replace(partial, final)
        await store.mark_archived(m.id, n, final, result["readings"], result["summary"])
        await store.event(m.id, "archive",
                          f"Sol {n} archived · {result['readings']:,} readings in {len(result['files'])} files · "
                          f"SHA-256 recorded · {final}", at=time.time())
        log.info("Sol %d archived to %s (%d readings)", n, final, result["readings"])
        out.append(n)
    return out


async def archiver_loop(every_s: float = 120) -> None:
    while True:
        try:
            await archive_due()
        except HTTPException as exc:
            log.warning("archiver: %s", exc.detail)
        except Exception:
            log.exception("archiver failed; retrying")
        await asyncio.sleep(every_s)


_archiver: Optional[asyncio.Task] = None


async def start_archiver() -> None:
    global _archiver
    if os.getenv("MISSION_ARCHIVER", "1") == "1" and _archiver is None:
        _archiver = asyncio.create_task(archiver_loop())

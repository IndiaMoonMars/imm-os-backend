"""
Health monitor persistence (Postgres): alarms, their events, EVA crew tracking and
the stream registry. The monitor owns these tables and creates them on connect
(SCHEMA below, idempotent), so an existing database needs no manual migration.

Writes go through one ordered queue, so an alarm's insert always lands before its
later updates. If Postgres is down the monitor keeps working from memory and the
queue (bounded) is retried; the alarms themselves never wait for the database.
"""
import asyncio
import json
import logging
import os
from collections import deque
from datetime import datetime, timezone
from typing import List, Optional

from services.health.alarms import Alarm
from services.health.streams import StreamKey

log = logging.getLogger("health_store")
MAX_PENDING = 5000

SCHEMA = """
CREATE TABLE IF NOT EXISTS alarms (
    id BIGSERIAL PRIMARY KEY,
    alarm_key TEXT NOT NULL,
    severity TEXT NOT NULL,
    category TEXT NOT NULL,
    source TEXT,
    message TEXT NOT NULL,
    state TEXT NOT NULL,                 -- active | cleared | rtn | closed (+ acked flag)
    acked BOOLEAN NOT NULL DEFAULT FALSE,
    acked_at TIMESTAMPTZ,
    acked_by TEXT,
    cleared_at TIMESTAMPTZ,
    closed_at TIMESTAMPTZ,
    value DOUBLE PRECISION,
    unverified BOOLEAN NOT NULL DEFAULT FALSE,
    simulated BOOLEAN NOT NULL DEFAULT FALSE,
    raise_count INTEGER NOT NULL DEFAULT 1,
    details JSONB NOT NULL DEFAULT '{}',
    raised_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- one open alarm per condition
CREATE UNIQUE INDEX IF NOT EXISTS alarms_open_key ON alarms (alarm_key) WHERE state <> 'closed';
CREATE INDEX IF NOT EXISTS alarms_raised_at ON alarms (raised_at DESC);

CREATE TABLE IF NOT EXISTS alarm_events (
    id BIGSERIAL PRIMARY KEY,
    alarm_id BIGINT REFERENCES alarms(id) ON DELETE SET NULL,
    alarm_key TEXT NOT NULL,
    event TEXT NOT NULL,                 -- raised, escalated, cleared, acked, closed, note, ...
    severity TEXT,
    message TEXT,
    value DOUBLE PRECISION,
    actor TEXT,
    details JSONB,
    at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS alarm_events_at ON alarm_events (at DESC);

CREATE TABLE IF NOT EXISTS eva_crew_status (
    crew_id TEXT PRIMARY KEY,
    armed BOOLEAN NOT NULL,
    armed_by TEXT,
    state TEXT NOT NULL,
    last_contact TIMESTAMPTZ,
    last_position JSONB,
    last_vitals JSONB,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS component_status (
    node_id TEXT NOT NULL,
    component TEXT NOT NULL,
    state TEXT NOT NULL,
    reason TEXT,
    interval_s DOUBLE PRECISION,
    details JSONB,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (node_id, component)
);

CREATE TABLE IF NOT EXISTS telemetry_streams (
    stream_key TEXT PRIMARY KEY,
    node_id TEXT NOT NULL,
    sensor TEXT NOT NULL,
    zone TEXT NOT NULL,
    crew_id TEXT,
    simulated BOOLEAN NOT NULL,
    expected_period_s DOUBLE PRECISION,
    first_seen TIMESTAMPTZ,
    last_seen TIMESTAMPTZ
);
"""


def _ts(t: Optional[float]):
    return None if t is None else datetime.fromtimestamp(t, tz=timezone.utc)


def _epoch(d) -> Optional[float]:
    return None if d is None else d.timestamp()


class HealthStore:
    def __init__(self):
        self.pool = None
        self.pending = deque(maxlen=MAX_PENDING)
        self.ok = False
        self.error: Optional[str] = None

    async def connect(self) -> bool:
        import asyncpg
        try:
            self.pool = await asyncpg.create_pool(
                user=os.getenv("POSTGRES_USER", "admin"), password=os.getenv("POSTGRES_PASSWORD", "changeme"),
                database=os.getenv("POSTGRES_DB", "imm_db"), host=os.getenv("POSTGRES_HOST", "postgres"),
                port=int(os.getenv("POSTGRES_PORT", "5432")), min_size=1, max_size=4, timeout=5)
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    # several monitors starting at once must not race on CREATE
                    await conn.execute("SELECT pg_advisory_xact_lock(7471001)")
                    await conn.execute(SCHEMA)
            self.ok, self.error = True, None
            return True
        except Exception as exc:     # database not up yet
            if self.pool is not None:
                await self.pool.close()
            self.pool, self.ok, self.error = None, False, str(exc)
            return False

    # ── loading after a restart ────────────────────────────────────
    async def load_open_alarms(self) -> List[Alarm]:
        rows = await self.pool.fetch("SELECT * FROM alarms WHERE state <> 'closed' ORDER BY raised_at")
        out = []
        for r in rows:
            out.append(Alarm(key=r["alarm_key"], severity=r["severity"], category=r["category"], source=r["source"] or "",
                             message=r["message"], raised_at=_epoch(r["raised_at"]), state=r["state"], acked=r["acked"],
                             acked_at=_epoch(r["acked_at"]), acked_by=r["acked_by"], cleared_at=_epoch(r["cleared_at"]),
                             value=r["value"], unverified=r["unverified"], simulated=r["simulated"],
                             raise_count=r["raise_count"], details=json.loads(r["details"] or "{}"), db_id=r["id"]))
        return out

    async def load_crews(self) -> List[dict]:
        rows = await self.pool.fetch("SELECT * FROM eva_crew_status WHERE armed")
        return [dict(r) for r in rows]

    async def load_components(self) -> List[dict]:
        rows = await self.pool.fetch("SELECT * FROM component_status WHERE updated_at > now() - interval '1 day'")
        return [{**dict(r), "details": json.loads(r["details"] or "{}")} for r in rows]

    async def load_streams(self, since_s: float) -> List[dict]:
        rows = await self.pool.fetch(
            "SELECT * FROM telemetry_streams WHERE last_seen > now() - make_interval(secs => $1)", since_s)
        return [dict(r) for r in rows]

    # ── writes ─────────────────────────────────────────────────────
    def enqueue(self, kind: str, payload) -> None:
        self.pending.append((kind, payload))

    async def flush(self) -> None:
        if self.pool is None and not await self.connect():
            return
        while self.pending:
            kind, payload = self.pending[0]
            try:
                await getattr(self, "_w_" + kind)(payload)
            except (OSError, asyncio.TimeoutError, ConnectionError) as exc:
                self.ok, self.error = False, str(exc)
                return                        # keep it queued; retry on the next flush
            except Exception as exc:          # a bad row must not block the queue
                if "connection" in str(exc).lower():
                    self.ok, self.error = False, str(exc)
                    return
                log.error("health store write %s failed: %s", kind, exc)
            self.pending.popleft()
            self.ok, self.error = True, None

    async def _w_alarm(self, ev: dict) -> None:
        a: Alarm = ev["_alarm"]
        kind, at = ev["event"], _ts(ev["at"])
        if kind == "raised" or (a.db_id is None and kind in ("reraised", "escalated", "deescalated")):
            a.db_id = await self.pool.fetchval(
                """INSERT INTO alarms (alarm_key, severity, category, source, message, state, acked, unverified, simulated,
                                       value, raise_count, details, raised_at)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
                   ON CONFLICT (alarm_key) WHERE state <> 'closed'
                   DO UPDATE SET severity=EXCLUDED.severity, message=EXCLUDED.message, state=EXCLUDED.state,
                                 updated_at=now()
                   RETURNING id""",
                a.key, a.severity, a.category, a.source, a.message, a.state, a.acked, a.unverified, a.simulated,
                a.value, a.raise_count, json.dumps(a.details, default=str), _ts(a.raised_at))
        elif a.db_id is not None:
            await self.pool.execute(
                """UPDATE alarms SET severity=$2, message=$3, state=$4, acked=$5, acked_at=$6, acked_by=$7,
                          cleared_at=$8, value=$9, unverified=$10, raise_count=$11,
                          closed_at=CASE WHEN $4='closed' THEN now() ELSE closed_at END, updated_at=now()
                   WHERE id=$1""",
                a.db_id, a.severity, a.message, a.state, a.acked, _ts(a.acked_at), a.acked_by, _ts(a.cleared_at),
                a.value, a.unverified, a.raise_count)
        snap = ev.get("alarm") or {}        # the alarm as it was at this event (a may have moved on)
        await self.pool.execute(
            "INSERT INTO alarm_events (alarm_id, alarm_key, event, severity, message, value, actor, at) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
            a.db_id, a.key, kind, snap.get("severity", a.severity), snap.get("message", a.message),
            snap.get("value", a.value), ev.get("actor"), at)

    async def _w_forget_node(self, node: str) -> None:
        await self.pool.execute("DELETE FROM telemetry_streams WHERE node_id = $1", node)
        await self.pool.execute("DELETE FROM component_status WHERE node_id = $1", node)

    async def _w_component(self, comp: dict) -> None:
        await self.pool.execute(
            """INSERT INTO component_status (node_id, component, state, reason, interval_s, details, updated_at)
               VALUES ($1,$2,$3,$4,$5,$6,now())
               ON CONFLICT (node_id, component) DO UPDATE SET state=EXCLUDED.state, reason=EXCLUDED.reason,
                   interval_s=EXCLUDED.interval_s, details=EXCLUDED.details, updated_at=now()""",
            comp["node_id"], comp["component"], comp["state"], comp.get("reason") or "", comp.get("interval_s"),
            json.dumps(comp.get("details") or {}, default=str))

    async def _w_note(self, ev: dict) -> None:
        await self.pool.execute(
            "INSERT INTO alarm_events (alarm_key, event, severity, message, actor, details, at) VALUES ($1,$2,$3,$4,$5,$6,$7)",
            ev.get("key", "event"), ev.get("event", "note"), ev.get("severity", "advisory"), ev.get("message", ""),
            ev.get("actor"), json.dumps(ev.get("details") or {}, default=str), _ts(ev.get("at")))

    async def _w_crew(self, snap: dict) -> None:
        await self.pool.execute(
            """INSERT INTO eva_crew_status (crew_id, armed, armed_by, state, last_contact, last_position, last_vitals, updated_at)
               VALUES ($1,$2,$3,$4,$5,$6,$7,now())
               ON CONFLICT (crew_id) DO UPDATE SET armed=EXCLUDED.armed, armed_by=EXCLUDED.armed_by, state=EXCLUDED.state,
                   last_contact=EXCLUDED.last_contact, last_position=EXCLUDED.last_position,
                   last_vitals=EXCLUDED.last_vitals, updated_at=now()""",
            snap["crew_id"], snap["armed"], snap.get("armed_by"), snap["state"], _ts(snap.get("_last_contact")),
            json.dumps(snap.get("last_position") or {}), json.dumps(snap.get("last_vitals") or {}))

    async def _w_streams(self, rows: List[dict]) -> None:
        await self.pool.executemany(
            """INSERT INTO telemetry_streams (stream_key, node_id, sensor, zone, crew_id, simulated, expected_period_s,
                                              first_seen, last_seen)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
               ON CONFLICT (stream_key) DO UPDATE SET expected_period_s=EXCLUDED.expected_period_s, last_seen=EXCLUDED.last_seen""",
            [(r["key"], r["node"], r["sensor"], r["zone"], r["crew"], r["simulated"], r["period"], _ts(r["first_seen"]),
              _ts(r["last_live"])) for r in rows])

    # ── reads for the API ──────────────────────────────────────────
    async def history(self, limit: int) -> List[dict]:
        if self.pool is None:
            return []
        rows = await self.pool.fetch(
            "SELECT e.*, a.category, a.source FROM alarm_events e LEFT JOIN alarms a ON a.id = e.alarm_id "
            "ORDER BY e.at DESC, e.id DESC LIMIT $1", limit)
        return [{**dict(r), "at": _epoch(r["at"])} for r in rows]


def stream_rows(tracker) -> List[dict]:
    out = []
    for s in tracker.streams.values():
        if s.last_live is None:
            continue
        k: StreamKey = s.key
        out.append({"key": k.id(), "node": k.node, "sensor": k.sensor, "zone": k.zone, "crew": k.crew,
                    "simulated": k.simulated, "period": s.period(), "first_seen": s.first_seen, "last_live": s.last_live})
    return out

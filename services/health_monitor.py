#!/usr/bin/env python3
"""
IMM-OS Health Monitor: fault detection, isolation and recovery (FDIR) at the MCC.

Reads every validated reading, every rejected one and every edge component status
from Kafka, and keeps the live health picture (services/health):
  - stream health and data quality, sensor faults, lost readings
  - redundant sources and failover per habitat measurement
  - alarms with a proper lifecycle (raise, escalate, clear, acknowledge), persisted
  - EVA loss-of-signal monitoring per crew member
  - MCC service checks (HTTP health, InfluxDB, Postgres, Kafka consumer lag)
  - subsystem GO / DEGRADED / NO-GO and the mission mode

Publishes every alarm/EVA/component event, and a summary every 5 s, on Kafka
health.events (the realtime socket forwards them to the browsers).

API (behind nginx at /api/health/, Keycloak token or the internal service token):
  GET  /api/health/summary                 mission mode, subsystems, open alarms, EVA
  GET  /api/health/alarms                  open alarms;  /api/health/alarms/history
  POST /api/health/alarms/{id}/ack         acknowledge;  POST /api/health/alarms/ack-all
  GET  /api/health/streams                 every stream's status, quality, integrity
  GET  /api/health/measurements            measurements with their sources
  GET  /api/health/eva                     crew LOS state; POST .../eva/{crew}/arm|disarm
  GET  /api/health/services                MCC service checks
  POST /api/health/nodes/{node}/forget     decommission a node (its streams, components, alarms)
  POST /api/health/events                  internal: autoheal and others report recoveries
  GET  /health                             liveness (the monitor's own loop is ticking)

Run: uvicorn services.health_monitor:app --port 8011
"""
import asyncio
import json
import logging
import os
import threading
import time
from typing import Dict, Optional

from fastapi import Body, Depends, FastAPI, HTTPException

from services import heartbeat
from services.auth import COMMANDER, MCC_OPERATOR, User, authenticated, current_user, require_roles
from services.health.alarms import SEVERITIES, Alarm, Condition
from services.health.eva import EvaMonitor
from services.health.monitor import HealthMonitor
from services.health.streams import StreamKey
from services.health_store import HealthStore, stream_rows

logging.basicConfig(level=logging.INFO, format="%(asctime)s [health] %(message)s")
log = logging.getLogger("health_monitor")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
IN_TOPICS = ["telemetry.validated", "telemetry.deadletter", "health.raw"]
EVENTS_TOPIC = "health.events"
TICK_S = 1.0
SUMMARY_EVERY_S = 5.0
CHECK_EVERY_S = 10.0
STREAMS_SAVE_EVERY_S = 60.0
PLAN_SYNC_EVERY_S = 15.0
REGISTRY_WINDOW_S = 3600.0          # streams seen within this before a restart are expected back
NOTE_ALARM_S = 600.0                # an autoheal restart stays visible (advisory) this long

# name=url, "*" suffix = critical; defaults match imm-os-infra docker-compose.yml
DEFAULT_CHECKS = ("telemetry-ingest=http://telemetry-ingest:8000/health*,backend=http://backend:8000/health*,"
                  "influxdb=http://influxdb:8086/health*,eclss-api=http://eclss-api:8003/health,"
                  "eva-api=http://eva-api:8004/health,time-service=http://time-service:8002/health,"
                  "comms-api=http://comms-api:8005/health,inventory-api=http://inventory-api:8010/health")
LAG_GROUPS = {"telemetry-processor": ("imm-telemetry-processor", ["telemetry.validated"]),
              "telemetry-validator": ("imm-telemetry-validator", ["telemetry.raw", "eva.raw"])}
LAG_STALLED = int(os.getenv("HEALTH_LAG_STALLED", "2000"))


def parse_checks(spec: str) -> Dict[str, dict]:
    out = {}
    for part in filter(None, (p.strip() for p in (spec or "").split(","))):
        if "=" not in part:
            continue
        name, url = part.split("=", 1)
        out[name.strip()] = {"url": url.rstrip("*").strip(), "critical": url.endswith("*")}
    return out


def env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except ValueError:
        return default


app = FastAPI(title="IMM-OS Health Monitor", version="1.0.0")
monitor = HealthMonitor(EvaMonitor(
    warn_s=env_float("EVA_LOS_WARN_S", 10), los_s=env_float("EVA_LOS_S", 30),
    contingency_s=env_float("EVA_LOS_CONTINGENCY_S", 120), partial_s=env_float("EVA_PARTIAL_LOSS_S", 15)))
store = HealthStore()
state = {"consumer_alive": False, "consumer_error": None, "last_message": None, "notes": {},
         "plans": {}, "services": {}, "comm_delay_s": None, "started": time.time()}
_lock = asyncio.Lock()
_queue: Optional[asyncio.Queue] = None
_producer = None


# ── Kafka in ───────────────────────────────────────────────────────
def consume(loop: asyncio.AbstractEventLoop) -> None:
    from confluent_kafka import Consumer, KafkaError
    from confluent_kafka.admin import AdminClient, NewTopic
    while True:
        try:
            admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
            existing = admin.list_topics(timeout=10).topics
            missing = [t for t in IN_TOPICS + [EVENTS_TOPIC] if t not in existing]
            if missing:
                for t, f in admin.create_topics([NewTopic(t, num_partitions=3, replication_factor=1) for t in missing]).items():
                    try:
                        f.result(timeout=15)
                    except Exception:
                        pass
            consumer = Consumer({"bootstrap.servers": KAFKA_BOOTSTRAP, "group.id": "imm-health-monitor",
                                 "auto.offset.reset": "latest", "enable.auto.commit": True,
                                 "topic.metadata.refresh.interval.ms": 10000})
            consumer.subscribe(IN_TOPICS)
            state["consumer_alive"], state["consumer_error"] = True, None
            log.info("Following %s", ", ".join(IN_TOPICS))
            while True:
                msg = consumer.poll(1.0)
                state["consumer_beat"] = time.time()
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        state["consumer_error"] = str(msg.error())
                    continue
                loop.call_soon_threadsafe(_queue.put_nowait, (msg.topic(), msg.key(), msg.value(), time.time()))
        except Exception as exc:   # Kafka down: retry
            state["consumer_alive"], state["consumer_error"] = False, str(exc)
            log.warning("Kafka consumer: %s; retrying in 5 s", exc)
            time.sleep(5)


def handle(topic: str, key: Optional[bytes], value: bytes, now: float) -> None:
    try:
        msg = json.loads(value or b"{}")
    except ValueError:
        return
    state["last_message"] = now
    if topic == "telemetry.validated":
        monitor.ingest_reading(msg, now)
    elif topic == "telemetry.deadletter":
        monitor.ingest_deadletter(msg, now)
    elif topic == "health.raw":
        mqtt_topic = key.decode("utf-8", "replace") if key else None
        monitor.ingest_component(msg, now, mqtt_topic)


# ── Kafka out ──────────────────────────────────────────────────────
def publish(event: dict) -> None:
    global _producer
    try:
        if _producer is None:
            from confluent_kafka import Producer
            _producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "acks": "1", "linger.ms": 50})
        body = {k: v for k, v in event.items() if not k.startswith("_")}
        _producer.produce(EVENTS_TOPIC, json.dumps(body, default=str).encode())
        _producer.poll(0)
    except Exception as exc:
        log.debug("health.events publish failed: %s", exc)


# ── service checks ─────────────────────────────────────────────────
async def check_services() -> None:
    import httpx
    checks = parse_checks(os.getenv("HEALTH_CHECKS", DEFAULT_CHECKS))
    prev = state["services"]
    out: Dict[str, dict] = {}
    async with httpx.AsyncClient(timeout=4.0) as client:
        async def one(name, spec):
            st = {"ok": True, "critical": spec["critical"], "error": None, "kind": "http"}
            try:
                r = await client.get(spec["url"])
                if r.status_code >= 400:
                    st.update(ok=False, error=f"HTTP {r.status_code}")
            except Exception as exc:
                st.update(ok=False, error=type(exc).__name__ + (f": {exc}" if str(exc) else ""))
            st["failures"] = (prev.get(name, {}).get("failures", 0) + 1) if not st["ok"] else 0
            out[name] = st
        await asyncio.gather(*(one(n, s) for n, s in checks.items()))
    # Postgres (alarms persistence)
    pg = {"ok": store.ok or not store.pending, "critical": False, "kind": "postgres", "error": store.error,
          "pending_writes": len(store.pending)}
    if store.pool is None:
        pg["ok"] = False
    pg["failures"] = 0 if pg["ok"] else prev.get("postgres", {}).get("failures", 0) + 1
    out["postgres"] = pg
    # Kafka consumer lag of the processing workers
    lag = await asyncio.get_running_loop().run_in_executor(None, kafka_lags)
    for name, info in lag.items():
        p = prev.get(name, {})
        hist = (p.get("lag_hist") or [])[-6:] + [info.get("lag")]
        stalled = info.get("lag") is not None and info["lag"] > LAG_STALLED and all(
            h is not None and h >= LAG_STALLED for h in hist[-4:]) and len(hist) >= 4
        out[name] = {"ok": info.get("ok", True) and not stalled, "critical": True, "kind": "kafka-lag",
                     "lag": info.get("lag"), "stalled": stalled, "error": info.get("error"), "lag_hist": hist,
                     "failures": 0 if not stalled else p.get("failures", 0) + 1}
    # is telemetry arriving at all?
    last = state.get("last_message")
    flow_ok = last is not None and time.time() - last < 60
    had = monitor.counts["readings"] > 0
    out["telemetry-flow"] = {"ok": flow_ok or not had, "critical": True, "kind": "flow",
                             "error": None if flow_ok else "no telemetry for 60 s (MQTT bridge, broker or every node)",
                             "failures": 0 if (flow_ok or not had) else prev.get("telemetry-flow", {}).get("failures", 0) + 1}
    state["services"] = out
    monitor.set_services(out)


def kafka_lags() -> Dict[str, dict]:
    out = {}
    try:
        from confluent_kafka import Consumer, TopicPartition
        probe = Consumer({"bootstrap.servers": KAFKA_BOOTSTRAP, "group.id": "imm-health-lag-probe",
                          "enable.auto.commit": False})
        try:
            meta = probe.list_topics(timeout=5)
            for name, (group, topics) in LAG_GROUPS.items():
                g = Consumer({"bootstrap.servers": KAFKA_BOOTSTRAP, "group.id": group, "enable.auto.commit": False})
                try:
                    parts = [TopicPartition(t, p) for t in topics if t in meta.topics for p in meta.topics[t].partitions]
                    if not parts:
                        continue
                    committed = g.committed(parts, timeout=5)
                    total = 0
                    for tp in committed:
                        lo, hi = probe.get_watermark_offsets(TopicPartition(tp.topic, tp.partition), timeout=5)
                        total += max(0, hi - (tp.offset if tp.offset >= 0 else lo))
                    out[name] = {"ok": True, "lag": total}
                finally:
                    g.close()
        finally:
            probe.close()
    except Exception as exc:
        for name in LAG_GROUPS:
            out[name] = {"ok": True, "lag": None, "error": f"lag unknown: {exc}"}
    return out


async def comm_delay() -> None:
    try:
        import redis.asyncio as redis
        r = redis.from_url(os.getenv("REDIS_URL", "redis://redis:6379/0"), decode_responses=True)
        cfg = await r.hgetall("imm:delay_config")
        state["comm_delay_s"] = float(cfg.get("value", 0)) if cfg else 0.0
        await r.aclose()
    except Exception:
        state["comm_delay_s"] = None


async def sync_plans() -> None:
    """EVA plans IN_PROGRESS arm their crew; COMPLETE / ABORTED disarm them."""
    if store.pool is None:
        return
    try:
        rows = await store.pool.fetch(
            "SELECT id, status, crew_members FROM eva_plans WHERE status IN ('IN_PROGRESS','COMPLETE','ABORTED') "
            "AND created_at > now() - interval '3 days'")
    except Exception:
        return
    now = time.time()
    for r in rows:
        before = state["plans"].get(r["id"])
        state["plans"][r["id"]] = r["status"]
        if before == r["status"]:
            continue
        for crew in r["crew_members"] or []:
            crew = str(crew).lower()
            if r["status"] == "IN_PROGRESS":
                emit(monitor.eva.arm(crew, now, by=f"plan {r['id']}"))
            elif before is not None or (monitor.eva.crews.get(crew) is not None
                                        and (monitor.eva.crews[crew].armed_by or "").startswith("plan")):
                emit(monitor.eva.disarm(crew, now, by=f"plan {r['id']} {r['status'].lower()}"))


# ── the loop ───────────────────────────────────────────────────────
def emit(events) -> None:
    for ev in events:
        publish(ev)
        if ev.get("type") == "alarm":
            store.enqueue("alarm", ev)
        elif ev.get("type") in ("eva", "component") and ev.get("event") in ("armed", "disarmed", "recovered", "state"):
            if ev.get("type") == "component" and ev.get("id") in monitor.components:
                store.enqueue("component", {k: v for k, v in monitor.components[ev["id"]].items() if not k.startswith("_")})
            if ev.get("type") == "eva":
                t = monitor.eva.crews.get(ev["crew_id"])
                if t is not None:
                    snap = t.snapshot(time.time())
                    snap["_last_contact"] = t.last_contact
                    store.enqueue("crew", snap)
            store.enqueue("note", {"key": f"{ev['type']}.{ev.get('crew_id') or ev.get('id')}", "event": ev["event"],
                                   "severity": "advisory", "message": json.dumps({k: v for k, v in ev.items() if k != "type"}, default=str),
                                   "at": ev.get("at")})


async def note_conditions(now: float) -> Dict[str, Condition]:
    return {k: c for k, (c, until) in state["notes"].items() if now < until}


async def run() -> None:
    last_summary = last_check = last_save = last_plans = 0.0
    while True:
        now = time.time()
        drained = 0
        while _queue is not None and not _queue.empty() and drained < 20000:
            topic, key, value, at = _queue.get_nowait()
            handle(topic, key, value, at)
            drained += 1
        async with _lock:
            monitor.extra_conditions = await note_conditions(now)   # autoheal notes: advisory alarms
            events = monitor.tick(now)
        emit(events)
        heartbeat.beat()
        if now - last_summary >= SUMMARY_EVERY_S:
            last_summary = now
            publish({"type": "summary", **monitor.summary(now), "comm_delay_s": state["comm_delay_s"]})
        if now - last_check >= CHECK_EVERY_S:
            last_check = now
            asyncio.create_task(check_services())
            asyncio.create_task(comm_delay())
        if now - last_plans >= PLAN_SYNC_EVERY_S:
            last_plans = now
            asyncio.create_task(sync_plans())
        if now - last_save >= STREAMS_SAVE_EVERY_S:
            last_save = now
            store.enqueue("streams", stream_rows(monitor.tracker))
        try:
            await store.flush()
        except Exception as exc:
            log.warning("store flush: %s", exc)
        await asyncio.sleep(max(0.05, TICK_S - (time.time() - now)))


async def restore() -> None:
    for attempt in range(30):
        if await store.connect():
            break
        log.warning("Postgres not reachable (%s); retrying", store.error)
        await asyncio.sleep(2)
    if store.pool is None:
        log.error("Starting without Postgres: alarms live in memory until it is back")
        return
    try:
        for a in await store.load_open_alarms():
            monitor.alarms.restore(a)
        now = time.time()
        for row in await store.load_crews():
            t = monitor.eva.track(row["crew_id"])
            t.armed, t.armed_by, t.state = True, row.get("armed_by") or "restored", row.get("state") or "NOMINAL"
            t.last_contact = row["last_contact"].timestamp() if row.get("last_contact") else now
            t.last_position = json.loads(row.get("last_position") or "{}")
            t.last_vitals = json.loads(row.get("last_vitals") or "{}")
        for row in await store.load_components():
            cid = f"{row['node_id']}/{row['component']}"
            monitor.components[cid] = {"node_id": row["node_id"], "component": row["component"], "state": row["state"],
                                       "reason": row.get("reason") or "", "interval_s": row.get("interval_s") or 60,
                                       "details": row.get("details") or {}, "timestamp": None, "_received": now}
        for row in await store.load_streams(REGISTRY_WINDOW_S):
            monitor.tracker.register(StreamKey(row["node_id"], row["sensor"], row["zone"], row["crew_id"] or "",
                                               row["simulated"]),
                                     row["first_seen"].timestamp(), row["last_seen"].timestamp(), row["expected_period_s"])
        log.info("Restored %d open alarm(s), %d armed crew, %d component(s), %d known stream(s)", len(monitor.alarms.alarms),
                 sum(1 for t in monitor.eva.crews.values() if t.armed), len(monitor.components), len(monitor.tracker.streams))
    except Exception as exc:
        log.error("Restoring state failed: %s", exc)


@app.on_event("shutdown")
async def shutdown() -> None:
    """Write what is still queued (a last acknowledgement, a closing alarm) before exiting."""
    try:
        await asyncio.wait_for(store.flush(), 5)
    except Exception as exc:
        log.warning("store flush at shutdown: %s", exc)


@app.on_event("startup")
async def startup() -> None:
    global _queue
    _queue = asyncio.Queue()
    await restore()
    threading.Thread(target=consume, args=(asyncio.get_running_loop(),), daemon=True).start()
    asyncio.create_task(run())


# ── API ────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    """
    Liveness: 503 only when the monitor's own loop has stopped ticking (then a restart
    helps). Kafka or Postgres being down is reported here and raised as alarms, but is
    not a reason to restart the monitor: it is the service that reports those outages.
    """
    age = None if monitor.last_tick is None else time.time() - monitor.last_tick
    alive = age is not None and age < 10
    body = {"status": "ok" if alive and state.get("consumer_alive") and store.ok else "degraded",
            "tick_age_s": age, "kafka": state.get("consumer_alive"), "postgres": store.ok,
            "pending_writes": len(store.pending)}
    if not alive and time.time() - state["started"] > 60:
        raise HTTPException(503, detail=body)
    return body


@app.get("/api/health/summary", dependencies=[Depends(current_user)])
def summary():
    return {**monitor.summary(time.time()), "comm_delay_s": state["comm_delay_s"]}


@app.get("/api/health/alarms", dependencies=[Depends(current_user)])
def alarms():
    return {"alarms": [a.to_dict() for a in monitor.alarms.open()]}


@app.get("/api/health/alarms/history", dependencies=[Depends(current_user)])
async def alarm_history(limit: int = 200):
    return {"events": await store.history(max(1, min(limit, 2000)))}


def _find(alarm_id: int) -> Alarm:
    a = monitor.alarms.find(alarm_id)
    if a is None:
        raise HTTPException(404, "No open alarm with that id")
    return a


@app.post("/api/health/alarms/{alarm_id}/ack")
async def ack(alarm_id: int, user: User = Depends(current_user)):
    async with _lock:
        a = _find(alarm_id)
        emit(monitor.alarms.acknowledge(a, user.username, time.time()))
    return {"alarm": a.to_dict()}


@app.post("/api/health/alarms/ack-all")
async def ack_all(user: User = Depends(current_user)):
    n = 0
    async with _lock:
        now = time.time()
        for a in list(monitor.alarms.open()):
            if not a.acked:
                emit(monitor.alarms.acknowledge(a, user.username, now))
                n += 1
    return {"acknowledged": n}


@app.get("/api/health/streams", dependencies=[Depends(current_user)])
def streams():
    return {"streams": monitor.streams(time.time())}


@app.get("/api/health/measurements", dependencies=[Depends(current_user)])
def measurements():
    return {"measurements": monitor.measurements()}


@app.get("/api/health/eva", dependencies=[Depends(current_user)])
def eva():
    return {"crews": monitor.eva_status(time.time()), "comm_delay_s": state["comm_delay_s"],
            "thresholds": {"warn_s": monitor.eva.warn_s, "los_s": monitor.eva.los_s,
                           "contingency_s": monitor.eva.contingency_s, "partial_s": monitor.eva.partial_s}}


def _crew_id(crew: str) -> str:
    crew = crew.strip().lower()
    if not crew or len(crew) > 64 or not all(c.isalnum() or c in "-_." for c in crew):
        raise HTTPException(422, "Invalid crew id")
    return crew


@app.post("/api/health/eva/{crew}/arm")
async def eva_arm(crew: str, user: User = Depends(current_user)):
    crew = _crew_id(crew)
    async with _lock:
        emit(monitor.eva.arm(crew, time.time(), by=user.username))
    return {"crew": monitor.eva.crews[crew].snapshot(time.time())}


@app.post("/api/health/eva/{crew}/disarm")
async def eva_disarm(crew: str, user: User = Depends(require_roles(MCC_OPERATOR, COMMANDER))):
    """Disarming stops LOS monitoring for that crew member: MCC operators and the commander only."""
    crew = _crew_id(crew)
    async with _lock:
        if crew not in monitor.eva.crews:
            raise HTTPException(404, "Crew member not tracked")
        emit(monitor.eva.disarm(crew, time.time(), by=user.username))
    return {"crew": monitor.eva.crews[crew].snapshot(time.time())}


@app.post("/api/health/nodes/{node}/forget")
async def forget_node(node: str, user: User = Depends(require_roles(MCC_OPERATOR, COMMANDER))):
    """Decommission a node: stop expecting its streams and close its alarms (MCC operator / commander)."""
    node = node.strip()
    if not node or len(node) > 64 or not all(c.isalnum() or c in "-_." for c in node):
        raise HTTPException(422, "Invalid node id")
    async with _lock:
        events = monitor.forget_node(node, time.time(), user.username)
        emit(events)
        store.enqueue("forget_node", node)
        store.enqueue("note", {"key": f"node.{node}", "event": "forgotten", "severity": "advisory",
                               "message": f"Node {node} decommissioned by {user.username}", "actor": user.username,
                               "at": time.time()})
    return {"node": node, "alarms_closed": sum(1 for e in events if e.get("event") == "retired")}


@app.get("/api/health/services", dependencies=[Depends(current_user)])
def services():
    return {"services": {k: {kk: vv for kk, vv in v.items() if kk != "lag_hist"} for k, v in state["services"].items()},
            "kafka_consumer": {"alive": state.get("consumer_alive"), "error": state.get("consumer_error")}}


@app.post("/api/health/events")
async def report_event(event: dict = Body(...), user: User = Depends(authenticated)):
    """Internal: a recovery or fault reported by another service (autoheal restarts)."""
    if not user.is_service:
        raise HTTPException(403, "Internal services only")
    now = time.time()
    key = str(event.get("key") or "event")[:150]
    sev = event.get("severity") if event.get("severity") in SEVERITIES else "advisory"
    msg = str(event.get("message") or key)[:500]
    state["notes"][f"note.{key}"] = (Condition(sev, str(event.get("category") or "pipeline"), str(event.get("source") or key),
                                               msg), now + float(event.get("hold_s") or NOTE_ALARM_S))
    store.enqueue("note", {"key": key, "event": "note", "severity": sev, "message": msg, "actor": user.username,
                           "details": event.get("details") or {}, "at": now})
    publish({"type": "note", "key": key, "severity": sev, "message": msg, "at": now})
    return {"ok": True}

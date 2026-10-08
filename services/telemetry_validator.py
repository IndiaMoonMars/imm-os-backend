#!/usr/bin/env python3
"""
IMM-OS Telemetry Validator

Consumes raw sensor messages from Kafka ``telemetry.raw`` and ``eva.raw`` (forwarded
verbatim from MQTT by the bridge, keyed by MQTT topic), validates each against the shared
telemetry schema and publishes:
  - valid readings  → ``telemetry.validated`` (read by the processor, realtime WS, AI)
  - rejected ones   → ``telemetry.deadletter`` with the reason, for debugging drivers

Run: python -u -m services.telemetry_validator
"""
import json
import logging
import os
import signal
import sys
import threading
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

from services import heartbeat
from services.telemetry_schema import InvalidTelemetry, NotTelemetry, normalise

ist_tz = timezone(timedelta(hours=5, minutes=30))
logging.Formatter.converter = lambda *args: datetime.now(ist_tz).timetuple()
logging.basicConfig(level=logging.INFO, format="[UTC %(created)f] [IST %(asctime)s] [validator] %(message)s")
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
RAW_TOPIC = "telemetry.raw"
EVA_RAW_TOPIC = "eva.raw"      # habitat/eva/# from the bridge
VALIDATED_TOPIC = "telemetry.validated"
DEADLETTER_TOPIC = "telemetry.deadletter"
GROUP_ID = "imm-telemetry-validator"


def route(key: Optional[bytes], value: Optional[bytes]) -> Optional[Tuple[str, bytes, bytes]]:
    """Decide where one raw message goes: (kafka topic, key, value), or None to skip it."""
    topic = key.decode("utf-8", "replace") if key else None
    try:
        message = json.loads(value or b"")
        reading = normalise(message, topic)
    except NotTelemetry:
        return None
    except (ValueError, InvalidTelemetry) as exc:  # JSONDecodeError is a ValueError
        reason = str(exc) if isinstance(exc, InvalidTelemetry) else "invalid JSON"
        body = {"reason": reason, "mqtt_topic": topic, "raw": (value or b"")[:2000].decode("utf-8", "replace")}
        return DEADLETTER_TOPIC, key or b"", json.dumps(body).encode()
    # same envelope telemetry-ingest's POST /ingest produces
    envelope = {"data": reading, "sig": reading.pop("sig", None)}
    msg_key = reading.get("sensor") or f"eva_position/{reading.get('crew_id')}"
    return VALIDATED_TOPIC, msg_key.encode(), json.dumps(envelope).encode()


def ensure_topics(bootstrap: str) -> None:
    from confluent_kafka.admin import AdminClient, NewTopic
    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = admin.list_topics(timeout=10).topics
    missing = [n for n in (RAW_TOPIC, EVA_RAW_TOPIC, VALIDATED_TOPIC, DEADLETTER_TOPIC) if n not in existing]
    if not missing:
        return
    futures = admin.create_topics([NewTopic(n, num_partitions=3, replication_factor=1) for n in missing])
    for name, fut in futures.items():
        try:
            fut.result(timeout=15)   # wait: subscribing before the topic exists stalls the consumer
            log.info("Created Kafka topic %s", name)
        except Exception as exc:     # another service (the bridge) created it first
            log.info("Kafka topic %s: %s", name, exc)


def main() -> None:
    from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

    ensure_topics(KAFKA_BOOTSTRAP)
    # At-least-once: offsets are committed only after every validated/rejected message of
    # the batch is confirmed by Kafka. A failed delivery exits (Docker restarts us) so the
    # batch is read again from the last commit instead of being skipped.
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
        # pick up topics created after start-up within seconds, not the 5 min default
        "topic.metadata.refresh.interval.ms": 10000,
    })
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "acks": "all", "enable.idempotence": True})
    failed = []

    def delivered(err, msg):
        if err is not None:
            failed.append(err)
    consumer.subscribe([RAW_TOPIC, EVA_RAW_TOPIC])
    log.info("Validating %s + %s → %s (rejects → %s)", RAW_TOPIC, EVA_RAW_TOPIC, VALIDATED_TOPIC, DEADLETTER_TOPIC)

    def _shutdown(sig, frame):
        log.info("Shutting down validator...")
        # Offsets are committed after each batch, so an unfinished one is simply read again.
        # Leaving the group politely can hang on a bad connection: never let that hold up
        # the exit (and with it Docker's restart) for more than a few seconds.
        producer.flush(5)
        closer = threading.Thread(target=consumer.close, daemon=True)
        closer.start()
        closer.join(5)
        os._exit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    counts = {VALIDATED_TOPIC: 0, DEADLETTER_TOPIC: 0}
    while True:
        msgs = consumer.consume(num_messages=500, timeout=1.0)
        heartbeat.beat()
        if not msgs:
            continue
        for msg in msgs:
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error("Kafka error: %s", msg.error())
                continue
            routed = route(msg.key(), msg.value())
            if routed is None:
                continue
            out_topic, key, value = routed
            if out_topic == DEADLETTER_TOPIC:
                log.warning("Rejected %s: %s", msg.key(), json.loads(value)["reason"])
            producer.produce(out_topic, key=key, value=value, on_delivery=delivered)
            producer.poll(0)
            counts[out_topic] += 1
            if sum(counts.values()) % 500 == 0:
                log.info("validated=%d rejected=%d", counts[VALIDATED_TOPIC], counts[DEADLETTER_TOPIC])
        while producer.flush(10) > 0:      # Kafka slow or restarting: wait, we are alive
            heartbeat.beat()
        if failed:
            log.error("Kafka did not take %d message(s) (%s); restarting to re-read them", len(failed), failed[0])
            sys.exit(1)
        try:
            consumer.commit(asynchronous=False)
        except KafkaException as exc:      # nothing to commit, or a rebalance
            log.debug("commit: %s", exc)


if __name__ == "__main__":
    main()

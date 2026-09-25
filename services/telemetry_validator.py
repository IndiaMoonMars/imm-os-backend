#!/usr/bin/env python3
"""
IMM-OS Telemetry Validator

Consumes raw sensor messages from Kafka ``telemetry.raw`` (forwarded verbatim from
MQTT by the bridge, keyed by MQTT topic), validates each against the shared
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
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

from services.telemetry_schema import InvalidTelemetry, normalise

ist_tz = timezone(timedelta(hours=5, minutes=30))
logging.Formatter.converter = lambda *args: datetime.now(ist_tz).timetuple()
logging.basicConfig(level=logging.INFO, format="[UTC %(created)f] [IST %(asctime)s] [validator] %(message)s")
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
RAW_TOPIC = "telemetry.raw"
VALIDATED_TOPIC = "telemetry.validated"
DEADLETTER_TOPIC = "telemetry.deadletter"
GROUP_ID = "imm-telemetry-validator"


def route(key: Optional[bytes], value: Optional[bytes]) -> Tuple[str, bytes, bytes]:
    """Decide where one raw message goes. Returns (kafka topic, key, value)."""
    topic = key.decode("utf-8", "replace") if key else None
    try:
        message = json.loads(value or b"")
        reading = normalise(message, topic)
    except (ValueError, InvalidTelemetry) as exc:  # JSONDecodeError is a ValueError
        reason = str(exc) if isinstance(exc, InvalidTelemetry) else "invalid JSON"
        body = {"reason": reason, "mqtt_topic": topic, "raw": (value or b"")[:2000].decode("utf-8", "replace")}
        return DEADLETTER_TOPIC, key or b"", json.dumps(body).encode()
    # same envelope telemetry-ingest's POST /ingest produces
    envelope = {"data": reading, "sig": reading.pop("sig", None)}
    return VALIDATED_TOPIC, reading["sensor"].encode(), json.dumps(envelope).encode()


def ensure_topics(bootstrap: str) -> None:
    from confluent_kafka.admin import AdminClient, NewTopic
    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = admin.list_topics(timeout=10).topics
    for name in (RAW_TOPIC, VALIDATED_TOPIC, DEADLETTER_TOPIC):
        if name not in existing:
            admin.create_topics([NewTopic(name, num_partitions=3, replication_factor=1)])
            log.info("Created Kafka topic %s", name)


def main() -> None:
    from confluent_kafka import Consumer, KafkaError, Producer

    ensure_topics(KAFKA_BOOTSTRAP)
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    })
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "acks": "all", "retries": 5})
    consumer.subscribe([RAW_TOPIC])
    log.info("Validating %s → %s (rejects → %s)", RAW_TOPIC, VALIDATED_TOPIC, DEADLETTER_TOPIC)

    def _shutdown(sig, frame):
        log.info("Shutting down validator...")
        producer.flush(10)
        consumer.close()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    counts = {VALIDATED_TOPIC: 0, DEADLETTER_TOPIC: 0}
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                log.error("Kafka error: %s", msg.error())
            continue
        out_topic, key, value = route(msg.key(), msg.value())
        if out_topic == DEADLETTER_TOPIC:
            log.warning("Rejected %s: %s", msg.key(), json.loads(value)["reason"])
        producer.produce(out_topic, key=key, value=value)
        producer.poll(0)
        counts[out_topic] += 1
        if sum(counts.values()) % 500 == 0:
            log.info("validated=%d rejected=%d", counts[VALIDATED_TOPIC], counts[DEADLETTER_TOPIC])


if __name__ == "__main__":
    main()

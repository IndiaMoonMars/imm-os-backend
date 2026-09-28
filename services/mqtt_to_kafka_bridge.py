#!/usr/bin/env python3
"""
IMM-OS MQTT → Kafka Bridge
Subscribes to Mosquitto and forwards every message verbatim to Kafka, keyed by
its MQTT topic:
  habitat/sensors/#  → telemetry.raw    sensor readings
  habitat/eva/#      → eva.raw          EVA suit vitals and positions
  habitat/health/#   → health.raw       edge component status (health monitor)
Beats services/heartbeat.py while connected, so a hung bridge is restarted.
"""

import os
import logging
import signal
import sys
import time

import paho.mqtt.client as mqtt
from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from datetime import datetime, timezone, timedelta

try:
    from services import heartbeat
except ImportError:      # run as a script: python services/mqtt_to_kafka_bridge.py
    import heartbeat

ist_tz = timezone(timedelta(hours=5, minutes=30))
logging.Formatter.converter = lambda *args: datetime.now(ist_tz).timetuple()
logging.basicConfig(level=logging.INFO, format="[UTC %(created)f] [IST %(asctime)s] [bridge] %(message)s")
log = logging.getLogger(__name__)

# ── Config from env ────────────────────────────────────────────────
MQTT_HOST   = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT   = int(os.getenv("MQTT_PORT", "1883"))
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
RAW_TOPIC      = "telemetry.raw"
EVA_RAW_TOPIC  = "eva.raw"
HEALTH_RAW_TOPIC = "health.raw"
MQTT_SUBSCRIBE = "habitat/sensors/#"
MQTT_EVA_SUB   = "habitat/eva/#"
MQTT_HEALTH_SUB = "habitat/health/#"

# ── Kafka Producer ─────────────────────────────────────────────────
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP,
                     "acks": "all",
                     "retries": 5})

def delivery_report(err, msg):
    if err:
        log.error("Kafka delivery failed: %s", err)

def ensure_topics():
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    existing = admin.list_topics(timeout=10).topics
    missing = [t for t in (RAW_TOPIC, EVA_RAW_TOPIC, HEALTH_RAW_TOPIC) if t not in existing]
    if missing:
        for topic, fut in admin.create_topics([NewTopic(t, num_partitions=3, replication_factor=1) for t in missing]).items():
            try:
                fut.result(timeout=15)
                log.info("Created Kafka topic: %s", topic)
            except Exception as exc:  # already created by the validator
                log.info("Kafka topic %s: %s", topic, exc)

# ── MQTT Callbacks ─────────────────────────────────────────────────
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        log.info("Connected to MQTT broker at %s:%d", MQTT_HOST, MQTT_PORT)
        client.subscribe(MQTT_SUBSCRIBE, qos=1)
        client.subscribe(MQTT_EVA_SUB, qos=1)
        client.subscribe(MQTT_HEALTH_SUB, qos=1)
        log.info("Subscribed to %s, %s and %s", MQTT_SUBSCRIBE, MQTT_EVA_SUB, MQTT_HEALTH_SUB)
    else:
        log.error("MQTT connection failed, rc=%d", rc)

def on_message(client, userdata, msg):
    try:
        # Route EVA streams to dedicated topic for low-latency OpenMCT tracking
        if msg.topic.startswith("habitat/eva/"):
            target_topic = EVA_RAW_TOPIC
        elif msg.topic.startswith("habitat/health/"):
            target_topic = HEALTH_RAW_TOPIC
        else:
            target_topic = RAW_TOPIC
        producer.produce(
            target_topic,
            key=msg.topic.encode(),
            value=msg.payload,
            callback=delivery_report,
        )
        producer.poll(0)
        log.debug("Forwarded %s → %s", msg.topic, target_topic)
    except KafkaException as e:
        log.error("Kafka produce error: %s", e)

# ── Entry point ────────────────────────────────────────────────────
def main():
    ensure_topics()

    # Persistent session (fixed client id, clean_session=False): while the bridge restarts,
    # Mosquitto queues the QoS 1 messages for it (max_queued_messages in mosquitto.conf)
    # instead of dropping them, and delivers them when it reconnects.
    client = mqtt.Client(client_id=os.getenv("MQTT_CLIENT_ID", "imm-mqtt-kafka-bridge"), clean_session=False)
    # Broker requires auth (allow_anonymous false); user/topics in imm-os-infra mosquitto/config/acl
    if os.getenv("MQTT_USERNAME"):
        client.username_pw_set(os.getenv("MQTT_USERNAME"), os.getenv("MQTT_PASSWORD"))
    if os.getenv("MQTT_TLS_CA"):  # broker TLS listener (8883); verifies cert + hostname
        client.tls_set(ca_certs=os.getenv("MQTT_TLS_CA"))
    client.on_connect = on_connect
    client.on_message = on_message
    client.reconnect_delay_set(min_delay=1, max_delay=30)
    client.connect_async(MQTT_HOST, MQTT_PORT, keepalive=60)

    def _shutdown(sig, frame):
        log.info("Shutting down bridge...")
        producer.flush(timeout=10)
        client.disconnect()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    log.info("Bridge running — MQTT → Kafka [%s]", RAW_TOPIC)
    client.loop_start()
    while True:
        producer.poll(0.5)
        if client.is_connected():
            heartbeat.beat()
        time.sleep(0.5)

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
IMM-OS MQTT → Kafka Bridge
Subscribes to habitat/sensors/# on Mosquitto and forwards every
message verbatim to Kafka topic 'telemetry.raw'.
"""

import os
import json
import logging
import signal
import sys

import paho.mqtt.client as mqtt
from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

logging.basicConfig(level=logging.INFO, format="%(asctime)s [bridge] %(message)s")
log = logging.getLogger(__name__)

# ── Config from env ────────────────────────────────────────────────
MQTT_HOST   = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT   = int(os.getenv("MQTT_PORT", "1883"))
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
RAW_TOPIC   = "telemetry.raw"
MQTT_SUBSCRIBE = "habitat/sensors/#"

# ── Kafka Producer ─────────────────────────────────────────────────
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP,
                     "acks": "all",
                     "retries": 5})

def delivery_report(err, msg):
    if err:
        log.error("Kafka delivery failed: %s", err)

def ensure_topic():
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    existing = admin.list_topics(timeout=10).topics
    if RAW_TOPIC not in existing:
        admin.create_topics([NewTopic(RAW_TOPIC, num_partitions=3, replication_factor=1)])
        log.info("Created Kafka topic: %s", RAW_TOPIC)

# ── MQTT Callbacks ─────────────────────────────────────────────────
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        log.info("Connected to MQTT broker at %s:%d", MQTT_HOST, MQTT_PORT)
        client.subscribe(MQTT_SUBSCRIBE, qos=1)
        log.info("Subscribed to %s", MQTT_SUBSCRIBE)
    else:
        log.error("MQTT connection failed, rc=%d", rc)

def on_message(client, userdata, msg):
    try:
        # Forward raw bytes to Kafka; key = MQTT topic for deduplication
        producer.produce(
            RAW_TOPIC,
            key=msg.topic.encode(),
            value=msg.payload,
            callback=delivery_report,
        )
        producer.poll(0)  # non-blocking flush trigger
        log.debug("Forwarded %s → %s", msg.topic, RAW_TOPIC)
    except KafkaException as e:
        log.error("Kafka produce error: %s", e)

# ── Entry point ────────────────────────────────────────────────────
def main():
    ensure_topic()

    client = mqtt.Client(client_id="imm-mqtt-kafka-bridge", clean_session=True)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)

    def _shutdown(sig, frame):
        log.info("Shutting down bridge...")
        producer.flush(timeout=10)
        client.disconnect()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    log.info("Bridge running — MQTT → Kafka [%s]", RAW_TOPIC)
    client.loop_forever()

if __name__ == "__main__":
    main()

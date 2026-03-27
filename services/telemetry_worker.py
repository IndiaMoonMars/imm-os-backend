"""
IMM-OS Telemetry Worker
=======================
Subscribes to MQTT telemetry topics and writes data to InfluxDB.

This service bridges the edge nodes (real or simulated) to the
time-series database. OpenMCT queries InfluxDB for dashboards.

Works identically whether data comes from:
  - sensor_sim.py (simulated — development mode)
  - Real RPi/Jetson sensors (production mode)
"""

import json
import logging
import os
import time

import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("telemetry-worker")

# ── Config from environment ───────────────────────────────────────
MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))

INFLUX_URL = os.getenv("INFLUX_URL", "http://localhost:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "imm-super-secret-token")
INFLUX_ORG = os.getenv("INFLUX_ORG", "imm_org")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "telemetry")

# ── InfluxDB client ───────────────────────────────────────────────
influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)


def write_to_influx(payload: dict):
    """Write a single telemetry reading to InfluxDB."""
    try:
        point = (
            Point(payload["measurement"])
            .tag("node_id", payload["node_id"])
            .tag("node_type", payload.get("node_type", "unknown"))
            .tag("unit", payload.get("unit", ""))
            .tag("simulated", str(payload.get("simulated", False)))
            .field("value", float(payload["value"]))
        )
        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
        log.debug(
            "Wrote %s/%s = %s %s",
            payload["node_id"],
            payload["measurement"],
            payload["value"],
            payload.get("unit", ""),
        )
    except Exception as exc:
        log.error("InfluxDB write failed: %s", exc)


# ── MQTT callbacks ────────────────────────────────────────────────

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        log.info("Connected to MQTT broker %s:%s", MQTT_HOST, MQTT_PORT)
        # Subscribe to all telemetry topics
        client.subscribe("imm/habitat/+/telemetry/+", qos=1)
        client.subscribe("imm/habitat/+/status", qos=0)
        log.info("Subscribed to imm/habitat/#")
    else:
        log.error("MQTT connect failed (rc=%s)", rc)


def on_message(client, userdata, msg):
    try:
        topic_parts = msg.topic.split("/")
        payload = json.loads(msg.payload.decode("utf-8"))

        # Skip status messages — not telemetry
        if "status" in msg.topic:
            log.debug("Heartbeat from %s", payload.get("node_id"))
            return

        write_to_influx(payload)

    except json.JSONDecodeError as exc:
        log.warning("Bad JSON on topic %s: %s", msg.topic, exc)
    except Exception as exc:
        log.error("Unexpected error processing message: %s", exc)


def on_disconnect(client, userdata, rc):
    log.warning("Disconnected (rc=%s). Will reconnect...", rc)


# ── Main ─────────────────────────────────────────────────────────

def main():
    log.info("Telemetry worker starting...")
    log.info("MQTT: %s:%s  |  InfluxDB: %s  |  Bucket: %s",
             MQTT_HOST, MQTT_PORT, INFLUX_URL, INFLUX_BUCKET)

    client = mqtt.Client(client_id="imm-telemetry-worker")
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    while True:
        try:
            client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
            client.loop_forever()
        except Exception as exc:
            log.error("Connection error: %s — retrying in 5s", exc)
            time.sleep(5)


if __name__ == "__main__":
    main()

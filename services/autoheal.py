#!/usr/bin/env python3
"""
IMM-OS autoheal: restart MCC containers that Docker reports unhealthy.

Docker restarts a container that exits (restart: unless-stopped) but not one that is
still running and hung. Every IMM-OS container has a healthcheck (HTTP /health, or
the worker heartbeat file, services/heartbeat.py); this service watches the ones
labelled imm.autoheal=true and, when one is unhealthy (or paused), restarts it:

  - at most MAX_RESTARTS restarts per container in WINDOW_S; after that it stops
    trying for BACKOFF_S and reports that the container needs a person
  - every restart and every give-up is reported to the health monitor (POST
    /api/health/events with the internal service token), so it shows as an
    advisory alarm and in the alarm history

Talks to the Docker Engine API over /var/run/docker.sock (stdlib only). The socket
is powerful: this service only lists containers and restarts labelled ones.

Run: python -u -m services.autoheal
"""
import http.client
import json
import logging
import os
import socket
import time
import urllib.parse
from typing import Dict, List

logging.basicConfig(level=logging.INFO, format="%(asctime)s [autoheal] %(message)s")
log = logging.getLogger("autoheal")

SOCKET = os.getenv("DOCKER_SOCKET", "/var/run/docker.sock")
LABEL = os.getenv("AUTOHEAL_LABEL", "imm.autoheal=true")
INTERVAL_S = float(os.getenv("AUTOHEAL_INTERVAL_S", "10"))
MAX_RESTARTS = int(os.getenv("AUTOHEAL_MAX_RESTARTS", "3"))
WINDOW_S = float(os.getenv("AUTOHEAL_WINDOW_S", "900"))
BACKOFF_S = float(os.getenv("AUTOHEAL_BACKOFF_S", "1800"))
REPORT_URL = os.getenv("AUTOHEAL_REPORT_URL", "http://health-monitor:8011/api/health/events")


class DockerSocket(http.client.HTTPConnection):
    def __init__(self, path: str, timeout: float = 30):
        super().__init__("localhost", timeout=timeout)
        self.path = path

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(self.path)


def docker(method: str, path: str, timeout: float = 30):
    conn = DockerSocket(SOCKET, timeout)
    try:
        conn.request(method, path, headers={"Host": "docker"})
        r = conn.getresponse()
        body = r.read()
        return r.status, (json.loads(body) if body and body[:1] in (b"{", b"[") else body)
    finally:
        conn.close()


def containers() -> List[dict]:
    filters = urllib.parse.quote(json.dumps({"label": [LABEL]}))
    status, body = docker("GET", f"/containers/json?all=1&filters={filters}")
    return body if status == 200 else []


def health_of(c: dict) -> str:
    """unhealthy | paused | healthy | starting | none, from the list entry."""
    if c.get("State") == "paused":
        return "paused"
    st = c.get("Status", "")      # "Up 3 minutes (unhealthy)", "Up 5 seconds (health: starting)"
    if "(unhealthy)" in st:
        return "unhealthy"
    if "(healthy)" in st:
        return "healthy"
    if "(health: starting)" in st:
        return "starting"
    return "none"


def report(key: str, severity: str, message: str, details: dict) -> None:
    token = os.getenv("IMM_SERVICE_TOKEN", "")
    if not token or not REPORT_URL:
        return
    try:
        import urllib.request
        req = urllib.request.Request(REPORT_URL, method="POST", data=json.dumps(
            {"key": key, "severity": severity, "category": "pipeline", "source": details.get("container"),
             "message": message, "details": details}).encode(),
            headers={"Content-Type": "application/json", "X-IMM-Service-Token": token})
        urllib.request.urlopen(req, timeout=5).read()
    except Exception as exc:
        log.warning("could not report to the health monitor: %s", exc)


class Healer:
    def __init__(self):
        self.history: Dict[str, List[float]] = {}
        self.given_up: Dict[str, float] = {}

    def step(self, now: float, listing: List[dict]) -> List[dict]:
        actions = []
        for c in listing:
            name = (c.get("Names") or ["?"])[0].lstrip("/")
            h = health_of(c)
            if h not in ("unhealthy", "paused"):
                continue
            if now < self.given_up.get(name, 0):
                continue
            hist = [t for t in self.history.get(name, []) if now - t < WINDOW_S]
            if len(hist) >= MAX_RESTARTS:
                self.given_up[name] = now + BACKOFF_S
                actions.append({"action": "give_up", "container": name, "restarts": len(hist), "health": h})
                continue
            hist.append(now)
            self.history[name] = hist
            actions.append({"action": "restart", "container": name, "id": c["Id"], "health": h, "attempt": len(hist)})
        return actions

    def apply(self, action: dict) -> None:
        name = action["container"]
        if action["action"] == "give_up":
            msg = (f"{name} is still {action['health']} after {action['restarts']} automatic restarts: "
                   f"needs a person (autoheal paused {BACKOFF_S / 60:.0f} min)")
            log.error(msg)
            report(f"autoheal.{name}.gave_up", "warning", msg, action)
            return
        if action["health"] == "paused":
            docker("POST", f"/containers/{action['id']}/unpause")
        status, body = docker("POST", f"/containers/{action['id']}/restart?t=10", timeout=60)
        ok = status in (204, 304)
        msg = f"Autoheal restarted {name} ({action['health']}, attempt {action['attempt']}/{MAX_RESTARTS})"
        if not ok:
            msg = f"Autoheal could not restart {name}: HTTP {status} {body!r:.200}"
        log.warning(msg)
        report(f"autoheal.{name}", "advisory" if ok else "caution", msg, action)


def main() -> None:
    healer = Healer()
    log.info("Watching containers labelled %s every %.0f s", LABEL, INTERVAL_S)
    while True:
        try:
            for action in healer.step(time.time(), containers()):
                healer.apply(action)
        except Exception as exc:
            log.warning("docker API: %s", exc)
        try:
            from services import heartbeat
            heartbeat.beat()
        except Exception:
            pass
        time.sleep(INTERVAL_S)


if __name__ == "__main__":
    main()

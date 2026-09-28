"""
Liveness heartbeat for the background workers (Kafka consumers, the MQTT bridge).

A worker calls beat() from its main loop, which touches IMM_HEARTBEAT_FILE (at most
every 2 s). The container's healthcheck runs `python -m services.heartbeat 60`, which
fails when the file is older than 60 s: the loop is hung (or dead), Docker marks the
container unhealthy and autoheal restarts it. A loop that is merely idle (no messages)
still beats, because poll() returns every second.
"""
import os
import sys
import time

PATH = os.getenv("IMM_HEARTBEAT_FILE", "/tmp/imm-heartbeat")
_last = 0.0


def beat(path: str = None, min_interval_s: float = 2.0) -> None:
    global _last
    now = time.monotonic()
    if now - _last < min_interval_s:
        return
    _last = now
    p = path or PATH
    try:
        with open(p, "a"):
            os.utime(p, None)
    except OSError:
        pass


def age(path: str = None) -> float:
    try:
        return time.time() - os.path.getmtime(path or PATH)
    except OSError:
        return float("inf")


if __name__ == "__main__":
    limit = float(sys.argv[1]) if len(sys.argv) > 1 else 60.0
    a = age()
    print(f"heartbeat age {a:.1f} s (limit {limit:.0f} s)")
    sys.exit(0 if a <= limit else 1)

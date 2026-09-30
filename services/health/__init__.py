"""
IMM-OS health monitoring (FDIR at the MCC): pure logic, driven by services/health_monitor.py.

  streams       every telemetry stream: freshness, integrity (sequence gaps), stuck
                values, impossible rates, invalid data, cross-checks between sensors
  measurements  habitat measurements with redundant sources: which source is used,
                NOMINAL / DEGRADED / LOST
  alarms        alarm lifecycle: raise (with on-delay), escalate, clear (with off-delay),
                acknowledge; one open instance per alarm key
  rules         the alarm conditions (limits with hysteresis, sensor faults, nodes,
                services, EVA) evaluated every second
  eva           EVA loss-of-signal monitoring per crew member
  status        subsystem GO / DEGRADED / NO-GO and the mission mode
  monitor       ties them together: ingest messages, tick(now) → events

Everything takes `now` explicitly, so the tests drive time.
See imm-os-docs/fdir-strategy.md.
"""

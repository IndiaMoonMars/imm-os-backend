"""
IMM-OS telemetry schema — one definition shared by every stage of the pipeline.

Edge drivers and the sensor simulator publish one JSON object per reading on
MQTT topic ``habitat/sensors/<sensor>/<zone>``. The bridge forwards it to Kafka
``telemetry.raw``; the validator checks it against ``TelemetryPayload`` and
publishes the normalised result to ``telemetry.validated``, which the processor
(InfluxDB + alerts), the realtime WebSocket (OpenMCT) and the AI processor read.

No imports from other IMM-OS modules, so it works both as ``services.telemetry_schema``
and as ``telemetry_schema`` (scripts run from the services/ directory).
"""
import time
from enum import Enum
from typing import Dict, List, Optional, Tuple

from pydantic import BaseModel, Field, validator

try:                                   # standalone module, like this one
    from services.quality import assess as assess_quality
except ImportError:                    # scripts run from services/
    from quality import assess as assess_quality

MAX_CLOCK_SKEW_S = 86400 * 7


class SensorType(str, Enum):
    bme280 = "bme280"          # temperature / humidity / pressure
    scd40 = "scd40"            # CO₂ (NDIR) / temperature / humidity
    mq7 = "mq7"                # CO via STM32
    max30100 = "max30100"      # heart rate / SpO₂
    ecg_ad8232 = "ecg_ad8232"  # ECG voltage
    tsl2561 = "tsl2561"        # light
    ina219 = "ina219"          # bus voltage / current / power
    o2 = "o2"                  # electrochemical O₂ cell via ADS1115
    sysmon = "sysmon"          # node health on every edge node (Raspberry Pi 4/5)
    jetson = "jetson"          # Jetson on-board CPU/GPU temperature and power (optional hardware)
    bms = "bms"                # battery management / solar input
    bno055 = "bno055"          # 9-axis IMU on the ESP32 sensor board (orientation, motion)
    mq4 = "mq4"                # methane (CH₄) on the ESP32 sensor board
    eva_biosensor = "eva_biosensor"  # EVA suit vitals (habitat/eva/biosensors/<crew>)
    board = "board"            # microcontroller board health (ESP32 sensor board: resets, I2C errors)
    geiger = "geiger"          # radiation: DFRobot SEN0463 (M4011 tube) on the external board
    gnss = "gnss"              # position / time: DFRobot TEL0157 on the external board


class TelemetryPayload(BaseModel):
    sensor: SensorType
    timestamp: float = Field(..., gt=0)   # Unix seconds; fractions kept (ECG runs at 100 Hz)
    sig: Optional[str] = None
    # provenance
    node_id: Optional[str] = Field(None, max_length=64)
    zone: Optional[str] = Field(None, max_length=64)
    crew_id: Optional[str] = Field(None, max_length=64)
    simulated: bool = False
    # integrity and quality (services/quality.py)
    seq: Optional[int] = Field(None, ge=0)               # per publisher run and topic: gaps = lost readings
    run: Optional[str] = Field(None, max_length=32)      # publisher run id (changes when the driver restarts)
    q: Optional[str] = Field(None, regex="^(good|suspect|bad)$")
    qf: Optional[List[str]] = Field(None, max_items=16)
    delayed: Optional[bool] = None                       # replayed / arrived late: not live
    # data fields; hard limits are physical impossibilities (implausible values are
    # flagged "soft_range" by services/quality.py instead of rejected)
    temp: Optional[float] = Field(None, ge=-100, le=150)
    hum: Optional[float] = Field(None, ge=0, le=100.5)
    pres: Optional[float] = Field(None, ge=0, le=2000)
    co2_ppm: Optional[float] = Field(None, ge=0, le=100000)
    co_ppm: Optional[float] = Field(None, ge=0, le=100000)
    hr_bpm: Optional[float] = Field(None, ge=0, le=350)
    spo2_pct: Optional[float] = Field(None, ge=0, le=100)
    voltage: Optional[float] = None
    lux: Optional[float] = Field(None, ge=0)
    voltage_v: Optional[float] = None
    current_ma: Optional[float] = None
    power_mw: Optional[float] = None
    o2_pct: Optional[float] = Field(None, ge=0, le=100)
    cpu_temp: Optional[float] = Field(None, ge=-40, le=150)
    gpu_temp: Optional[float] = None
    power_w: Optional[float] = Field(None, ge=0)
    cpu_load: Optional[float] = Field(None, ge=0)
    mem_pct: Optional[float] = Field(None, ge=0, le=100)
    disk_pct: Optional[float] = Field(None, ge=0, le=100)
    fan_rpm: Optional[float] = Field(None, ge=0)
    supply_v: Optional[float] = Field(None, ge=0, le=30)
    undervolt: Optional[int] = Field(None, ge=0, le=1)
    throttled: Optional[int] = Field(None, ge=0, le=1)
    undervolt_boot: Optional[int] = Field(None, ge=0, le=1)
    battery_pct: Optional[float] = Field(None, ge=0, le=100)
    solar_w: Optional[float] = Field(None, ge=0)
    skin_temp_c: Optional[float] = Field(None, ge=0, le=60)
    ecg_mv: Optional[float] = None
    heading_deg: Optional[float] = Field(None, ge=0, le=360)
    roll_deg: Optional[float] = Field(None, ge=-180, le=180)
    pitch_deg: Optional[float] = Field(None, ge=-180, le=180)
    lin_acc_ms2: Optional[float] = Field(None, ge=0)
    imu_calib: Optional[int] = Field(None, ge=0, le=3)        # BNO055 system calibration
    vout_mv: Optional[float] = Field(None, ge=0, le=5500)     # MQ-4 load voltage
    rs_r0: Optional[float] = Field(None, ge=0)
    ch4_ppm: Optional[float] = Field(None, ge=0)
    rs_rl: Optional[float] = Field(None, ge=0)                # MQ-4 Rs/RL (before calibration too)
    warming: Optional[int] = Field(None, ge=0, le=1)          # MQ-4 heater still warming up
    warm_left_s: Optional[int] = Field(None, ge=0, le=86400)  # seconds of warm-up left
    cal_restored: Optional[int] = Field(None, ge=0, le=1)     # BNO055 calibration written back from flash
    asc: Optional[int] = Field(None, ge=0, le=1)              # SCD40 automatic self-calibration on
    window_s: Optional[int] = Field(None, ge=0, le=3600)      # Geiger: seconds in its counting window
    calibrated: Optional[int] = Field(None, ge=0, le=1)       # MQ-4 R0 stored (CAL_MQ4)
    dew_point_c: Optional[float] = Field(None, ge=-80, le=80) # from temp + hum at the edge
    grav_ms2: Optional[float] = Field(None, ge=0, le=160)     # BNO055 gravity vector magnitude (9.81)
    mag_ut: Optional[float] = Field(None, ge=0, le=2600)      # BNO055 magnetic field magnitude
    gyro_dps: Optional[float] = Field(None, ge=0, le=3600)    # BNO055 rotation rate magnitude
    calib_gyro: Optional[int] = Field(None, ge=0, le=3)
    calib_acc: Optional[int] = Field(None, ge=0, le=3)
    calib_mag: Optional[int] = Field(None, ge=0, le=3)
    # node health (sysmon_driver.py): services and the link to the MCC
    svc_failed: Optional[int] = Field(None, ge=0)             # IMM-OS systemd units in failed state
    svc_restarts: Optional[int] = Field(None, ge=0)           # total automatic restarts since boot
    mcc_link: Optional[int] = Field(None, ge=0, le=1)         # local broker's bridge to the MCC is up
    mqtt_backlog: Optional[int] = Field(None, ge=0)           # messages queued for the MCC (store-and-forward)
    # board health (ESP32 sensor board)
    uptime_s: Optional[float] = Field(None, ge=0)
    reset_reason: Optional[int] = Field(None, ge=0, le=64)    # ESP-IDF esp_reset_reason_t
    boot_count: Optional[int] = Field(None, ge=0)
    i2c_err: Optional[int] = Field(None, ge=0)
    bme_resets: Optional[int] = Field(None, ge=0)
    rssi_dbm: Optional[int] = Field(None, ge=-127, le=0)      # the board's Wi-Fi signal
    heal_cause: Optional[int] = Field(None, ge=0, le=15)      # this boot was the firmware's own reboot: HEAL_CAUSES
    heal_reboots: Optional[int] = Field(None, ge=0)           # self-heal reboots since flashing
    heap_free: Optional[int] = Field(None, ge=0)              # free memory, bytes
    heap_min: Optional[int] = Field(None, ge=0)               # lowest free memory since boot, bytes
    wifi_drops: Optional[int] = Field(None, ge=0)             # Wi-Fi link losses since boot
    wifi_reason: Optional[int] = Field(None, ge=0, le=1023)   # why the link last dropped (ESP-IDF wifi_err_reason_t)
    net_restarts: Optional[int] = Field(None, ge=0)           # web server + mDNS restarts (not polled) since boot
    # radiation (external board)
    cpm: Optional[float] = Field(None, ge=0, le=1_000_000)    # counts per minute (60 s window)
    usv_h: Optional[float] = Field(None, ge=0, le=100_000)    # dose rate, µSv/h
    counts: Optional[int] = Field(None, ge=0)                 # pulses since the board started
    # GNSS (external board)
    fix: Optional[int] = Field(None, ge=0, le=1)
    sats: Optional[int] = Field(None, ge=0, le=64)
    lat: Optional[float] = Field(None, ge=-90, le=90)
    lon: Optional[float] = Field(None, ge=-180, le=180)
    alt_m: Optional[float] = Field(None, ge=-1000, le=20000)
    sog_kn: Optional[float] = Field(None, ge=0, le=1000)
    cog_deg: Optional[float] = Field(None, ge=0, le=360)
    gnss_utc: Optional[str] = Field(None, max_length=24)      # the receiver's UTC (to check the node clock)

    @validator("timestamp")
    def timestamp_reasonable(cls, v):
        if abs(v - int(time.time())) > MAX_CLOCK_SKEW_S:
            raise ValueError("Timestamp too far from current time")
        return v


# Metric fields each sensor reports (the processor writes one point per metric).
SENSOR_METRICS: Dict[str, List[str]] = {
    "bme280": ["temp", "hum", "pres", "dew_point_c"],
    "scd40": ["co2_ppm", "temp", "hum", "dew_point_c", "asc"],
    "mq7": ["co_ppm"],
    "max30100": ["hr_bpm", "spo2_pct"],
    "ecg_ad8232": ["voltage"],
    "tsl2561": ["lux"],
    "ina219": ["voltage_v", "current_ma", "power_mw"],
    "o2": ["o2_pct", "calibrated"],
    "sysmon": ["cpu_temp", "cpu_load", "mem_pct", "disk_pct", "fan_rpm", "power_w", "supply_v",
               "undervolt", "throttled", "undervolt_boot", "svc_failed", "svc_restarts", "mcc_link",
               "mqtt_backlog"],
    "jetson": ["cpu_temp", "gpu_temp", "power_w"],
    "bms": ["battery_pct", "solar_w"],
    "eva_biosensor": ["hr_bpm", "spo2_pct", "skin_temp_c"],
    "bno055": ["heading_deg", "roll_deg", "pitch_deg", "lin_acc_ms2", "imu_calib",
               "grav_ms2", "mag_ut", "gyro_dps", "temp", "calib_gyro", "calib_acc", "calib_mag", "cal_restored"],
    "mq4": ["ch4_ppm", "rs_r0", "vout_mv", "rs_rl", "warming", "calibrated", "warm_left_s"],
    "board": ["uptime_s", "reset_reason", "boot_count", "i2c_err", "bme_resets", "rssi_dbm",
              "heal_cause", "heal_reboots", "heap_free", "heap_min", "wifi_drops", "wifi_reason", "net_restarts"],
    "geiger": ["cpm", "usv_h", "counts", "warming", "window_s"],
    "gnss": ["fix", "sats", "lat", "lon", "alt_m", "sog_kn", "cog_deg"],
}

# Metrics that are state or counters, not measurements: no z-score, no stuck-value check
STATE_METRICS = {"warming", "calibrated", "warm_left_s", "cal_restored", "asc", "window_s", "imu_calib", "calib_gyro", "calib_acc", "calib_mag",
                 "undervolt", "throttled", "undervolt_boot", "svc_failed", "svc_restarts", "mcc_link",
                 "mqtt_backlog", "uptime_s", "reset_reason", "boot_count", "i2c_err", "bme_resets",
                 "rssi_dbm", "counts", "fix", "sats", "lat", "lon", "alt_m", "sog_kn", "cog_deg",
                 "heal_cause", "heal_reboots", "heap_free", "heap_min", "wifi_drops", "wifi_reason", "net_restarts"}

# ESP32 sensor board: esp_reset_reason_t values worth a word, and the firmware's own reboots
RESET_REASONS = {1: "power-on", 2: "RESET button", 3: "software", 4: "crash", 5: "interrupt watchdog",
                 6: "task watchdog", 7: "watchdog", 9: "brownout (supply dipped)"}
HEAL_CAUSES = {1: "I2C bus stall", 2: "Wi-Fi lost", 3: "not polled by the Pi", 4: "memory low"}

# (sensor, metric) → (dashboard measurement, unit). The first sensor listed for a
# measurement wins when several report it (e.g. BME280 temperature over SCD40's).
DASHBOARD_MEASUREMENTS: Dict[Tuple[str, str], Tuple[str, str]] = {
    ("bme280", "temp"): ("temperature", "celsius"),
    ("bme280", "hum"): ("humidity", "percent"),
    ("bme280", "pres"): ("pressure", "hPa"),
    ("scd40", "co2_ppm"): ("co2", "ppm"),
    ("scd40", "temp"): ("temperature", "celsius"),
    ("scd40", "hum"): ("humidity", "percent"),
    ("o2", "o2_pct"): ("o2", "percent"),
    ("mq7", "co_ppm"): ("co", "ppm"),
    ("tsl2561", "lux"): ("light", "lux"),
    ("sysmon", "cpu_temp"): ("cpu_temp", "celsius"),
    ("sysmon", "power_w"): ("power_draw", "watts"),
    ("sysmon", "cpu_load"): ("cpu_load", "percent"),
    ("sysmon", "mem_pct"): ("memory", "percent"),
    ("sysmon", "disk_pct"): ("disk", "percent"),
    ("sysmon", "fan_rpm"): ("fan", "rpm"),
    ("sysmon", "supply_v"): ("supply_voltage", "volts"),
    ("sysmon", "undervolt"): ("undervoltage", "flag"),
    ("sysmon", "throttled"): ("throttled", "flag"),
    ("jetson", "cpu_temp"): ("cpu_temp", "celsius"),
    ("jetson", "gpu_temp"): ("gpu_temp", "celsius"),
    ("jetson", "power_w"): ("power_draw", "watts"),
    ("bms", "battery_pct"): ("battery_level", "percent"),
    ("bms", "solar_w"): ("solar_input", "watts"),
    ("mq4", "ch4_ppm"): ("methane", "ppm"),
    ("geiger", "usv_h"): ("radiation", "usv_h"),
}

SENSOR_PRIORITY = ["bme280", "o2", "scd40", "mq7", "mq4", "tsl2561", "sysmon", "jetson", "bms"]


class InvalidTelemetry(ValueError):
    """Raised by normalise() with a short reason for the dead-letter topic."""


def _topic_parts(topic: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """habitat/sensors/<sensor>/<zone> → (sensor, zone)."""
    if not topic:
        return None, None
    parts = topic.split("/")
    if len(parts) == 4 and parts[0] == "habitat" and parts[1] == "sensors":
        return parts[2], parts[3]
    return None, None


class NotTelemetry(Exception):
    """A message the pipeline deliberately doesn't forward (e.g. raw GPS/UWB fusion inputs)."""


def _eva_topic(topic: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """habitat/eva/<kind>[/<crew>] → (kind, crew)."""
    parts = (topic or "").split("/")
    if len(parts) >= 3 and parts[0] == "habitat" and parts[1] == "eva":
        return parts[2], (parts[3].lower() if len(parts) > 3 else None)
    return None, None


def normalise_eva_position(message: dict, crew: str) -> dict:
    """Fused EVA position (habitat/eva/position/<crew>) → dict for OpenMCT's EVA tracker."""
    if not isinstance(message, dict):
        raise InvalidTelemetry("payload is not a JSON object")
    if str(message.get("crew_id", crew)).lower() != crew:
        raise InvalidTelemetry("crew_id does not match topic")
    mode = message.get("mode")
    fields = {"uwb": ("x_m", "y_m"), "gps": ("lat", "lon")}.get(mode)
    if not fields:
        raise InvalidTelemetry(f"unknown position mode {mode!r}")
    out = {"crew_id": crew, "mode": mode}
    try:
        for k in fields + ("z_m", "quality", "timestamp"):
            if k in message:
                out[k] = float(message[k]) if k != "timestamp" else int(message[k])
    except (TypeError, ValueError) as exc:
        raise InvalidTelemetry(f"bad number in position: {exc}") from exc
    if not all(k in out for k in fields):
        raise InvalidTelemetry(f"{mode} position needs {fields}")
    out.setdefault("timestamp", int(time.time()))
    for k in ("seq", "run", "delayed"):
        if k in message:
            out[k] = message[k]
    q = assess_quality({"timestamp": out["timestamp"], "delayed": message.get("delayed")}, ())
    out["q"], out["qf"], out["delayed"] = q["q"], q["qf"], q["delayed"]
    return out


def normalise(message: dict, topic: Optional[str] = None) -> dict:
    """
    Validate one reading and return the canonical dict published to telemetry.validated.

    Accepts a bare reading or a ``{"data": {...}, "sig": ...}`` envelope. Sensor and zone
    fall back to the MQTT topic when the payload omits them; a sensor named in both must
    agree, so a node can't publish one sensor's data on another sensor's topic.
    """
    if not isinstance(message, dict):
        raise InvalidTelemetry("payload is not a JSON object")
    data = message.get("data", message)
    if not isinstance(data, dict):
        raise InvalidTelemetry("data is not a JSON object")
    data = dict(data)
    eva_kind, eva_crew = _eva_topic(topic)
    if eva_kind in ("gps", "uwb"):
        raise NotTelemetry("raw positioning input (fused into habitat/eva/position)")
    if eva_kind == "position":
        return normalise_eva_position(data, eva_crew or "")
    if eva_kind == "biosensors":
        if data.get("sensor") not in (None, "eva_biosensor"):
            raise InvalidTelemetry("EVA biosensor topic carries a non-EVA sensor")
        if eva_crew and str(data.get("crew_id", eva_crew)).lower() != eva_crew:
            raise InvalidTelemetry("crew_id does not match topic")
        data["sensor"] = "eva_biosensor"
        data["crew_id"] = eva_crew or data.get("crew_id")
        data.setdefault("zone", "eva")
    elif data.get("sensor") == "eva_biosensor":
        raise InvalidTelemetry("eva_biosensor readings must use habitat/eva/biosensors/<crew>")
    t_sensor, t_zone = _topic_parts(topic)
    if t_sensor:
        if data.get("sensor") not in (None, t_sensor):
            raise InvalidTelemetry(f"sensor {data.get('sensor')!r} does not match topic {topic!r}")
        data.setdefault("sensor", t_sensor)
    if t_zone and not data.get("zone"):
        data["zone"] = t_zone
    if "sig" in message and "sig" not in data:
        data["sig"] = message["sig"]
    try:
        payload = TelemetryPayload(**data)
    except Exception as exc:  # pydantic ValidationError, enum errors
        raise InvalidTelemetry(str(exc).replace("\n", " ")[:300]) from exc
    if not any(getattr(payload, m) is not None for m in SENSOR_METRICS[payload.sensor.value]):
        raise InvalidTelemetry(f"no {payload.sensor.value} metrics in payload")
    out = payload.dict(exclude_none=True)
    out["sensor"] = payload.sensor.value
    ts = round(payload.timestamp, 3)
    out["timestamp"] = int(ts) if ts.is_integer() else ts
    out.setdefault("zone", "unknown")
    out.update(assess_quality(out, SENSOR_METRICS[out["sensor"]]))
    return out

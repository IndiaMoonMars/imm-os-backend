from datetime import datetime, timezone, timedelta
import time

# --- Constants ---
UNIX_EPOCH_JD = 2440587.5
LEAP_SECONDS = 37  # Current leap seconds
TT_OFFSET = 32.184 + LEAP_SECONDS

MARS_SOL_DAYS = 1.0274912517
LUNAR_CYCLE_DAYS = 29.530588


def get_jd_utc(unix_ts: float) -> float:
    return UNIX_EPOCH_JD + (unix_ts / 86400.0)

def get_jd_tt(unix_ts: float) -> float:
    return get_jd_utc(unix_ts) + (TT_OFFSET / 86400.0)

def calculate_msd_cmt(unix_ts: float) -> tuple[float, str]:
    """
    Returns (Mars Sol Date, Coordinated Mars Time string 'HH:MM:SS').
    Based on NASA Mars24 algorithm (Allison and McEwen 2000).
    """
    jd_tt = get_jd_tt(unix_ts)
    delta_j2000 = jd_tt - 2451545.0
    
    # Mars Sol Date calculation
    msd = (delta_j2000 - 4.5) / MARS_SOL_DAYS + 44796.0 - 0.0009626
    
    # Coordinated Mars Time (24 "Mars hours" per sol)
    mtc_hours = (msd % 1.0) * 24.0
    
    h = int(mtc_hours)
    m = int((mtc_hours - h) * 60)
    s = int((((mtc_hours - h) * 60) - m) * 60)
    
    cmt_str = f"{h:02d}:{m:02d}:{s:02d}"
    return msd, cmt_str


def calculate_lst(unix_ts: float) -> str:
    """
    Calculates Lunar Standard Time.
    1 Lunar Day = 29.530588 Earth days.
    Returns format 'Lunar Day {day}, {hh:mm:ss}' tracking since J2000.
    """
    jd_utc = get_jd_utc(unix_ts)
    delta_j2000 = jd_utc - 2451545.0
    
    lunar_days_since_j2000 = delta_j2000 / LUNAR_CYCLE_DAYS
    day = int(lunar_days_since_j2000)
    fractional_day = lunar_days_since_j2000 - day
    
    lst_hours = fractional_day * 24.0
    h = int(lst_hours)
    m = int((lst_hours - h) * 60)
    s = int((((lst_hours - h) * 60) - m) * 60)
    
    return f"LSD {day} - {h:02d}:{m:02d}:{s:02d}"


def calculate_all(unix_ts: float = None) -> dict:
    if unix_ts is None:
        unix_ts = time.time()
        
    dt_utc = datetime.fromtimestamp(unix_ts, tz=timezone.utc)
    ist_tz = timezone(timedelta(hours=5, minutes=30))
    dt_ist = dt_utc.astimezone(ist_tz)
    
    msd, cmt = calculate_msd_cmt(unix_ts)
    lst = calculate_lst(unix_ts)
    
    return {
        "unix_ts": unix_ts,
        "utc": dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "ist": dt_ist.strftime("%Y-%m-%dT%H:%M:%S+05:30"),
        "lst": lst,
        "msd": round(msd, 5),
        "cmt": cmt
    }

def convert_tz(source_tz: str, target_tz: str, unix_ts: float) -> dict:
    # A generic gateway primarily to invoke calculate_all and extract the necessary values.
    # We will compute properties from the unix_ts and return the structure.
    data = calculate_all(unix_ts)
    return {
        "source": source_tz,
        "target": target_tz,
        "unix_ts": unix_ts,
        "source_value": data.get(source_tz.lower(), "unknown"),
        "target_value": data.get(target_tz.lower(), "unknown"),
        "all": data
    }

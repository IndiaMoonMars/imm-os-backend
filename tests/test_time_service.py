import pytest
import time
import asyncio
from services.time_service.math_engine import calculate_all, get_jd_tt
import services.time_service.delay_queue as dq

def test_mars_sol_date_calc():
    """
    Test NASA Mars24 algorithm correctness.
    We test against a known UTC timestamp.
    Example: J2000.0 (Jan 1, 2000, 12:00:00 UTC) = Unix 946728000
    NASA MSD roughly = 44796.0 
    """
    unix_j2000 = 946728000.0
    res = calculate_all(unix_j2000)
    
    # NASA references MSD ~44796.0 at exactly J2000 epoch
    assert 44795.9 < res["msd"] < 44796.1

def test_ist_timezone():
    """
    Test UTC to IST shift (+5:30)
    """
    unix_j2000 = 946728000.0
    res = calculate_all(unix_j2000)
    assert res["utc"] == "2000-01-01T12:00:00Z"
    assert res["ist"] == "2000-01-01T17:30:00+05:30"

@pytest.mark.asyncio
async def test_delay_queue_hold():
    """
    Test that delay queue correctly holds and assigns release times.
    """
    await dq.r.flushall()
    
    await dq.set_delay_config("moon")
    res = await dq.enqueue_message("test_1", "payload")
    
    assert res["delay_applied"] == 1.28
    assert res["release_time"] > time.time() + 1.2
    
    # Test setting custom
    await dq.set_delay_config("custom", 0.05)
    res = await dq.enqueue_message("test_2", "payload")
    
    assert res["delay_applied"] == 0.05

@pytest.mark.asyncio
async def test_delay_queue_pop():
    """
    Test queue processing worker mechanics
    """
    await dq.r.flushall()
    await dq.set_delay_config("custom", 0.05)
    await dq.enqueue_message("pop_1", "val")
    
    # Initially length = 1
    length = await dq.r.zcard(dq.DELAY_ZSET_KEY)
    assert length == 1
    
    # Sleep to allow time offset to breach score threshold
    await asyncio.sleep(0.06)
    
    now = time.time()
    ready = await dq.r.zrangebyscore(dq.DELAY_ZSET_KEY, min="-inf", max=now)
    assert len(ready) == 1
    assert "pop_1::val" in ready[0]

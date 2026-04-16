import os
import asyncio
import logging
import redis.asyncio as redis

logging.basicConfig(level=logging.INFO, format="%(asctime)s [delay_queue] %(message)s")
log = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
DELAY_ZSET_KEY = "imm:delay_queue"
DELAY_CONFIG_KEY = "imm:delay_config"

# Standard one-way light delays (seconds)
DELAY_PROFILES = {
    "none": 0.0,
    "moon": 1.28,
    "mars": 480.0,  # ~8 minutes average
}

r = redis.from_url(REDIS_URL, decode_responses=True)

async def set_delay_config(mode: str, custom_val: float = None):
    val = custom_val if mode == "custom" else DELAY_PROFILES.get(mode, 0.0)
    await r.hset(DELAY_CONFIG_KEY, mapping={"mode": mode, "value": str(val)})
    return {"mode": mode, "value": val}

async def get_delay_config() -> dict:
    data = await r.hgetall(DELAY_CONFIG_KEY)
    if not data:
        return {"mode": "none", "value": 0.0}
    return {"mode": data["mode"], "value": float(data["value"])}

async def enqueue_message(message_id: str, payload: str):
    """Adds a message to be held until current_time + current_delay."""
    import time
    config = await get_delay_config()
    delay_sec = config["value"]
    release_time = time.time() + delay_sec
    
    await r.zadd(DELAY_ZSET_KEY, {f"{message_id}::{payload}": release_time})
    log.info(f"Queued msg {message_id} with {delay_sec}s delay (release @ {release_time})")
    return {"message_id": message_id, "delay_applied": delay_sec, "release_time": release_time}

async def process_queue():
    """Background worker popping ready messages continuously."""
    import time
    log.info("Started Redis delay queue processor.")
    while True:
        try:
            now = time.time()
            # Get all elements with score <= now
            ready_msgs = await r.zrangebyscore(DELAY_ZSET_KEY, min="-inf", max=now)
            if ready_msgs:
                for item in ready_msgs:
                    msg_id, payload = item.split("::", 1)
                    log.info(f"RELEASED delayed message: {msg_id}")
                    # In a full edge routing implementation, this is where it forwards to MCC.
                    await r.zrem(DELAY_ZSET_KEY, item)
                    
        except Exception as e:
            log.error(f"Queue worker error: {e}")
            
        await asyncio.sleep(0.05)  # 50ms polling loop

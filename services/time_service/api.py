import time
import asyncio
from typing import Optional
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from math_engine import calculate_all, convert_tz
import delay_queue

app = FastAPI(title="IMM-OS Time Service", version="1.0.0")

class DelayConfigReq(BaseModel):
    mode: str  # "none", "moon", "mars", "custom"
    custom_val: Optional[float] = None

class QueueTestReq(BaseModel):
    message_id: str
    payload: str

@app.on_event("startup")
async def startup_event():
    # Start the delay queue processor in background
    asyncio.create_task(delay_queue.process_queue())

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/api/v1/time/now")
def get_time_now():
    """Returns current time in IST, UTC, LST, CMT, MSD simultaneously"""
    return calculate_all(time.time())

@app.get("/api/v1/time/convert")
def time_convert(from_tz: str, to_tz: str, ts: float):
    """Converts any timestamp between supported planetary zones."""
    supported = ["utc", "ist", "lst", "msd", "cmt"]
    if from_tz.lower() not in supported or to_tz.lower() not in supported:
        raise HTTPException(status_code=400, detail="Invalid timezone selected.")
        
    # We strictly use the provided Unix TS to generate the conversions.
    # The 'convert_tz' function computes properties at 'ts'.
    return convert_tz(from_tz.lower(), to_tz.lower(), ts)

@app.post("/api/v1/time/delay/config")
async def set_delay_config(req: DelayConfigReq):
    if req.mode not in ["none", "moon", "mars", "custom"]:
        raise HTTPException(status_code=400, detail="Invalid mode. Supported: none, moon, mars, custom")
    if req.mode == "custom" and req.custom_val is None:
        raise HTTPException(status_code=400, detail="custom_val required for custom mode")
        
    return await delay_queue.set_delay_config(req.mode, req.custom_val)

@app.get("/api/v1/time/delay")
async def get_delay():
    return await delay_queue.get_delay_config()

@app.post("/api/v1/time/delay/queue/test_push")
async def test_queue_push(req: QueueTestReq):
    return await delay_queue.enqueue_message(req.message_id, req.payload)

# If run as standalone via standard python instead of uvicorn (only for dev tests)
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)

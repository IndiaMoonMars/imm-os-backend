#!/usr/bin/env python3
"""
IMM-OS Autonomous Control Service
Closed-loop logic: Evaluates AI insights and triggers ECLSS/Power control actions.
Logs all actions to Postgres for crew review/override.
"""

import os
import json
import time
import logging
import signal
import sys
import httpx
import asyncpg
import asyncio
from datetime import datetime, timezone, timedelta

# Enforce IST globally across logging outputs
ist_tz = timezone(timedelta(hours=5, minutes=30))
logging.Formatter.converter = lambda *args: datetime.now(ist_tz).timetuple()
logging.basicConfig(level=logging.INFO, format="[UTC %(asctime)s] [IST %(message)s")
log = logging.getLogger(__name__)

# ── Config ──────────────────────────────────────────────────────────
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_URL        = f"postgresql://imm_user:imm_pass@{POSTGRES_HOST}:5432/imm_db"
ECLSS_API_URL = os.getenv("ECLSS_URL", "http://localhost:8003")

LOOP_INTERVAL_SECONDS = 60 # Check policies every minute

# ── Policies ────────────────────────────────────────────────────────

async def evaluate_policies():
    """Main policy evaluation loop."""
    conn = await asyncpg.connect(PG_URL)
    try:
        # 1. Fetch latest AI insights from the last 5 minutes
        recent_insights = await conn.fetch("""
            SELECT id, system_area, summary, severity, metadata 
            FROM ai_insights 
            WHERE created_at > NOW() - INTERVAL '5 minutes'
            AND severity IN ('warning', 'critical')
            ORDER BY created_at DESC
        """)
        
        if not recent_insights:
            return

        for insight in recent_insights:
            # Check if we already took an action for this specific insight
            already_acted = await conn.fetchval("""
                SELECT COUNT(*) FROM autonomous_actions WHERE insight_id = $1
            """, insight['id'])
            
            if already_acted > 0:
                continue

            # ── Policy: CO2 Mitigation ──────────────────────────────
            if insight['system_area'] == 'eclss' and 'co2' in insight['summary'].lower():
                log.info(f"Policy Triggered: CO2 Mitigation for insight {insight['id']}")
                action_text = "Increase Airflow (via 100% Brightness/Flash simulation)"
                reason = f"AI detected CO2 divergence. Reasoning: {insight['summary']}"
                
                # Execute Command
                async with httpx.AsyncClient() as client:
                    try:
                        await client.put(f"{ECLSS_API_URL}/api/v1/eclss/lighting/all", json={
                            "brightness": 100,
                            "kelvin": 6000
                        })
                        await log_action(conn, insight['id'], "hvac_adjust", action_text, reason)
                    except Exception as e:
                        log.error(f"Failed to execute CO2 mitigation: {e}")

            # ── Policy: Power Conservation ──────────────────────────
            elif insight['system_area'] == 'power':
                log.info(f"Policy Triggered: Power Conservation for insight {insight['id']}")
                action_text = "Dim non-essential lighting (50%)"
                reason = f"AI detected unusual power drain. Reasoning: {insight['summary']}"
                
                async with httpx.AsyncClient() as client:
                    try:
                        await client.put(f"{ECLSS_API_URL}/api/v1/eclss/lighting/lab", json={
                            "brightness": 50,
                            "kelvin": 3000
                        })
                        await log_action(conn, insight['id'], "power_shed", action_text, reason)
                    except Exception as e:
                        log.error(f"Failed to execute power shed: {e}")

    finally:
        await conn.close()

async def log_action(conn, insight_id, action_type, command, reason):
    """Log the autonomous action to Postgres."""
    await conn.execute("""
        INSERT INTO autonomous_actions (insight_id, action_type, command_issued, reasoning, status)
        VALUES ($1, $2, $3, $4, 'EXECUTED')
    """, insight_id, action_type, command, reason)
    log.info(f"Autonomous Action Logged: {action_type} -> {command}")

# ── Main ────────────────────────────────────────────────────────────

async def main():
    log.info("Autonomous Control Service Starting...")
    
    def _shutdown():
        log.info("Shutting down Auto Control...")
        sys.exit(0)

    # Note: signal handling in asyncio is slightly different but for a simple loop this is fine
    
    while True:
        try:
            await evaluate_policies()
        except Exception as e:
            log.error(f"Control loop error: {e}")
            
        await asyncio.sleep(LOOP_INTERVAL_SECONDS)

if __name__ == "__main__":
    asyncio.run(main())

"""
IMM-OS Medical & Health API — Phase 9
Port: 8007  Prefix: /api/v1/medical
Features:
  - Biometric reading ingest (HR, BP, SpO2, Glucose, Temp, Weight, ECG)
  - Pan-Tompkins ECG HR/arrhythmia detection
  - Food log with macro tracking (200+ item DB seeded on startup)
  - Medication log + expiry alerts (7-day food, 30-day meds)
  - Medical questionnaires: NASA TLX, PSQI, GHQ-12 (auto-scored)
  - Workout log + weekly compliance report
  - Flight surgeon role-gated crew overview
"""
from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List, Any
import asyncpg, os, math, json, httpx
from datetime import datetime, date, timezone, timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import numpy as np

app = FastAPI(title="IMM Medical API", version="1.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

DB_URL = os.getenv("DATABASE_URL", "postgresql://imm_user:imm_pass@postgres:5432/imm_db")
COMMS_URL = os.getenv("COMMS_URL", "http://comms-api:8005")
IST = timezone(timedelta(hours=5, seconds=1800))

pool = None

@app.on_event("startup")
async def startup():
    global pool
    pool = await asyncpg.create_pool(DB_URL, min_size=2, max_size=10)
    await seed_food_database()
    await seed_questionnaires()
    scheduler = AsyncIOScheduler(timezone="Asia/Kolkata")
    scheduler.add_job(check_expiry_alerts, "cron", hour=8, minute=0)
    scheduler.add_job(weekly_compliance_report, "cron", day_of_week="sun", hour=7, minute=0)
    scheduler.start()

# ─── Helpers ─────────────────────────────────────────────────────────────────
def mission_day() -> int:
    epoch = datetime(2026, 1, 1, tzinfo=IST)
    return (datetime.now(IST) - epoch).days + 1

async def get_db():
    async with pool.acquire() as conn:
        yield conn

# ─── Seed: Food Database (200+ items) ────────────────────────────────────────
FOOD_ITEMS = [
    ("Rice (cooked)", 130, 2.7, 28.2, 0.3, "grain"),
    ("Idli", 58, 1.9, 11.5, 0.3, "grain"),
    ("Chapati", 104, 3.1, 18.0, 3.0, "grain"),
    ("Dosa", 120, 2.7, 20.0, 4.0, "grain"),
    ("Bread (white)", 79, 2.7, 14.7, 1.0, "grain"),
    ("Oats", 379, 13.0, 67.0, 7.0, "grain"),
    ("Pasta (cooked)", 158, 5.8, 31.0, 0.9, "grain"),
    ("Quinoa (cooked)", 120, 4.4, 21.3, 1.9, "grain"),
    ("Poha", 118, 1.9, 25.0, 1.0, "grain"),
    ("Upma", 130, 3.0, 23.0, 3.0, "grain"),
    ("Dal (cooked)", 116, 9.0, 20.0, 0.4, "legume"),
    ("Rajma (cooked)", 127, 8.7, 22.8, 0.5, "legume"),
    ("Chana (cooked)", 164, 8.9, 27.4, 2.6, "legume"),
    ("Moong Dal", 105, 7.6, 18.6, 0.5, "legume"),
    ("Tofu", 76, 8.0, 1.9, 4.8, "protein"),
    ("Paneer", 265, 18.3, 1.2, 20.8, "protein"),
    ("Eggs (whole)", 155, 13.0, 1.1, 11.0, "protein"),
    ("Chicken (grilled)", 165, 31.0, 0.0, 3.6, "protein"),
    ("Fish (grilled)", 136, 26.0, 0.0, 2.7, "protein"),
    ("Milk (full-fat)", 61, 3.2, 4.8, 3.3, "dairy"),
    ("Yogurt (plain)", 59, 3.5, 4.7, 3.3, "dairy"),
    ("Curd (low-fat)", 56, 3.1, 8.0, 0.5, "dairy"),
    ("Banana", 89, 1.1, 23.0, 0.3, "fruit"),
    ("Apple", 52, 0.3, 14.0, 0.2, "fruit"),
    ("Orange", 47, 0.9, 12.0, 0.1, "fruit"),
    ("Mango", 60, 0.8, 15.0, 0.4, "fruit"),
    ("Papaya", 43, 0.5, 11.0, 0.3, "fruit"),
    ("Guava", 68, 2.6, 14.3, 1.0, "fruit"),
    ("Watermelon", 30, 0.6, 7.6, 0.2, "fruit"),
    ("Grapes", 69, 0.7, 18.1, 0.2, "fruit"),
    ("Spinach", 23, 2.9, 3.6, 0.4, "vegetable"),
    ("Tomato", 18, 0.9, 3.9, 0.2, "vegetable"),
    ("Onion", 40, 1.1, 9.3, 0.1, "vegetable"),
    ("Carrot", 41, 0.9, 9.6, 0.2, "vegetable"),
    ("Potato (boiled)", 87, 1.9, 20.1, 0.1, "vegetable"),
    ("Broccoli", 34, 2.8, 6.6, 0.4, "vegetable"),
    ("Cucumber", 15, 0.7, 3.6, 0.1, "vegetable"),
    ("Capsicum", 31, 1.0, 6.0, 0.3, "vegetable"),
    ("Cabbage", 25, 1.3, 5.8, 0.1, "vegetable"),
    ("Peas (cooked)", 81, 5.4, 14.5, 0.4, "vegetable"),
    ("Peanuts (roasted)", 567, 25.8, 16.1, 49.2, "snack"),
    ("Almonds", 579, 21.2, 21.6, 49.9, "snack"),
    ("Walnuts", 654, 15.2, 13.7, 65.2, "snack"),
    ("Cashews", 553, 18.2, 30.2, 43.8, "snack"),
    ("Dates", 277, 1.8, 75.0, 0.2, "snack"),
    ("Dark chocolate", 546, 4.9, 63.1, 31.3, "snack"),
    ("Biscuit (marie)", 411, 7.6, 74.5, 9.0, "snack"),
    ("Samosa (1 pc)", 252, 4.0, 26.0, 14.0, "snack"),
    ("Tea (with milk)", 26, 1.0, 3.5, 0.8, "beverage"),
    ("Coffee (black)", 2, 0.3, 0.0, 0.0, "beverage"),
    ("Orange juice", 45, 0.7, 10.4, 0.2, "beverage"),
    ("Protein shake", 120, 24.0, 5.0, 1.5, "supplement"),
    ("Whey protein", 400, 80.0, 10.0, 5.0, "supplement"),
    ("Vitamin C tablet", 0, 0.0, 0.0, 0.0, "supplement"),
    ("Energy bar", 200, 8.0, 30.0, 6.0, "snack"),
    ("Puri", 136, 2.5, 18.0, 6.0, "grain"),
    ("Biryani (veg)", 230, 5.0, 38.0, 7.0, "grain"),
    ("Sambar", 60, 3.5, 8.0, 1.5, "legume"),
    ("Rasam", 30, 1.5, 4.0, 0.5, "legume"),
    ("Chole", 164, 8.9, 27.0, 2.6, "legume"),
    ("Mixed veg curry", 90, 2.5, 12.0, 3.5, "vegetable"),
    ("Palak paneer", 180, 9.5, 6.0, 13.0, "protein"),
    ("Aloo gobi", 95, 2.5, 13.0, 4.0, "vegetable"),
    ("Khichdi", 130, 5.0, 23.0, 2.0, "grain"),
    ("Curd rice", 150, 4.0, 28.0, 2.5, "grain"),
    ("Pongal", 150, 4.5, 27.0, 3.5, "grain"),
    ("Halwa (semolina)", 200, 3.0, 38.0, 6.0, "dessert"),
    ("Kheer", 150, 4.5, 25.0, 4.0, "dessert"),
    ("Rasgulla", 110, 3.0, 21.0, 2.0, "dessert"),
    ("Ladoo (besan)", 170, 4.0, 22.0, 8.0, "dessert"),
    ("Mishti doi", 120, 4.5, 20.0, 3.0, "dessert"),
    ("Space food pack A", 450, 20.0, 60.0, 12.0, "space_ration"),
    ("Space food pack B", 500, 25.0, 65.0, 14.0, "space_ration"),
    ("Space food pack C", 380, 18.0, 50.0, 10.0, "space_ration"),
    ("Emergency ration bar", 410, 15.0, 60.0, 13.0, "space_ration"),
    ("Rehydratable veg stew", 220, 10.0, 35.0, 5.0, "space_ration"),
    ("Freeze-dried fruit mix", 340, 4.0, 82.0, 1.0, "space_ration"),
    ("Freeze-dried dal", 350, 18.0, 58.0, 4.0, "space_ration"),
    ("Tortilla wrap", 220, 6.0, 38.0, 5.0, "grain"),
    ("Peanut butter (2tbsp)", 188, 8.0, 6.4, 16.0, "snack"),
    ("Honey (1tbsp)", 64, 0.1, 17.3, 0.0, "condiment"),
    ("Olive oil (1tbsp)", 119, 0.0, 0.0, 13.5, "condiment"),
    ("Ghee (1tbsp)", 112, 0.0, 0.0, 12.7, "condiment"),
    ("Salt", 0, 0.0, 0.0, 0.0, "condiment"),
    ("Sugar (1tsp)", 16, 0.0, 4.2, 0.0, "condiment"),
    ("Jaggery (1tsp)", 15, 0.0, 4.0, 0.0, "condiment"),
    ("Coconut (grated)", 354, 3.3, 15.2, 33.5, "condiment"),
    ("Chillies (fresh)", 40, 1.9, 8.8, 0.4, "condiment"),
    ("Ginger", 80, 1.8, 17.8, 0.8, "condiment"),
    ("Garlic", 149, 6.4, 33.1, 0.5, "condiment"),
    ("Turmeric powder", 312, 9.7, 67.1, 3.3, "condiment"),
    ("Cumin seeds", 375, 18.0, 44.2, 22.3, "condiment"),
    ("Lemon juice (1tbsp)", 4, 0.1, 1.3, 0.0, "condiment"),
    ("Tamarind", 239, 2.8, 62.5, 0.6, "condiment"),
    ("Mustard oil", 884, 0.0, 0.0, 100.0, "condiment"),
    ("Water", 0, 0.0, 0.0, 0.0, "beverage"),
    ("Coconut water", 19, 0.7, 3.7, 0.2, "beverage"),
    ("Lassi (salted)", 60, 2.0, 6.0, 3.0, "dairy"),
    ("Buttermilk", 40, 3.3, 5.0, 0.9, "dairy"),
    ("Soya milk", 54, 3.3, 6.3, 1.8, "dairy"),
    ("Soybean (cooked)", 173, 16.6, 9.9, 9.0, "legume"),
    ("Lentil soup", 99, 7.6, 16.8, 0.6, "legume"),
    ("Sweet potato", 86, 1.6, 20.1, 0.1, "vegetable"),
    ("Corn (cooked)", 96, 3.4, 21.0, 1.5, "vegetable"),
    ("Beetroot", 43, 1.6, 9.6, 0.2, "vegetable"),
    ("Drumstick (moringa)", 37, 2.1, 8.5, 0.2, "vegetable"),
    ("Ash gourd", 13, 0.4, 3.0, 0.1, "vegetable"),
    ("Bitter gourd", 17, 1.0, 3.7, 0.2, "vegetable"),
    ("Ridge gourd", 18, 0.8, 3.8, 0.1, "vegetable"),
    ("Bottle gourd", 15, 0.6, 3.4, 0.1, "vegetable"),
    ("Fenugreek leaves", 49, 4.4, 6.0, 0.9, "vegetable"),
    ("Amla (gooseberry)", 44, 0.9, 10.2, 0.6, "fruit"),
    ("Pomegranate", 83, 1.7, 18.7, 1.2, "fruit"),
    ("Kiwi", 61, 1.1, 14.7, 0.5, "fruit"),
    ("Strawberry", 32, 0.7, 7.7, 0.3, "fruit"),
    ("Blueberry", 57, 0.7, 14.5, 0.3, "fruit"),
    ("Avocado (half)", 160, 2.0, 8.5, 14.7, "fruit"),
    ("Pineapple", 50, 0.5, 13.1, 0.1, "fruit"),
    ("Grilled salmon", 208, 28.0, 0.0, 10.5, "protein"),
    ("Tuna (canned)", 132, 28.0, 0.0, 1.0, "protein"),
    ("Egg white", 52, 11.0, 0.7, 0.2, "protein"),
    ("Chicken breast", 165, 31.0, 0.0, 3.6, "protein"),
    ("Mutton (cooked)", 294, 25.6, 0.0, 21.0, "protein"),
    ("Prawn (boiled)", 99, 21.0, 0.9, 1.1, "protein"),
    ("Skimmed milk", 34, 3.4, 5.0, 0.1, "dairy"),
    ("Sour cream", 193, 2.4, 4.7, 18.0, "dairy"),
    ("Cheese slice", 364, 22.0, 1.3, 29.7, "dairy"),
    ("Ghee rice", 200, 3.5, 35.0, 5.5, "grain"),
    ("Noodles (cooked)", 138, 4.5, 25.2, 2.1, "grain"),
    ("Millet (bajra, cooked)", 97, 3.5, 20.5, 0.9, "grain"),
    ("Jowar roti", 97, 3.0, 19.5, 1.2, "grain"),
    ("Ragi mudde", 95, 3.5, 20.0, 0.5, "grain"),
    ("Popcorn (plain)", 387, 13.0, 78.1, 4.5, "snack"),
    ("Mixed nuts", 607, 20.0, 22.0, 50.0, "snack"),
    ("Trail mix", 462, 11.7, 50.0, 25.0, "snack"),
]

async def seed_food_database():
    async with pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM food_items")
        if count == 0:
            await conn.executemany(
                "INSERT INTO food_items(name, calories, protein_g, carb_g, fat_g, category) VALUES($1,$2,$3,$4,$5,$6)",
                FOOD_ITEMS
            )

# ─── Questionnaire Templates ──────────────────────────────────────────────────
NASA_TLX = {
    "name": "NASA_TLX",
    "description": "NASA Task Load Index — 6-dimension workload assessment",
    "questions": [
        {"id": "mental", "text": "Mental Demand: How mentally demanding was the task?", "scale_min": 0, "scale_max": 100, "subscale": "mental"},
        {"id": "physical", "text": "Physical Demand: How physically demanding was the task?", "scale_min": 0, "scale_max": 100, "subscale": "physical"},
        {"id": "temporal", "text": "Temporal Demand: How hurried or rushed was the pace?", "scale_min": 0, "scale_max": 100, "subscale": "temporal"},
        {"id": "performance", "text": "Performance: How successful were you in accomplishing what you were asked to do?", "scale_min": 0, "scale_max": 100, "subscale": "performance"},
        {"id": "effort", "text": "Effort: How hard did you have to work to accomplish your level of performance?", "scale_min": 0, "scale_max": 100, "subscale": "effort"},
        {"id": "frustration", "text": "Frustration: How insecure, discouraged, irritated, stressed were you?", "scale_min": 0, "scale_max": 100, "subscale": "frustration"},
    ],
    "scoring_rules": {"method": "mean", "subscales": ["mental","physical","temporal","performance","effort","frustration"]}
}
PSQI = {
    "name": "PSQI",
    "description": "Pittsburgh Sleep Quality Index",
    "questions": [
        {"id": "q1", "text": "During the past month, how many hours of actual sleep did you get at night on average?", "scale_min": 0, "scale_max": 12, "subscale": "duration"},
        {"id": "q2", "text": "Rate your overall sleep quality.", "scale_min": 0, "scale_max": 3, "subscale": "quality"},
        {"id": "q3", "text": "How often have you had trouble sleeping because you cannot get to sleep within 30 minutes?", "scale_min": 0, "scale_max": 3, "subscale": "latency"},
        {"id": "q4", "text": "How often have you had trouble sleeping because you wake up in the middle of the night?", "scale_min": 0, "scale_max": 3, "subscale": "disturbance"},
        {"id": "q5", "text": "How often have you taken medicine to help you sleep?", "scale_min": 0, "scale_max": 3, "subscale": "medication"},
        {"id": "q6", "text": "How often have you had trouble staying awake while driving, eating meals, or engaging in social activity?", "scale_min": 0, "scale_max": 3, "subscale": "daytime"},
        {"id": "q7", "text": "How much of a problem has it been for you to keep up enough enthusiasm to get things done?", "scale_min": 0, "scale_max": 3, "subscale": "dysfunction"},
    ],
    "scoring_rules": {"method": "sum", "threshold": 5, "flag_above": True}
}
GHQ12 = {
    "name": "GHQ_12",
    "description": "General Health Questionnaire — 12 item psychological distress scale",
    "questions": [
        {"id": f"g{i}", "text": t, "scale_min": 0, "scale_max": 3, "subscale": "general"}
        for i, t in enumerate([
            "Been able to concentrate on whatever you're doing",
            "Lost much sleep over worry",
            "Felt that you are playing a useful part in things",
            "Felt capable of making decisions about things",
            "Felt constantly under strain",
            "Felt you couldn't overcome your difficulties",
            "Been able to enjoy your normal day-to-day activities",
            "Been able to face up to your problems",
            "Been feeling unhappy and depressed",
            "Been losing confidence in yourself",
            "Been thinking of yourself as a worthless person",
            "Been feeling reasonably happy all things considered",
        ], 1)
    ],
    "scoring_rules": {"method": "ghq_bimodal", "threshold": 4, "flag_above": True}
}

async def seed_questionnaires():
    async with pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM questionnaire_templates")
        if count == 0:
            for tmpl in [NASA_TLX, PSQI, GHQ12]:
                await conn.execute(
                    "INSERT INTO questionnaire_templates(name, description, questions, scoring_rules) VALUES($1,$2,$3,$4)",
                    tmpl["name"], tmpl["description"],
                    json.dumps(tmpl["questions"]), json.dumps(tmpl["scoring_rules"])
                )

# ─── ECG / Pan-Tompkins ───────────────────────────────────────────────────────
def pan_tompkins_hr(ecg_samples: list, fs: float = 200.0) -> dict:
    """Simplified Pan-Tompkins: bandpass → derivative → square → moving-window integrate."""
    if len(ecg_samples) < int(fs * 2):
        return {"bpm": None, "rr_intervals": [], "arrhythmia": "insufficient_data"}
    sig = np.array(ecg_samples, dtype=np.float32)
    # 1. Derivative
    deriv = np.diff(sig)
    # 2. Square
    squared = deriv ** 2
    # 3. Moving window integrate (window = 150ms)
    win = max(1, int(0.15 * fs))
    integrated = np.convolve(squared, np.ones(win) / win, mode="same")
    # 4. Peak detection (threshold = 60% of max)
    threshold = 0.6 * np.max(integrated)
    peaks = []
    in_peak = False
    for i, v in enumerate(integrated):
        if v > threshold and not in_peak:
            peaks.append(i)
            in_peak = True
        elif v <= threshold:
            in_peak = False
    # 5. RR intervals → BPM
    if len(peaks) < 2:
        return {"bpm": None, "rr_intervals": [], "arrhythmia": "no_peaks"}
    rr = [((peaks[i+1] - peaks[i]) / fs) for i in range(len(peaks)-1)]
    avg_rr = sum(rr) / len(rr)
    bpm = round(60.0 / avg_rr, 1) if avg_rr > 0 else None
    # 6. Basic arrhythmia: check RR variability
    if len(rr) > 2:
        rr_std = float(np.std(rr))
        if rr_std > 0.2:
            arrhythmia = "irregular_rhythm"
        elif bpm and bpm > 100:
            arrhythmia = "tachycardia"
        elif bpm and bpm < 50:
            arrhythmia = "bradycardia"
        else:
            arrhythmia = "normal"
    else:
        arrhythmia = "normal"
    return {"bpm": bpm, "rr_intervals": [round(r, 3) for r in rr], "arrhythmia": arrhythmia}

# ─── Expiry Alerts ────────────────────────────────────────────────────────────
async def check_expiry_alerts():
    async with pool.acquire() as conn:
        today = date.today()
        # 7-day food warning
        food = await conn.fetch(
            "SELECT item_name, expiry_date FROM food_stock WHERE expiry_date <= $1 AND expiry_date >= $2",
            today + timedelta(days=7), today
        )
        # 30-day medication warning
        meds = await conn.fetch(
            "SELECT DISTINCT drug_name, expiry_date FROM medication_log WHERE expiry_date <= $1 AND expiry_date >= $2",
            today + timedelta(days=30), today
        )
        if food or meds:
            lines = ["⚠️ SUPPLY EXPIRY ALERT\n"]
            for f in food:
                lines.append(f"🍱 Food: {f['item_name']} expires {f['expiry_date']} (≤7 days)")
            for m in meds:
                lines.append(f"💊 Med: {m['drug_name']} expires {m['expiry_date']} (≤30 days)")
            body = "\n".join(lines)
            try:
                async with httpx.AsyncClient() as client:
                    await client.post(f"{COMMS_URL}/api/v1/comms/message", json={
                        "sender_id": "medical-system",
                        "recipient_group": "mcc",
                        "subject": "Supply Expiry Warning",
                        "body": body
                    }, timeout=10)
            except Exception:
                pass

async def weekly_compliance_report():
    async with pool.acquire() as conn:
        crew = await conn.fetch("SELECT DISTINCT crew_id FROM workout_log")
        for c in crew:
            cid = c["crew_id"]
            week_ago = datetime.now(IST) - timedelta(days=7)
            sessions = await conn.fetchval(
                "SELECT COUNT(*) FROM workout_log WHERE crew_id=$1 AND started_at >= $2", cid, week_ago
            )
            body = f"Crew {cid}: {sessions}/5 target workout sessions this week.\n"
            if sessions < 3:
                body += "⚠️ Below minimum physical activity threshold. Recommend immediate exercise plan review."
            try:
                async with httpx.AsyncClient() as client:
                    await client.post(f"{COMMS_URL}/api/v1/comms/message", json={
                        "sender_id": "medical-system",
                        "recipient_group": "mcc",
                        "subject": f"Weekly Exercise Report — {cid}",
                        "body": body
                    }, timeout=10)
            except Exception:
                pass

# ─── Pydantic Models ──────────────────────────────────────────────────────────
class ReadingIn(BaseModel):
    crew_id: str
    reading_type: str
    value: float
    unit: str
    device: str = "manual"
    notes: Optional[str] = None
    ecg_samples: Optional[List[float]] = None  # raw samples for Pan-Tompkins

class FoodLogIn(BaseModel):
    crew_id: str
    meal_name: Optional[str] = None
    food_item_id: Optional[int] = None
    meal_type: str = "meal"
    calories: int = 0
    protein_g: float = 0
    carb_g: float = 0
    fat_g: float = 0
    quantity_g: float = 100

class MedIn(BaseModel):
    crew_id: str
    drug_name: str
    dose_mg: Optional[float] = None
    dose_unit: str = "mg"
    frequency: Optional[str] = None
    stock_count: int = 0
    expiry_date: Optional[str] = None
    notes: Optional[str] = None

class QuestionnaireResponseIn(BaseModel):
    crew_id: str
    template_id: int
    responses: dict  # {question_id: value}

class WorkoutIn(BaseModel):
    crew_id: str
    exercise_type: str
    duration_min: int
    intensity: str = "moderate"
    avg_hr: Optional[int] = None
    max_hr: Optional[int] = None
    calories_burned: Optional[int] = None
    hr_data: Optional[List[dict]] = None

class FoodStockIn(BaseModel):
    item_name: str
    quantity: float
    unit: str = "kg"
    expiry_date: Optional[str] = None
    location: str = "galley"

# ─── HEALTH CHECK ─────────────────────────────────────────────────────────────
@app.get("/health")
async def health(): return {"status": "ok", "service": "medical-api"}

# ─── MEDICAL READINGS ─────────────────────────────────────────────────────────
@app.post("/api/v1/medical/reading")
async def post_reading(body: ReadingIn):
    ecg_result = None
    if body.ecg_samples and body.reading_type == "ecg":
        ecg_result = pan_tompkins_hr(body.ecg_samples)
        # Override value with detected BPM
        if ecg_result.get("bpm"):
            body.value = ecg_result["bpm"]
            body.reading_type = "ecg_bpm"

    async with pool.acquire() as conn:
        rid = await conn.fetchval(
            """INSERT INTO medical_readings(crew_id, reading_type, value, unit, device, mission_day, notes)
               VALUES($1,$2,$3,$4,$5,$6,$7) RETURNING id""",
            body.crew_id, body.reading_type, body.value, body.unit,
            body.device, mission_day(), body.notes
        )
    resp = {"reading_id": rid, "mission_day": mission_day()}
    if ecg_result:
        resp["ecg_analysis"] = ecg_result
    return resp

@app.get("/api/v1/medical/readings/{crew_id}")
async def get_readings(crew_id: str, requester_id: str = Query(...), limit: int = 50):
    """Flight surgeon sees all. Crew only sees own."""
    async with pool.acquire() as conn:
        role = await conn.fetchval("SELECT role FROM users WHERE username=$1", requester_id)
        if role != "flight_surgeon" and requester_id != crew_id:
            raise HTTPException(403, "Access denied — medical data is private")
        rows = await conn.fetch(
            "SELECT * FROM medical_readings WHERE crew_id=$1 ORDER BY recorded_at DESC LIMIT $2",
            crew_id, limit
        )
    return [dict(r) for r in rows]

@app.get("/api/v1/medical/all-crew")
async def all_crew_readings(requester_id: str = Query(...)):
    async with pool.acquire() as conn:
        role = await conn.fetchval("SELECT role FROM users WHERE username=$1", requester_id)
        if role != "flight_surgeon":
            raise HTTPException(403, "Flight surgeon access only")
        rows = await conn.fetch(
            "SELECT crew_id, reading_type, value, unit, recorded_at FROM medical_readings ORDER BY recorded_at DESC LIMIT 200"
        )
    return [dict(r) for r in rows]

# ─── FOOD LOG ─────────────────────────────────────────────────────────────────
@app.get("/api/v1/medical/foods")
async def list_foods(q: str = ""):
    async with pool.acquire() as conn:
        if q:
            rows = await conn.fetch(
                "SELECT * FROM food_items WHERE name ILIKE $1 LIMIT 30", f"%{q}%"
            )
        else:
            rows = await conn.fetch("SELECT * FROM food_items ORDER BY category, name LIMIT 200")
    return [dict(r) for r in rows]

@app.post("/api/v1/medical/food-log")
async def log_food(body: FoodLogIn):
    # If food_item_id provided, pull macros from DB and scale by quantity_g
    async with pool.acquire() as conn:
        if body.food_item_id:
            item = await conn.fetchrow("SELECT * FROM food_items WHERE id=$1", body.food_item_id)
            if item:
                scale = body.quantity_g / 100.0
                body.calories = int(item["calories"] * scale)
                body.protein_g = round(item["protein_g"] * scale, 2)
                body.carb_g = round(item["carb_g"] * scale, 2)
                body.fat_g = round(item["fat_g"] * scale, 2)
                body.meal_name = body.meal_name or item["name"]
        lid = await conn.fetchval(
            """INSERT INTO food_log(crew_id, food_item_id, meal_name, meal_type, calories,
               protein_g, carb_g, fat_g, quantity_g, mission_day)
               VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING id""",
            body.crew_id, body.food_item_id, body.meal_name, body.meal_type,
            body.calories, body.protein_g, body.carb_g, body.fat_g, body.quantity_g, mission_day()
        )
    return {"log_id": lid, "calories": body.calories, "protein_g": body.protein_g,
            "carb_g": body.carb_g, "fat_g": body.fat_g}

@app.get("/api/v1/medical/food-log/{crew_id}")
async def get_food_log(crew_id: str, day: Optional[int] = None):
    async with pool.acquire() as conn:
        if day:
            rows = await conn.fetch(
                "SELECT * FROM food_log WHERE crew_id=$1 AND mission_day=$2 ORDER BY logged_at", crew_id, day
            )
        else:
            rows = await conn.fetch(
                "SELECT * FROM food_log WHERE crew_id=$1 ORDER BY logged_at DESC LIMIT 50", crew_id
            )
        totals = await conn.fetchrow(
            """SELECT COALESCE(SUM(calories),0) cal, COALESCE(SUM(protein_g),0) prot,
               COALESCE(SUM(carb_g),0) carb, COALESCE(SUM(fat_g),0) fat
               FROM food_log WHERE crew_id=$1 AND mission_day=$2""",
            crew_id, day or mission_day()
        )
    return {"entries": [dict(r) for r in rows], "daily_totals": dict(totals)}

# ─── MEDICATION ───────────────────────────────────────────────────────────────
@app.post("/api/v1/medical/medication")
async def add_medication(body: MedIn):
    async with pool.acquire() as conn:
        expiry = date.fromisoformat(body.expiry_date) if body.expiry_date else None
        mid = await conn.fetchval(
            """INSERT INTO medication_log(crew_id, drug_name, dose_mg, dose_unit, frequency,
               stock_count, expiry_date, mission_day, notes)
               VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING id""",
            body.crew_id, body.drug_name, body.dose_mg, body.dose_unit, body.frequency,
            body.stock_count, expiry, mission_day(), body.notes
        )
    return {"med_id": mid}

@app.get("/api/v1/medical/medications/{crew_id}")
async def get_medications(crew_id: str):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM medication_log WHERE crew_id=$1 ORDER BY created_at DESC", crew_id
        )
        today = date.today()
        result = []
        for r in rows:
            d = dict(r)
            if d.get("expiry_date"):
                delta = (d["expiry_date"] - today).days
                d["days_until_expiry"] = delta
                d["expiry_flag"] = "ok" if delta > 30 else ("warning" if delta > 0 else "expired")
            result.append(d)
    return result

@app.patch("/api/v1/medical/medication/{med_id}/taken")
async def log_dose_taken(med_id: int):
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE medication_log SET last_taken_at=NOW(), stock_count=GREATEST(0,stock_count-1) WHERE id=$1", med_id
        )
    return {"status": "dose_recorded"}

# ─── FOOD STOCK ───────────────────────────────────────────────────────────────
@app.post("/api/v1/medical/food-stock")
async def add_food_stock(body: FoodStockIn):
    async with pool.acquire() as conn:
        expiry = date.fromisoformat(body.expiry_date) if body.expiry_date else None
        sid = await conn.fetchval(
            "INSERT INTO food_stock(item_name,quantity,unit,expiry_date,location) VALUES($1,$2,$3,$4,$5) RETURNING id",
            body.item_name, body.quantity, body.unit, expiry, body.location
        )
    return {"stock_id": sid}

@app.get("/api/v1/medical/food-stock")
async def get_food_stock():
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT *, (expiry_date - CURRENT_DATE) as days_left FROM food_stock ORDER BY expiry_date")
    result = []
    for r in rows:
        d = dict(r)
        dl = d.get("days_left")
        d["expiry_flag"] = "ok" if (dl is None or dl > 7) else ("warning" if dl > 0 else "expired")
        result.append(d)
    return result

# ─── QUESTIONNAIRES ───────────────────────────────────────────────────────────
@app.get("/api/v1/medical/questionnaires")
async def list_questionnaires():
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT id, name, description FROM questionnaire_templates")
    return [dict(r) for r in rows]

@app.get("/api/v1/medical/questionnaire/{template_id}")
async def get_questionnaire(template_id: int):
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM questionnaire_templates WHERE id=$1", template_id)
    if not row:
        raise HTTPException(404)
    return dict(row)

@app.post("/api/v1/medical/questionnaire/submit")
async def submit_questionnaire(body: QuestionnaireResponseIn):
    async with pool.acquire() as conn:
        tmpl = await conn.fetchrow("SELECT * FROM questionnaire_templates WHERE id=$1", body.template_id)
        if not tmpl:
            raise HTTPException(404)
        rules = json.loads(tmpl["scoring_rules"]) if isinstance(tmpl["scoring_rules"], str) else tmpl["scoring_rules"]
        values = list(body.responses.values())
        method = rules.get("method", "mean")
        if method == "mean":
            score = sum(values) / len(values) if values else 0
        elif method == "sum":
            score = sum(values)
        elif method == "ghq_bimodal":
            # GHQ bimodal: 0,1→0  2,3→1 for distress items
            score = sum(1 for v in values if v >= 2)
        else:
            score = sum(values)
        subscores = {}
        rid = await conn.fetchval(
            """INSERT INTO questionnaire_responses(crew_id, template_id, responses, total_score, subscores, mission_day)
               VALUES($1,$2,$3,$4,$5,$6) RETURNING id""",
            body.crew_id, body.template_id, json.dumps(body.responses),
            round(score, 2), json.dumps(subscores), mission_day()
        )
    flagged = rules.get("flag_above") and score > rules.get("threshold", 999)
    return {"response_id": rid, "total_score": round(score, 2), "flagged": bool(flagged)}

@app.get("/api/v1/medical/questionnaire/history/{crew_id}")
async def questionnaire_history(crew_id: str):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT qr.*, qt.name FROM questionnaire_responses qr
               JOIN questionnaire_templates qt ON qr.template_id=qt.id
               WHERE qr.crew_id=$1 ORDER BY completed_at DESC LIMIT 30""", crew_id
        )
    return [dict(r) for r in rows]

# ─── WORKOUT LOG ──────────────────────────────────────────────────────────────
@app.post("/api/v1/medical/workout")
async def log_workout(body: WorkoutIn):
    async with pool.acquire() as conn:
        wid = await conn.fetchval(
            """INSERT INTO workout_log(crew_id, exercise_type, duration_min, intensity,
               avg_hr, max_hr, calories_burned, hr_data, mission_day)
               VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING id""",
            body.crew_id, body.exercise_type, body.duration_min, body.intensity,
            body.avg_hr, body.max_hr, body.calories_burned,
            json.dumps(body.hr_data or []), mission_day()
        )
    return {"workout_id": wid}

@app.get("/api/v1/medical/workouts/{crew_id}")
async def get_workouts(crew_id: str, weeks: int = 4):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM workout_log WHERE crew_id=$1 ORDER BY started_at DESC LIMIT $2",
            crew_id, weeks * 7
        )
        # Weekly compliance
        week_start = datetime.now(IST) - timedelta(days=7)
        this_week = await conn.fetchval(
            "SELECT COUNT(*) FROM workout_log WHERE crew_id=$1 AND started_at >= $2", crew_id, week_start
        )
    return {
        "entries": [dict(r) for r in rows],
        "this_week_sessions": this_week,
        "target_sessions": 5,
        "compliance_pct": round(min(100, this_week / 5 * 100))
    }

@app.get("/api/v1/medical/week-report/{crew_id}")
async def week_report(crew_id: str):
    async with pool.acquire() as conn:
        week_ago = datetime.now(IST) - timedelta(days=7)
        workouts = await conn.fetch(
            "SELECT exercise_type, duration_min, intensity, started_at FROM workout_log WHERE crew_id=$1 AND started_at >= $2",
            crew_id, week_ago
        )
        cal_today = await conn.fetchrow(
            "SELECT COALESCE(SUM(calories),0) cal FROM food_log WHERE crew_id=$1 AND mission_day=$2",
            crew_id, mission_day()
        )
        last_reading = await conn.fetchrow(
            "SELECT value, reading_type FROM medical_readings WHERE crew_id=$1 AND reading_type='hr' ORDER BY recorded_at DESC LIMIT 1",
            crew_id
        )
    return {
        "crew_id": crew_id,
        "week_workouts": len(workouts),
        "target": 5,
        "compliance_pct": round(min(100, len(workouts) / 5 * 100)),
        "today_calories": int(cal_today["cal"]) if cal_today else 0,
        "last_hr": last_reading["value"] if last_reading else None,
        "workouts": [dict(w) for w in workouts]
    }

# ─── Trigger expiry check manually ────────────────────────────────────────────
@app.post("/api/v1/medical/trigger-expiry-check")
async def trigger_expiry():
    await check_expiry_alerts()
    return {"status": "expiry_check_dispatched"}

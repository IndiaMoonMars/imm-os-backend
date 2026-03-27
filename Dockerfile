# ── IMM-OS Backend — Dockerfile ──────────────────────────────────
FROM python:3.10-slim

# Metadata
LABEL org.opencontainers.image.title="imm-os-backend"
LABEL org.opencontainers.image.description="India Moon Mars OS Backend API"
LABEL org.opencontainers.image.version="0.1.0"

WORKDIR /app

# Install dependencies first (layer cache optimisation)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY . .

EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=5s --retries=3 \
    CMD python -c "import httpx; httpx.get('http://localhost:8000/health').raise_for_status()"

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

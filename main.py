"""
IMM-OS Backend — Phase 0 skeleton
India Moon Mars Operating System · FastAPI microservice entrypoint
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(
    title="IMM-OS Backend",
    description="India Moon Mars Operating System — Mission Control & Habitat backend API",
    version="0.1.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Restrict in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health", tags=["System"])
async def health():
    """Liveness probe — used by Docker healthcheck and K3s."""
    return {"status": "ok", "service": "imm-os-backend", "version": "0.1.0"}


@app.get("/", tags=["System"])
async def root():
    """Root endpoint — quick sanity check."""
    return {
        "message": "IMM-OS Backend is running",
        "version": "0.1.0",
        "docs": "/api/docs",
    }

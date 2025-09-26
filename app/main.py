# main.py
import time
import logging
from fastapi import FastAPI, Request
from .routers import router as chunk_router

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("chunkr_service")

app = FastAPI(title="Chunkr Service", version="0.1.0")

@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.time()
    try:
        response = await call_next(request)
        duration_ms = int((time.time() - start) * 1000)
        logger.info("%s %s -> %s (%d ms)", request.method, request.url.path, response.status_code, duration_ms)
        return response
    except Exception as exc:  # noqa: BLE001
        duration_ms = int((time.time() - start) * 1000)
        logger.exception("%s %s -> 500 (%d ms) error=%s", request.method, request.url.path, duration_ms, exc)
        raise

@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}

app.include_router(chunk_router, prefix="/v1")

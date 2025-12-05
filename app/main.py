from contextlib import asynccontextmanager
import logging
from typing import Any, Dict
from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse
from redis.asyncio import Redis
from app.core.config import settings
from app.core.logging import configure_logging
from app.cache.redis_client import (
    create_redis_client,
    get_redis_client,
    close_redis_client,
)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context for FastAPI.
    - On startup: configure logging, create Redis client, attach to app.state
    - On shutdown: close Redis client
    """
    configure_logging()
    logger.info("🚀 Starting Async Smart Order Processing System...")
    redis_client = await create_redis_client()
    # redis-client-conn object is attached in app:state for reuse in overall project
    app.state.redis = redis_client
    try:
        # returning controller to main process
        yield
    finally:
        logger.info("🛑 Shutting down Async Smart Order Processing System...")
        await close_redis_client()


# overall app object
app = FastAPI(
    title=settings.app_name,
    version="0.1.0",
    lifespan=lifespan,
)


async def redis_from_app(request: Request) -> Redis:
    """
    Dependency to fetch Redis client from app.state.
    This will be used in many endpoints and services later.
    """
    return request.app.state.redis


@app.get("/system-health")
async def system_health(redis: Redis = Depends(redis_from_app)) -> JSONResponse:
    """
    Real health-check endpoint.
    - Pings Redis asynchronously
    - Returns a structured JSON response
    """
    redis_status: str
    components: Dict[str, Any] = {}
    try:
        pong = await redis.ping()
        redis_status = "up" if pong else "degraded"
    except Exception as exc:  # noqa: BLE001
        logger.exception("Redis health check failed: %s", exc)
        redis_status = "down"
    overall_status = "up" if redis_status == "up" else "degraded"
    components["redis"] = {"status": redis_status}
    return JSONResponse(
        {
            "app": settings.app_name,
            "environment": settings.environment,
            "status": overall_status,
            "components": components,
        }
    )


@app.get("/")
async def root() -> Dict[str, str]:
    """
    Simple landing endpoint to verify app is running.
    """
    return {
        "message": "Welcome to Python Async Smart Order Processing System",
        "docs": "/docs",
        "health": "/system-health",
    }

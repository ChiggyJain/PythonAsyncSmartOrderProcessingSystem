from typing import Optional
from redis.asyncio import Redis
from app.core.config import settings

# global variable for redis-client-con-instances
# here redis is used as asyncio wala concept because development is happening in fastAPI framework
_redis_client: Optional[Redis] = None


async def create_redis_client() -> Redis:
    """
    Create a global async Redis client.
    Called once during application startup.
    """
    global _redis_client
    if _redis_client is None:
        _redis_client = Redis.from_url(settings.redis_url, encoding="utf-8", decode_responses=True)
    return _redis_client


async def get_redis_client() -> Redis:
    """
    Simple accessor used by other modules.
    Assumes create_redis_client() was already called on startup.
    """
    if _redis_client is None:
        # In a real system we might raise custom error or init lazily
        raise RuntimeError("Redis client is not initialized")
    return _redis_client


async def close_redis_client() -> None:
    """
    Gracefully close the Redis connection on shutdown.
    """
    global _redis_client
    if _redis_client is not None:
        await _redis_client.aclose()
        _redis_client = None

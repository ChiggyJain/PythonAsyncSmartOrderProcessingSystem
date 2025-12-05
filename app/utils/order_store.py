import logging
from typing import Any, Dict, Optional
from redis.asyncio import Redis

logger = logging.getLogger(__name__)


async def redis_get_order(redis: Redis, order_id: str) -> Optional[Dict[str, Any]]:
    """
    Fetch order hash from Redis and return as a Python dict.
    Returns:
        None if order does not exist.
    """
    key = f"order:{order_id}"
    if not await redis.exists(key):
        return None
    data = await redis.hgetall(key)
    return data


async def redis_get_order_status(redis: Redis, order_id: str) -> Optional[str]:
    """
    Return only the order status from Redis.
    Useful for lightweight polling and real-time status checks.
    """
    key = f"order:{order_id}"
    if not await redis.exists(key):
        return None
    status = await redis.hget(key, "status")
    return status

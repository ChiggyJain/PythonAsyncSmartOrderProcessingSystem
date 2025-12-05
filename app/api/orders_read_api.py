import logging
from typing import Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Request, status
from redis.asyncio import Redis
from app.utils.order_store import redis_get_order, redis_get_order_status

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/orders", tags=["orders"])


async def redis_from_app(request: Request) -> Redis:
    return request.app.state.redis


@router.get("/{order_id}")
async def get_order(
    order_id: str,
    redis: Redis = Depends(redis_from_app),
) -> Dict[str, Any]:
    """
    Fetch complete order information from Redis.
    Returned format is the full Redis hash used by write pipeline.
    """
    order = await redis_get_order(redis, order_id)
    if order is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "order_not_found", "order_id": order_id},
        )
    logger.info("Order fetched: %s", order_id)
    return order


@router.get("/{order_id}/status")
async def get_order_status(
    order_id: str,
    redis: Redis = Depends(redis_from_app),
) -> Dict[str, Any]:
    """
    Lightweight endpoint to check only status.
    Used by UI polling, background systems, delivery apps, etc.
    """
    status_value = await redis_get_order_status(redis, order_id)
    if status_value is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "order_not_found", "order_id": order_id},
        )
    logger.info("Order status fetched: %s -> %s", order_id, status_value)
    return {
        "order_id": order_id,
        "status": status_value,
    }

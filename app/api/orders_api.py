import logging
from datetime import datetime
from typing import Any, Dict
from fastapi import APIRouter, Depends, HTTPException, Request, status
from redis.asyncio import Redis
from app.cache.redis_client import get_redis_client
from app.core.exceptions import OrderValidationError
from app.schemas.order_schema import OrderCreate, OrderPublic
from app.services.order_validator import validate_order_business_rules
from app.utils.id_generator import generate_order_id

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/orders", tags=["orders"])


async def redis_from_app(request: Request) -> Redis:
    # NOTE: same pattern as in main.py, but local here for testability.
    return request.app.state.redis


@router.post(
    "",
    response_model=OrderPublic,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new order",
)
async def create_order(
    order: OrderCreate,
    redis: Redis = Depends(redis_from_app),
) -> OrderPublic:
    """
    Create a new order.
    Steps:
    1. Validate business rules.
    2. Generate order_id.
    3. Store initial order snapshot in Redis.
    4. Return public representation.
    Kafka dispatch + enrichment will come in later phases.
    """
    try:
        total_amount, total_amount_inr = await validate_order_business_rules(order)
    except OrderValidationError as exc:
        logger.warning("Order validation failed: %s | details=%s", exc.message, exc.details)
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "error": "order_validation_failed",
                "message": exc.message,
                "details": exc.details,
            },
        ) from exc
    order_id = generate_order_id()
    now = datetime.utcnow().isoformat()
    # Prepare minimal snapshot (we can enrich later)
    order_key = f"order:{order_id}"
    # In a real system we'd use Redis JSON or a DB.
    # Here we store as a hash for speed and simplicity.
    order_data: Dict[str, Any] = {
        "order_id": order_id,
        "status": "received",
        "customer_id": order.customer_id,
        "currency": order.currency,
        "total_amount": str(total_amount),
        "total_amount_inr": str(total_amount_inr),
        "created_at": order.created_at.isoformat(),
        # "created_at": None,
        "source": order.source,
        "last_updated_at": now,
    }
    await redis.hset(order_key, mapping=order_data)
    # Optional: set TTL, e.g., 24 hours
    await redis.expire(order_key, 24 * 60 * 60)
    logger.info(
        "Order created: order_id=%s customer_id=%s total=%.2f %s",
        order_id,
        order.customer_id,
        total_amount,
        order.currency,
    )
    return OrderPublic(
        order_id=order_id,
        status="received",
        customer_id=order.customer_id,
        currency=order.currency,
        total_amount=total_amount,
        created_at=order.created_at,
        source=order.source,
    )

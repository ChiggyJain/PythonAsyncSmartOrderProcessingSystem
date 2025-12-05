import logging
from datetime import datetime
from typing import Any, Dict
from fastapi import APIRouter, Depends, HTTPException, Request, status
from redis.asyncio import Redis
from app.core.config import settings
from app.cache.redis_client import get_redis_client
from app.core.exceptions import OrderValidationError, InventoryUnavailableError
from app.schemas.order_schema import OrderCreate, OrderPublic
from app.services.order_validator import validate_order_business_rules
from app.utils.id_generator import generate_order_id
from app.services.inventory_service import reserve_inventory_for_order
from app.brokers.kafka_producer import AsyncKafkaProducer


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
    2. Reserve inventory for all items.
    3. Generate order_id.
    4. Store initial order snapshot in Redis with status "validated".
    """

    try:
        total_amount, total_amount_inr = await validate_order_business_rules(order)
        await reserve_inventory_for_order(redis, order)
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
    except InventoryUnavailableError as exc:
        logger.warning(
            "Inventory unavailable for order: sku=%s requested=%s available=%s",
            exc.sku,
            exc.requested,
            exc.available,
        )
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "inventory_unavailable",
                "message": str(exc),
                "sku": exc.sku,
                "requested": exc.requested,
                "available": exc.available,
            },
        ) from exc
        
    order_id = generate_order_id()
    order_status = "validated"
    now = datetime.utcnow().isoformat()
    
    # order details is stored into redis hset
    order_key = f"order:{order_id}"
    order_data: Dict[str, Any] = {
        "order_id": order_id,
        "status": order_status,
        "customer_id": order.customer_id,
        "currency": order.currency,
        "total_amount": str(total_amount),
        "total_amount_inr": str(total_amount_inr),
        "created_at": order.created_at.isoformat(),
        "source": order.source,
        "last_updated_at": now,
    }
    await redis.hset(order_key, mapping=order_data)
    await redis.expire(order_key, 24 * 60 * 60)

    # producing message to kafka
    kafka_message = {
        "order_id": order_id,
        "customer_id": order.customer_id,
        "currency": order.currency,
        "total_amount": total_amount,
        "total_amount_inr": total_amount_inr,
        "created_at": order.created_at.isoformat(),
        "source": order.source,
    }
    await AsyncKafkaProducer.send(settings.kafka_orders_topic, kafka_message)

    # Update order status → dispatched
    await redis.hset(order_key, "status", "dispatched")

    logger.info(
        "Order created & validated: order_id=%s customer_id=%s total=%.2f %s",
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

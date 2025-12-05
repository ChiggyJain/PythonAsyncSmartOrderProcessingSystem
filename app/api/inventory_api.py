import logging
from typing import Any, Dict
from fastapi import APIRouter, Depends, HTTPException, Request, status
from redis.asyncio import Redis
from app.services.inventory_service import (
    get_inventory,
    load_inventory_from_csv,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/inventory", tags=["inventory"])


async def redis_from_app(request: Request) -> Redis:
    return request.app.state.redis


@router.get("/{sku}")
async def get_inventory_endpoint(
    sku: str,
    redis: Redis = Depends(redis_from_app),
) -> Dict[str, Any]:
    """
    Get current inventory info for a SKU.
    """
    inv = await get_inventory(redis, sku)
    if inv is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "sku_not_found", "sku": sku},
        )
    return inv


@router.post("/reload")
async def reload_inventory_from_csv(
    redis: Redis = Depends(redis_from_app),
) -> Dict[str, Any]:
    """
    Reload inventory data from CSV file.
    Useful for local dev / admin operations.
    """
    count = await load_inventory_from_csv(redis, "sample_data/inventory.csv")
    return {"reloaded_items": count}

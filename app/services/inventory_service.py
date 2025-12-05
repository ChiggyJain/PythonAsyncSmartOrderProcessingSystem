import asyncio
import csv
import logging
import os
from typing import Dict, Optional, List, Tuple
from redis.asyncio import Redis
from app.core.exceptions import InventoryUnavailableError
from app.schemas.order_schema import OrderCreate

logger = logging.getLogger(__name__)
INVENTORY_KEY_PREFIX = "inventory:"


async def load_inventory_from_csv(redis: Redis, file_path: str) -> int:
    """
    Load inventory data from CSV into Redis.
    CSV columns:
      - sku
      - name
      - available_quantity
    """

    if not os.path.exists(file_path):
        logger.warning("Inventory CSV not found at %s", file_path)
        return 0

    # reading csv file function
    def _read_csv() -> list[dict[str, str]]:
        with open(file_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            return list(reader)

    # here asyncio is running new background thread to execute the blocking-wala read_csv function
    # getting all rows as dict form
    rows = await asyncio.to_thread(_read_csv)

    # iterating each row details of extracted csv file
    count = 0
    for row in rows:
        sku = row["sku"].strip().upper()
        name = row["name"].strip()
        try:
            available = int(row["available_quantity"])
        except ValueError:
            logger.warning(
                "Invalid available_quantity for SKU=%s, row=%s", sku, row
            )
            continue
        # storing each inventory item details in redis db
        key = f"{INVENTORY_KEY_PREFIX}{sku}"
        await redis.hset(
            key,
            mapping={
                "sku": sku,
                "name": name,
                "available_quantity": str(available),
                "reserved_quantity": "0",
            },
        )
        count+= 1
    logger.info("Inventory loaded from CSV: %s items", count)
    return count


async def get_inventory(redis: Redis, sku: str) -> Optional[Dict[str, int | str]]:
    """
    Get inventory info for a single SKU.
    """
    # extracting given single inventory item from redis
    sku = sku.strip().upper()
    key = f"{INVENTORY_KEY_PREFIX}{sku}"
    if not await redis.exists(key):
        return None
    data = await redis.hgetall(key)
    return {
        "sku": data.get("sku", sku),
        "name": data.get("name", ""),
        "available_quantity": int(data.get("available_quantity", "0")),
        "reserved_quantity": int(data.get("reserved_quantity", "0")),
    }


async def reserve_inventory(redis: Redis, sku: str, quantity: int) -> None:
    """
    Reserve inventory for a single SKU.
    NOTE: Ye simple version hai (no strict concurrency guarantees).
    Single-instance system / local dev ke liye enough hai.
    """

    sku = sku.strip().upper()
    key = f"{INVENTORY_KEY_PREFIX}{sku}"
    data = await redis.hgetall(key)
    if not data:
        raise InventoryUnavailableError(sku, quantity, 0)
    available = int(data.get("available_quantity", "0"))
    reserved = int(data.get("reserved_quantity", "0"))
    if available < quantity:
        raise InventoryUnavailableError(sku, quantity, available)
    new_available = available - quantity
    new_reserved = reserved + quantity
    # reducing quantity of given inventory into redis
    await redis.hset(
        key,
        mapping={
            "available_quantity": str(new_available),
            "reserved_quantity": str(new_reserved),
        },
    )
    logger.info(
        "Inventory reserved: SKU=%s qty=%s -> available=%s reserved=%s",
        sku,
        quantity,
        new_available,
        new_reserved,
    )


async def release_inventory(redis: Redis, sku: str, quantity: int) -> None:
    """
    Release previously reserved inventory (rollback scenario).
    """

    sku = sku.strip().upper()
    key = f"{INVENTORY_KEY_PREFIX}{sku}"
    data = await redis.hgetall(key)
    if not data:
        logger.warning(
            "Attempted to release inventory for unknown SKU=%s", sku
        )
        return
    available = int(data.get("available_quantity", "0"))
    reserved = int(data.get("reserved_quantity", "0"))
    new_available = available + quantity
    new_reserved = max(reserved - quantity, 0)
    await redis.hset(
        key,
        mapping={
            "available_quantity": str(new_available),
            "reserved_quantity": str(new_reserved),
        },
    )
    logger.info(
        "Inventory released: SKU=%s qty=%s -> available=%s reserved=%s",
        sku,
        quantity,
        new_available,
        new_reserved,
    )


async def reserve_inventory_for_order(redis: Redis, order: OrderCreate) -> None:
    """
    Reserve inventory for all items of an order.
    - If any SKU fails -> rollback all previous reservations
    - Raises InventoryUnavailableError with details of failing SKU
    """
    reserved: List[Tuple[str, int]] = []
    try:
        # iterating each inventory item and reducing stock of respective inventory which is stored in redis
        for item in order.items:
            await reserve_inventory(redis, item.sku, item.quantity)
            reserved.append((item.sku, item.quantity))
    except InventoryUnavailableError as exc:
        # Rollback / release all already reserved items
        logger.warning(
            "Inventory reservation failed for SKU=%s, rolling back...",
            exc.sku,
        )
        for sku, qty in reserved:
            await release_inventory(redis, sku, qty)
        raise

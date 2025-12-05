import asyncio
import json
import logging
import sys
from datetime import datetime
from typing import Any, Dict
from aiokafka import AIOKafkaConsumer
from redis.asyncio import Redis
from app.core.config import settings
from app.cache.redis_client import create_redis_client, close_redis_client
from app.brokers.kafka_producer import AsyncKafkaProducer

logger = logging.getLogger(__name__)
MAX_RETRIES = 3



async def process_order_event(event: Dict[str, Any], redis: Redis) -> None:
    """
    Final processing of an order event.
    - Marks order as 'processed'
    - Adds processed_at + last_updated_at timestamps
    - (Later phases: enrichment, analytics triggers, etc.)
    """
    order_id = event.get("order_id")
    if not order_id:
        logger.warning("Received event without order_id: %s", event)
        return
    key = f"order:{order_id}"
    if not await redis.exists(key):
        logger.warning("Order not found in Redis for order_id=%s", order_id)
        return
    now_iso = datetime.utcnow().isoformat()
    await redis.hset(
        key,
        mapping={
            "status": "processed",
            "processed_at": now_iso,
            "last_updated_at": now_iso,
        },
    )
    logger.info("Order processed successfully: %s", order_id)


async def handle_main_topic_event(
    event: Dict[str, Any],
    redis: Redis,
) -> None:
    """
    Handle events from primary 'orders' topic.
    On failure, send to retry topic with attempt=1.
    """
    try:
        await process_order_event(event, redis)
    except Exception as exc:
        logger.exception("Error processing main topic event: %s", exc)
        # Add attempt count & send to retry topic
        event["attempt"] = int(event.get("attempt", 0)) + 1
        await AsyncKafkaProducer.send(settings.kafka_retry_topic, event)
        logger.info(
            "Event forwarded to retry topic: order_id=%s attempt=%s",
            event.get("order_id"),
            event["attempt"],
        )


async def handle_retry_topic_event(
    event: Dict[str, Any],
    redis: Redis,
) -> None:
    """
    Handle events from retry topic.
    - If attempt <= MAX_RETRIES -> try processing again
    - On failure or exceeded attempts -> send to DLQ
    """
    attempt = int(event.get("attempt", 1))
    if attempt > MAX_RETRIES:
        logger.warning(
            "Max retries exceeded for order_id=%s, sending to DLQ",
            event.get("order_id"),
        )
        await AsyncKafkaProducer.send(settings.kafka_dlq_topic, event)
        return
    try:
        await process_order_event(event, redis)
        logger.info(
            "Event processed successfully from retry topic: order_id=%s attempt=%s",
            event.get("order_id"),
            attempt,
        )
    except Exception as exc:
        logger.exception("Retry processing failed: %s", exc)
        # increment attempt and requeue to retry again
        event["attempt"] = attempt + 1
        await AsyncKafkaProducer.send(settings.kafka_retry_topic, event)
        logger.info(
            "Requeued to retry topic: order_id=%s attempt=%s",
            event.get("order_id"),
            event["attempt"],
        )


async def handle_dlq_topic_event(event: Dict[str, Any]) -> None:
    """
    Handle events from DLQ topic.
    For now, just log them. In a real system:
    - Persist to DB
    - Trigger alerts
    - Expose to admin UI
    """
    logger.error(
        "DLQ event received. Manual intervention required. event=%s", event
    )


# this is generalize function to consume messages from respective given topic only
# here handler is callback type of function
async def run_consumer(
    topic: str,
    group_id: str,
    handler,
) -> None:
    """
    Generic async Kafka consumer loop.
    - Connects to Kafka
    - Creates Redis client
    - Initializes async Kafka producer (for retry/DLQ)
    - Consumes messages and passes them to handler()
    """

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    logger.info("Starting consumer for topic=%s group_id=%s", topic, group_id)

    # redis connection
    redis = await create_redis_client()

    # producer connection
    await AsyncKafkaProducer.init()

    # consumer connection
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await consumer.start()

    try:
        # infinite loop consumer reading msg
        async for msg in consumer:
            try:
                event = json.loads(msg.value.decode("utf-8"))
                logger.info("Received message from %s: %s", topic, event)
                await handler(event, redis)
                # here consumer commiting upto last offset number messages.
                await consumer.commit()
            except Exception as exc:
                logger.exception("Error handling message: %s", exc)
                # Don't commit offset so we can try again
    finally:
        await consumer.stop()
        await AsyncKafkaProducer.close()
        await close_redis_client()
        logger.info("Consumer stopped for topic=%s group_id=%s", topic, group_id)




async def run_main_worker() -> None:
    await run_consumer(
        topic=settings.kafka_orders_topic,
        group_id="order-processing-main",
        handler=handle_main_topic_event,
    )


async def run_retry_worker() -> None:
    await run_consumer(
        topic=settings.kafka_retry_topic,
        group_id="order-processing-retry",
        handler=handle_retry_topic_event,
    )


async def run_dlq_worker() -> None:
    await run_consumer(
        topic=settings.kafka_dlq_topic,
        group_id="order-processing-dlq",
        handler=lambda event, _redis: handle_dlq_topic_event(event),
    )



# demo and testing purpose
def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python -m app.workers.order_processing_worker [main|retry|dlq]")
        sys.exit(1)
    mode = sys.argv[1].strip().lower()
    if mode == "main":
        asyncio.run(run_main_worker())
    elif mode == "retry":
        asyncio.run(run_retry_worker())
    elif mode == "dlq":
        asyncio.run(run_dlq_worker())
    else:
        print("Unknown mode. Use one of: main, retry, dlq")
        sys.exit(1)


if __name__ == "__main__":
    main()

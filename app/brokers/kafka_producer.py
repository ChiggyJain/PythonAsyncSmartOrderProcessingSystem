import json
import logging
from aiokafka import AIOKafkaProducer
from app.core.config import settings

logger = logging.getLogger(__name__)


# defined own class to manage singletion-producer connection in overall application
class AsyncKafkaProducer:
    # class-variable
    _producer: AIOKafkaProducer | None = None

    # defined class-method. This is not instances method
    @classmethod
    async def init(cls) -> None:
        if cls._producer is None:
            # producer connection details and making to start
            cls._producer = AIOKafkaProducer(
                bootstrap_servers=settings.kafka_bootstrap_servers,
                client_id="order-service",
                acks="all",
                enable_idempotence=True,
            )
            await cls._producer.start()
            logger.info("Async Kafka producer started.")

    # defined class-method. This is not instances method
    # send the msg to respective topic via producer (broker-server)
    @classmethod
    async def send(cls, topic: str, message: dict) -> None:
        if cls._producer is None:
            raise RuntimeError("Kafka Producer is not initialized")
        payload = json.dumps(message).encode("utf-8")
        try:
            await cls._producer.send_and_wait(topic, payload)
            logger.info("Message sent → topic=%s", topic)
        except Exception as e:
            logger.error("Kafka send error: %s", e)
            raise

    # defined class-method. This is not instances method
    # closing the open producer connection        
    @classmethod
    async def close(cls) -> None:
        if cls._producer:
            await cls._producer.stop()
            logger.info("Async Kafka producer stopped.")

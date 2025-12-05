from pydantic_settings import BaseSettings

# this class is inherit from inbuilt python class: BaseSettings
class Settings(BaseSettings):
    app_name: str = "Python Async Smart Order Processing System"
    environment: str = "local"  # local | dev | prod
    redis_url: str = "redis://localhost:6379/0"
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_orders_topic: str = "orders"
    kafka_retry_topic: str = "orders_retry"
    kafka_dlq_topic: str = "orders_dlq"
    class Config:
        # loading environment file it is available
        # if any key is found in env file then key becomes in uppercase and will match with respective key of class:Settings
        env_file = ".env"
        env_file_encoding = "utf-8"

# getting settings class instances-object
settings = Settings()

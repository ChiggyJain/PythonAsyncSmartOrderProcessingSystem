import logging
import sys
from .config import settings


def configure_logging() -> None:
    """
    Configure application-wide logging.
    - Logs to stdout (Docker/k8s friendly)
    - Includes basic context like environment and app_name
    """
    log_level = logging.INFO
    logging.basicConfig(
        level=log_level,
        format=(
            "%(asctime)s | %(levelname)s | %(name)s | "
            f"{settings.environment} | {settings.app_name} | "
            "%(message)s"
        ),
        # sends logs to a stream and now messages will be print on respective machine terminal only
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    # uvicorn will be showing only [info] logs
    logging.getLogger("uvicorn.access").setLevel(logging.INFO)
    # kafka will be showing only warning level logs
    logging.getLogger("aiokafka").setLevel(logging.WARNING)
    # redis will be showing only warning level logs only
    logging.getLogger("redis").setLevel(logging.WARNING)

import logging
import os
import sys
from typing import Optional

LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
DEFAULT_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()


def configure_logging(level: Optional[str] = None) -> logging.Logger:
    """
    Ensure the root logger has handlers and the desired level so INFO logs surface everywhere.
    Uvicorn configures handlers before importing our code, Celery/scripts usually do not.
    """
    resolved_level = (level or DEFAULT_LEVEL).upper()
    root_logger = logging.getLogger()

    if not root_logger.handlers:
        logging.basicConfig(
            level=resolved_level,
            format=LOG_FORMAT,
            handlers=[logging.StreamHandler(sys.stdout)],
        )
    else:
        root_logger.setLevel(resolved_level)

    logging.captureWarnings(True)
    return logging.getLogger("yarra")


logger = configure_logging()

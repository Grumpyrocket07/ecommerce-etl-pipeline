"""
Structured logging shared by every ETL stage.

Every module gets a logger via get_logger(__name__) and logs to both the
console and a rotating file under LOG_DIR, so a pipeline run leaves a
permanent, inspectable trail (start/end of each stage, row counts, failures)
independent of whatever's shown in the GitHub Actions run log.
"""
import logging
import logging.handlers
import sys
from etl.config import LOG_DIR, LOG_LEVEL

LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "pipeline.log"

_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        # Already configured (e.g. re-imported) — don't duplicate handlers.
        return logger

    logger.setLevel(LOG_LEVEL)
    formatter = logging.Formatter(_FORMAT)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    file_handler = logging.handlers.RotatingFileHandler(
        LOG_FILE, maxBytes=5_000_000, backupCount=3
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger

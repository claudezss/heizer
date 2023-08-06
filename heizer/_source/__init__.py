import logging
import sys

from heizer.env_vars import HEIZER_LOG_LEVEL

FORMAT = "heizer %(asctime)s %(levelname)-8s %(message)s"


def get_logger(name: str) -> logging.Logger:
    """Get a logger with the given name."""
    formatter = logging.Formatter(fmt=FORMAT, datefmt="%Y-%m-%d %H:%M:%S")
    handler = logging.FileHandler("/tmp/heizer_log.txt", mode="w")
    handler.setFormatter(formatter)
    screen_handler = logging.StreamHandler(stream=sys.stdout)
    screen_handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(HEIZER_LOG_LEVEL)
    logger.addHandler(handler)
    logger.addHandler(screen_handler)
    return logger

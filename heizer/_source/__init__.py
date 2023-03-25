import logging
import os
import sys

FORMAT = "%(asctime)s %(levelname)-8s %(message)s"


def get_logger(name: str) -> logging.Logger:
    formatter = logging.Formatter(fmt=FORMAT, datefmt="%Y-%m-%d %H:%M:%S")
    handler = logging.FileHandler("/tmp/heizer_log.txt", mode="w")
    handler.setFormatter(formatter)
    screen_handler = logging.StreamHandler(stream=sys.stdout)
    screen_handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(os.environ.get("HEIZER_LOG_LEVEL", "DEBUG"))
    logger.addHandler(handler)
    logger.addHandler(screen_handler)
    return logger

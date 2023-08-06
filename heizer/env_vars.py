import os

CONSUMER_STATUS_FILE_PATH = os.environ.get("HEIZER_CONSUMER_STATUS_FILE_PATH", "/tmp/heizer_consumers_status.json")

HEIZER_LOG_LEVEL = os.environ.get("HEIZER_LOG_LEVEL", "INFO")

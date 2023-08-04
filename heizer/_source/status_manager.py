import json
import os
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, Optional

from heizer._source.enums import ConsumerStatusEnum
from heizer.env_vars import CONSUMER_STATUS_FILE_PATH


def write_consumer_status(
    consumer_id: str,
    status: ConsumerStatusEnum,
    pid: int,
    consumer_name: Optional[str] = None,
) -> None:
    if os.path.exists(CONSUMER_STATUS_FILE_PATH):
        data = json.loads(open(CONSUMER_STATUS_FILE_PATH).read())
        data.update(
            {
                consumer_id: {
                    "name": consumer_name or consumer_id,
                    "status": status,
                    "pid": pid,
                    "timestamp": datetime.utcnow().isoformat(),
                }
            }
        )
    else:
        data = defaultdict(Dict[str, Any])
        data[consumer_id] = {
            "name": consumer_name or consumer_id,
            "status": status,
            "pid": pid,
            "timestamp": datetime.utcnow().isoformat(),
        }

    with open(CONSUMER_STATUS_FILE_PATH, "+w") as f:
        json.dump(data, f)


def read_consumer_status(consumer_id: Optional[str] = None) -> Dict[str, Any]:
    with open(CONSUMER_STATUS_FILE_PATH) as f:
        data = json.loads(f.read())
        if consumer_id:
            return data.get(consumer_id, {})
        else:
            return data

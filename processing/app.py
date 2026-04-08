import json
import logging
import logging.config
import os
from datetime import datetime, timezone
from typing import Any, Dict, Tuple

import connexion
import requests
import yaml
from apscheduler.schedulers.background import BackgroundScheduler
from connexion.middleware import MiddlewarePosition
from starlette.middleware.cors import CORSMiddleware


# load config and logging
APP_CONFIG_FILE = os.environ.get("APP_CONFIG_FILE", "/config/processing_config.yml")
LOG_CONFIG_FILE = os.environ.get("LOG_CONF_FILE", "/config/processing_log_config.yml")

# stats filename, scheduler interval, and storage URLs
with open(APP_CONFIG_FILE, "r") as f:
    app_conf = yaml.safe_load(f)

with open(LOG_CONFIG_FILE, "r") as f:
    log_conf = yaml.safe_load(f)

logging.config.dictConfig(log_conf)
logger = logging.getLogger("basicLogger")

# read files from config
STATS_FILE = app_conf["datastore"]["filename"]
PERIOD_SEC = int(app_conf["scheduler"]["period_sec"])

ITEMS_URL = app_conf["eventstores"]["checklist_items"]["url"]
SUMMARIES_URL = app_conf["eventstores"]["checklist_summaries"]["url"]


# helpers

def _utc_now() -> datetime:
    return datetime.now(timezone.utc)

def _to_rfc3339_z(dt: datetime) -> str:
    """
    convert datetime -> timestamp string
    format used in GET requests to storage
    """
    dt = dt.astimezone(timezone.utc).replace(microsecond=0)
    return dt.isoformat().replace("+00:00", "Z")

def _parse_rfc3339(ts: str) -> datetime:
    """
    convert timestamp string into datetime
    """
    if ts is None or str(ts).strip() == "":
        raise ValueError("timestamp is required")

    s = ts.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"

    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)

def _default_stats() -> Dict[str, Any]:
    return {
        "num_checklist_item_events": 0,
        "num_checklist_summary_events": 0,
        "max_estimated_mins": None,
        "max_items_completed": None,
        "last_updated": None
    }

def _read_stats_file() -> Tuple[Dict[str, Any], bool]:
    """
    read stats from data.json
    returns (stats_dict, exists)
    """
    if not os.path.exists(STATS_FILE):
        return _default_stats(), False

    with open(STATS_FILE, "r") as f:
        return json.load(f), True

def _write_stats_file(stats: Dict[str, Any]) -> None:
    with open(STATS_FILE, "w") as f:
        json.dump(stats, f, indent=2)


# periodic processing
def populate_stats():
    """
    scheduler runs every few seconds
    - figures out the time window to query
    - calls storage GET endpoints for events in window
    - updates the stats in data.json
    """
    logger.info("Periodic processing started")

    stats, _exists = _read_stats_file()

    # determine time window
    if stats.get("last_updated"):
        start_dt = _parse_rfc3339(stats["last_updated"])
    else:
        # pull everything the first time
        start_dt = datetime(1970, 1, 1, tzinfo=timezone.utc)

    end_dt = _utc_now()

    start_ts = _to_rfc3339_z(start_dt)
    end_ts = _to_rfc3339_z(end_dt)

    params = {"start_timestamp": start_ts, "end_timestamp": end_ts}

    # pull checklist item events from storage
    try:
        r_items = requests.get(ITEMS_URL, params=params, timeout=5)
        if r_items.status_code != 200:
            logger.error("Checklist items request failed. Status code: %d", r_items.status_code)
            logger.info("Periodic processing ended")
            return
        items = r_items.json()
    except Exception as e:
        logger.error("Checklist items request error: %s", e)
        logger.info("Periodic processing ended")
        return

    # pull checklist summary events from storage
    try:
        r_summ = requests.get(SUMMARIES_URL, params=params, timeout=5)
        if r_summ.status_code != 200:
            logger.error("Checklist summaries request failed. Status code: %d", r_summ.status_code)
            logger.info("Periodic processing ended")
            return
        summaries = r_summ.json()
    except Exception as e:
        logger.error("Checklist summaries request error: %s", e)
        logger.info("Periodic processing ended")
        return

    logger.info("Received %d checklist item events and %d checklist summary events", len(items), len(summaries))

    # update total counts
    stats["num_checklist_item_events"] = int(stats.get("num_checklist_item_events", 0)) + len(items)
    stats["num_checklist_summary_events"] = int(stats.get("num_checklist_summary_events", 0)) + len(summaries)

    # update max_estimated_mins from checklist item events
    new_estimated = [
        e.get("estimated_mins")
        for e in items
        if isinstance(e.get("estimated_mins"), int)
    ]
    if new_estimated:
        current_max = stats.get("max_estimated_mins")
        candidate = max(new_estimated)
        stats["max_estimated_mins"] = candidate if current_max is None else max(int(current_max), candidate)

    # update max_items_completed from summary events
    new_completed = [
        e.get("items_completed")
        for e in summaries
        if isinstance(e.get("items_completed"), int)
    ]
    if new_completed:
        current_max = stats.get("max_items_completed")
        candidate = max(new_completed)
        stats["max_items_completed"] = candidate if current_max is None else max(int(current_max), candidate)

    stats["last_updated"] = end_ts

    _write_stats_file(stats)
    logger.debug("Updated stats: %s", stats)

    logger.info("Periodic processing ended")


def init_scheduler():
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(populate_stats, "interval", seconds=PERIOD_SEC)
    sched.start()
    return sched


# Assignment 1: new health endpoint
def health():
    """
    GET /health
    Returns 200 if processing service is running.
    """
    return {"status": "Processing is healthy"}, 200
    
def get_stats():
    """
    GET /stats
    reads the data.json file and returns stats
    """
    logger.info("Request received: GET /stats")

    stats, exists = _read_stats_file()
    if not exists:
        logger.error("Statistics do not exist")
        logger.info("Request completed: GET /stats (404)")
        return {"message": "Statistics do not exist"}, 404

    logger.debug("Stats payload: %s", stats)
    logger.info("Request completed: GET /stats (200)")
    return stats, 200


# connexion app
app = connexion.FlaskApp(__name__, specification_dir=".")

app.add_middleware(
    CORSMiddleware,
    position=MiddlewarePosition.BEFORE_EXCEPTION,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_api("openapi.yml", strict_validation=True, validate_responses=True)

if __name__ == "__main__":
    init_scheduler()
    app.run(port=8100, host="0.0.0.0")
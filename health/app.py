import logging
import logging.config
import os
import sqlite3
from datetime import datetime, timezone

import connexion
import requests
import yaml
from apscheduler.schedulers.background import BackgroundScheduler
from connexion.middleware import MiddlewarePosition
from starlette.middleware.cors import CORSMiddleware


APP_CONFIG_FILE = os.environ.get("APP_CONFIG_FILE", "/config/health_config.yml")
LOG_CONFIG_FILE = os.environ.get("LOG_CONF_FILE", "/config/health_log_config.yml")

with open(APP_CONFIG_FILE, "r") as f:
    app_config = yaml.safe_load(f)

with open(LOG_CONFIG_FILE, "r") as f:
    log_config = yaml.safe_load(f)

logging.config.dictConfig(log_config)
logger = logging.getLogger("basicLogger")

DB_FILE = app_config["datastore"]["filename"]
POLL_INTERVAL = int(app_config["scheduler"]["period_sec"])
REQUEST_TIMEOUT = int(app_config["scheduler"]["timeout_sec"])

RECEIVER_URL = app_config["services"]["receiver"]["url"]
STORAGE_URL = app_config["services"]["storage"]["url"]
PROCESSING_URL = app_config["services"]["processing"]["url"]
ANALYZER_URL = app_config["services"]["analyzer"]["url"]


def utc_now_string():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def get_db_connection():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_default_rows():
    conn = get_db_connection()
    cur = conn.cursor()

    service_names = ["receiver", "storage", "processing", "analyzer"]

    for name in service_names:
        cur.execute(
            """
            INSERT OR IGNORE INTO service_status (service_name, status, last_update)
            VALUES (?, ?, ?)
            """,
            (name, "Unknown", utc_now_string())
        )

    conn.commit()
    conn.close()


def update_service_status(service_name, status):
    timestamp = utc_now_string()

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE service_status
        SET status = ?, last_update = ?
        WHERE service_name = ?
        """,
        (status, timestamp, service_name)
    )
    conn.commit()
    conn.close()

    logger.info("Recorded %s service as %s at %s", service_name, status, timestamp)


def check_one_service(service_name, url):
    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT)
        if response.status_code == 200:
            update_service_status(service_name, "Up")
        else:
            update_service_status(service_name, "Down")
    except Exception:
        update_service_status(service_name, "Down")


def poll_health():
    logger.info("Running scheduled health check poll")

    check_one_service("receiver", RECEIVER_URL)
    check_one_service("storage", STORAGE_URL)
    check_one_service("processing", PROCESSING_URL)
    check_one_service("analyzer", ANALYZER_URL)


def init_scheduler():
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(poll_health, "interval", seconds=POLL_INTERVAL)
    sched.start()
    return sched


def get_service_statuses():
    """
    GET /checks
    Return current health status of all backend services.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT service_name, status, last_update FROM service_status")
    rows = cur.fetchall()
    conn.close()

    result = {
        "receiver": "Unknown",
        "storage": "Unknown",
        "processing": "Unknown",
        "analyzer": "Unknown",
        "last_update": None
    }

    latest_timestamp = None

    for row in rows:
        result[row["service_name"]] = row["status"]

        if latest_timestamp is None or row["last_update"] > latest_timestamp:
            latest_timestamp = row["last_update"]

    result["last_update"] = latest_timestamp

    logger.info("Health status of all services retrieved through API")
    return result, 200


def health():
    """
    GET /health
    Health endpoint for the health service itself.
    """
    return {"status": "Health Check service is healthy"}, 200


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
    ensure_default_rows()
    poll_health()
    init_scheduler()
    app.run(port=8120, host="0.0.0.0")
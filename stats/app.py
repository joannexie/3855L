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


APP_CONFIG_FILE = os.environ.get("APP_CONFIG_FILE", "/config/stats_config.yml")
LOG_CONFIG_FILE = os.environ.get("LOG_CONF_FILE", "/config/stats_log_config.yml")

with open(APP_CONFIG_FILE, "r") as f:
    app_config = yaml.safe_load(f)

with open(LOG_CONFIG_FILE, "r") as f:
    log_config = yaml.safe_load(f)

logging.config.dictConfig(log_config)
logger = logging.getLogger("basicLogger")

DB_FILE = app_config["datastore"]["filename"]
POLL_INTERVAL = int(app_config["scheduler"]["period_sec"])


RECEIVER_URL = app_config["services"]["receiver"]["url"]
STORAGE_URL = app_config["services"]["storage"]["url"]
PROCESSING_URL = app_config["services"]["processing"]["url"]
ANALYZER_URL = app_config["services"]["analyzer"]["url"]
TIMEOUT = int(app_config["scheduler"]["timeout_sec"])

def update_stats():
    num_available = 0

    receiver_stats = "Unavailable"
    try:
        response = httpx.get(RECEIVER_URL, timeout=TIMEOUT)
        if response.status_code == 200:
            receiver_json = response.json()
            receiver_status = f"Receiver is healthy at {receiver_json['status_datetime']}"
            logger.info("receiver is healthy")
            num_available += 1
        else:
            logger.info("reciever returning non-200 response")
    except (TimeoutException):
        logger.info("receiver is not available")

    storage_stats = "Unavailable"
    try:
        response = httpx.get(STORAGE_URL, timeout=TIMEOUT)
        if response.status_code == 200:
            storage_json = response.json()
            storage_status = f"Storage has {storage_json['num_temp']} temperature events and {storage_json['num_precip']} precipitation events"
            logger.info("storage is healthy")
            num_available += 1
        else:
            logger.info("storage returning non-200 response")
    except (TimeoutException):
        logger.info("storage is not available")

    processing_stats = "Unavailable"
    try:
        response = httpx.get(PROCESSING_URL, timeout=TIMEOUT)
        if response.status_code == 200:
            processing_json = response.json()
            processing_status = f"Processing is healthy at {processing_json['status_datetime']}"
            logger.info("processing is healthy")
            num_available += 1
        else:
            logger.info("processing returning non-200 response")
    except (TimeoutException):
        logger.info("processing is not available")
    
    analyzer_stats = "Unavailable"
    try:
        response = httpx.get(ANALYZER_URL, timeout=TIMEOUT)
        if response.status_code == 200:
            analyzer_json = response.json()
            analyzer_status = f"Analyzer is healthy at {analyzer_json['status_datetime']}"
            logger.info("analyzer is healthy")
            num_available += 1
        else:
            logger.info("analyzer returning non-200 response")
    except (TimeoutException):
        logger.info("analyzer is not available")    
    
def get_stats():
    return {
        "receiver": receiver_stats,
        "storage": storage_stats,
        "processing": processing_stats,
        "analyzer": analyzer_stats
    }, 200






app = connexion.FlaskApp(__name__, specification_dir=".")

if "CORS_ALLOW_ALL" in os.environ and os.environ["CORS_ALLOW_ALL"] == "yes":
    app.add_middleware(
        CORSMiddleware,
        position=MiddlewarePosition.BEFORE_EXCEPTION,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

app.add_api(
    "openapi.yml",
    base_path="/stats",  
    strict_validation=True,
    validate_responses=True
)

if __name__ == "__main__":
    ensure_default_rows()
    init_scheduler()
    app.run(port=8130, host="0.0.0.0")
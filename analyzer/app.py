import os
import json
import logging
import logging.config
from datetime import datetime, timezone

import yaml
import connexion
from kafka import KafkaConsumer
from connexion.middleware import MiddlewarePosition
from starlette.middleware.cors import CORSMiddleware


# Load config
APP_CONF_FILE = os.environ.get("APP_CONF_FILE", "/config/analyzer_config.yml")
LOG_CONF_FILE = os.environ.get("LOG_CONF_FILE", "/config/analyzer_log_config.yaml")

with open(APP_CONF_FILE, "r", encoding="utf-8") as f:
    app_config = yaml.safe_load(f)

with open(LOG_CONF_FILE, "r", encoding="utf-8") as f:
    log_config = yaml.safe_load(f)
    logging.config.dictConfig(log_config)

logger = logging.getLogger("basicLogger")

EVENTS_HOSTNAME = app_config["events"]["hostname"]
EVENTS_PORT = app_config["events"]["port"]
KAFKA_TOPIC = app_config["events"]["topic"]

SERVICE_PORT = int(app_config.get("service", {}).get("port", 8110))

# Event type strings produced by receiver
EVENT1_TYPE = app_config.get("event_types", {}).get("checklist_items", "checklist_item")
EVENT2_TYPE = app_config.get("event_types", {}).get("checklist_summaries", "checklist_summary")


def _get_consumer():
    """
    Create a consumer that reads from the beginning and stops when it times out.
    """
    bootstrap = f"{EVENTS_HOSTNAME}:{EVENTS_PORT}"
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[bootstrap],
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=1000,
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )
    return consumer


def _parse_index(index):
    if index is None:
        return None, ({"message": "Missing required query parameter: index"}, 400)

    try:
        idx = int(index)
    except (ValueError, TypeError):
        return None, ({"message": "index must be an integer"}, 400)

    if idx < 0:
        return None, ({"message": "index must be >= 0"}, 400)

    return idx, None


def _find_event_by_type_index(wanted_type: str, wanted_index: int):
    """
    Kafka topic contains mixed event types.
    Count only events of wanted_type and return the one at wanted_index.
    """
    consumer = _get_consumer()
    count = 0

    try:
        for msg in consumer:
            event = msg.value
            if event.get("type") != wanted_type:
                continue

            if count == wanted_index:
                return event, 200

            count += 1

    finally:
        consumer.close()

    return {"message": f"No event of type '{wanted_type}' at index {wanted_index}"}, 404


def health():
    """
    GET /health
    Returns 200 if analyzer service is running.
    """
    return {"status": "Analyzer is healthy"}, 200



def get_event1(index=None):
    """
    GET /todo/checklist-items?index=N
    """
    idx, err = _parse_index(index)
    if err:
        return err

    return _find_event_by_type_index(EVENT1_TYPE, idx)


def get_event2(index=None):
    """
    GET /todo/checklist-summaries?index=N
    """
    idx, err = _parse_index(index)
    if err:
        return err

    return _find_event_by_type_index(EVENT2_TYPE, idx)


def get_stats():
    """
    GET /stats
    Count how many of each event type exist in the Kafka topic.
    """
    consumer = _get_consumer()
    num_event1 = 0
    num_event2 = 0

    try:
        for msg in consumer:
            event = msg.value
            et = event.get("type")

            if et == EVENT1_TYPE:
                num_event1 += 1
            elif et == EVENT2_TYPE:
                num_event2 += 1

    finally:
        consumer.close()

    return {
        "num_event1": num_event1,
        "num_event2": num_event2,
        "last_updated": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    }, 200


def main():
    logger.info(
        f"Analyzer starting. Kafka={EVENTS_HOSTNAME}:{EVENTS_PORT} "
        f"topic={KAFKA_TOPIC} event1={EVENT1_TYPE} event2={EVENT2_TYPE}"
    )

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
        base_path="/analyzer",
        strict_validation=True,
        validate_responses=True
    )

    app.run(port=SERVICE_PORT, host="0.0.0.0")


if __name__ == "__main__":
    main()
import connexion
from connexion import NoContent
import json
from datetime import datetime
import uuid
import yaml
import logging
import logging.config

from kafka import KafkaProducer

MAX_BATCH_EVENTS = 5  # not used in Kafka version, but kept if your lab wants it later

# load app config
with open("/config/receiver_config.yml", "r") as f:
    app_config = yaml.safe_load(f.read())

# Kafka config
KAFKA_HOST = app_config["events"]["hostname"]
KAFKA_PORT = app_config["events"]["port"]
KAFKA_TOPIC = app_config["events"]["topic"]

# load logging config
with open("/config/receiver_log_config.yml", "r") as f:
    log_config = yaml.safe_load(f.read())
logging.config.dictConfig(log_config)
logger = logging.getLogger("basicLogger")

# Kafka Producer (kafka-python)
producer = KafkaProducer(
    bootstrap_servers=[f"{KAFKA_HOST}:{KAFKA_PORT}"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8") if k else None,
    acks="all",
    retries=5
)

def _produce(event_type: str, payload: dict):
    msg = {
        "type": event_type,
        "datetime": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "payload": payload
    }
    producer.send(KAFKA_TOPIC, key=event_type, value=msg)
    producer.flush()
    logger.info(
        f"Produced Kafka event type={event_type} trace_id={payload.get('trace_id')} topic={KAFKA_TOPIC}"
    )

def report_checklist_item_events(body):
    """
    receiver endpoint - batch:
    POST /todo/checklist-items

    takes ChecklistItemBatch:
    sender_id, sent_timestamp, list_name, items[]
    for each item in items[], PRODUCE a single ChecklistItemEvent to Kafka
    """
    # keep your existing behavior: 1 trace_id per batch
    trace_id = str(uuid.uuid4())
    logger.info(f"Received checklist item batch with a trace id of {trace_id}")

    sender_id = body["sender_id"]
    sent_timestamp = body["sent_timestamp"]
    list_name = body["list_name"]
    items = body.get("items", [])

    for item in items:
        payload = {
            "trace_id": trace_id,
            "sender_id": sender_id,
            "sent_timestamp": sent_timestamp,
            "list_name": list_name,
            "item_name": item["item_name"],
            "completed": item["completed"],
            "completed_at": item["completed_at"],
            "estimated_mins": item["estimated_mins"],
        }
        _produce("checklist_item", payload)

    return NoContent, 201

def report_checklist_summary_events(body):
    """
    receiver endpoint - batch:
    POST /todo/checklist-summaries

    takes ChecklistSummaryBatch:
    sender_id, sent_timestamp, list_name, summaries[]
    for each summary in summaries[], PRODUCE a single ChecklistSummaryEvent to Kafka
    """
    # keep your existing behavior: 1 trace_id per batch
    trace_id = str(uuid.uuid4())
    logger.info(f"Received checklist summary batch with a trace id of {trace_id}")

    sender_id = body["sender_id"]
    sent_timestamp = body["sent_timestamp"]
    list_name = body["list_name"]
    summaries = body.get("summaries", [])

    for summary in summaries:
        payload = {
            "trace_id": trace_id,
            "sender_id": sender_id,
            "sent_timestamp": sent_timestamp,
            "list_name": list_name,
            "items_completed": summary["items_completed"],
            "items_total": summary["items_total"],
            "summary_at": summary["summary_at"],
        }
        _produce("checklist_summary", payload)

    return NoContent, 201

app = connexion.FlaskApp(__name__, specification_dir="")

app.add_api(
    "openapi.yml",
    strict_validation=True,
    validate_responses=True
)

if __name__ == "__main__":
    app.run(port=8080, host="0.0.0.0")

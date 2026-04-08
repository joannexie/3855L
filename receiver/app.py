import connexion
from connexion import NoContent
import json
from datetime import datetime
import uuid
import yaml
import logging
import logging.config
import time
from threading import Lock

from kafka import KafkaProducer
from kafka.errors import KafkaError


# load app config
with open("/config/receiver_config.yml", "r") as f:
    app_config = yaml.safe_load(f.read())

# Kafka config
KAFKA_HOST = app_config["events"]["hostname"]
KAFKA_PORT = app_config["events"]["port"]
KAFKA_TOPIC = app_config["events"]["topic"]

# logging config
with open("/config/receiver_log_config.yml", "r") as f:
    log_config = yaml.safe_load(f.read())
logging.config.dictConfig(log_config)
logger = logging.getLogger("basicLogger")


# L11: added wrapper class so receiver reuses ONE producer
# instead of reconnecting to Kafka repeatedly, and so it can retry if Kafka fails
class KafkaProducerWrapper:
    def __init__(self, host: str, port: int, topic: str):
        self.bootstrap_servers = [f"{host}:{port}"]
        self.topic = topic
        self.producer = None
        self.lock = Lock()   # L11: added lock for thread-safe producer use

    # L11: create producer only when needed
    def _connect(self):
        if self.producer is not None:
            return

        logger.info("Creating Kafka producer")
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",

            # L11: retry/batching settings to improve performance
            retries=5,
            linger_ms=5,
            batch_size=16384
        )
        logger.info("Kafka producer created successfully")

    # L11: reset producer if send fails
    def _reset(self):
        try:
            if self.producer is not None:
                self.producer.close()
        except Exception:
            pass
        self.producer = None

    # L11: send whole batch and retry several times if Kafka is temporarily down
    def send_batch(self, messages, max_attempts: int = 10):
        last_error = None

        for attempt in range(1, max_attempts + 1):
            try:
                with self.lock:
                    if self.producer is None:
                        self._connect()

                    futures = []
                    for msg in messages:
                        futures.append(
                            self.producer.send(
                                self.topic,
                                key=msg["type"],
                                value=msg
                            )
                        )

                    # L11: wait for all sends to finish
                    for future in futures:
                        future.get(timeout=10)

                    # L11: flush once per batch instead of once per event
                    self.producer.flush(timeout=10)

                logger.info("Produced %d Kafka messages to topic=%s", len(messages), self.topic)
                return

            except KafkaError as e:
                last_error = e
                logger.warning(
                    "Kafka send failed on attempt %d/%d: %s",
                    attempt,
                    max_attempts,
                    e
                )
                self._reset()   # L11: reconnect on failure
                time.sleep(1)
            except Exception as e:
                last_error = e
                logger.warning(
                    "Unexpected Kafka send error on attempt %d/%d: %s",
                    attempt,
                    max_attempts,
                    e
                )
                self._reset()   # L11: reconnect on failure
                time.sleep(1)

        raise RuntimeError(f"Unable to send Kafka messages after {max_attempts} attempts: {last_error}")


# L11: one global producer wrapper reused by all requests
producer_wrapper = KafkaProducerWrapper(KAFKA_HOST, KAFKA_PORT, KAFKA_TOPIC)


# helper
def _build_message(event_type: str, payload: dict):
    return {
        "type": event_type,
        "datetime": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "payload": payload
    }


def report_checklist_item_events(body):
    """
    POST /todo/checklist-items
    Takes one batch and sends one Kafka event per item.
    """
    trace_id = str(uuid.uuid4())
    logger.info("Received checklist item batch with trace id %s", trace_id)

    sender_id = body["sender_id"]
    sent_timestamp = body["sent_timestamp"]
    list_name = body["list_name"]
    items = body.get("items", [])

    messages = []
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
        messages.append(_build_message("checklist_item", payload))

    try:
        if messages:
            # L11: use wrapper instead of direct producer.send/flush each time
            producer_wrapper.send_batch(messages)
        return NoContent, 201
    except Exception as e:
        logger.exception("Failed to publish checklist item batch: %s", e)

        # L11: return 503 if Kafka is unavailable
        return {"message": "Kafka is unavailable, please try again"}, 503


def report_checklist_summary_events(body):
    """
    POST /todo/checklist-summaries
    Takes one batch and sends one Kafka event per summary.
    """
    trace_id = str(uuid.uuid4())
    logger.info("Received checklist summary batch with trace id %s", trace_id)

    sender_id = body["sender_id"]
    sent_timestamp = body["sent_timestamp"]
    list_name = body["list_name"]
    summaries = body.get("summaries", [])

    messages = []
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
        messages.append(_build_message("checklist_summary", payload))

    try:
        if messages:
            # L11: use wrapper instead of direct producer.send/flush each time
            producer_wrapper.send_batch(messages)
        return NoContent, 201
    except Exception as e:
        logger.exception("Failed to publish checklist summary batch: %s", e)

        # L11: return 503 if Kafka is unavailable
        return {"message": "Kafka is unavailable, please try again"}, 503

app = connexion.FlaskApp(__name__, specification_dir="")

app.add_api(
    "openapi.yml",
    strict_validation=True,
    validate_responses=True
)

if __name__ == "__main__":
    app.run(port=8080, host="0.0.0.0")

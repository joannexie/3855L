import functools
from datetime import datetime, timezone
import yaml
import logging
import logging.config
from threading import Thread
import json
import time

import connexion
from sqlalchemy.exc import SQLAlchemyError
from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable

from db import make_session, ENGINE
from models import Base, ChecklistItemEvent, ChecklistSummaryEvent


# logging setup
with open("/config/storage_log_config.yml", "r") as f:
    log_config = yaml.safe_load(f.read())
logging.config.dictConfig(log_config)
logger = logging.getLogger("basicLogger")

# load Kafka config
with open("/config/storage_config.yaml", "r") as f:
    app_config = yaml.safe_load(f.read())
KAFKA_HOST = app_config["events"]["hostname"]
KAFKA_PORT = app_config["events"]["port"]
KAFKA_TOPIC = app_config["events"]["topic"]


# create tables
Base.metadata.create_all(ENGINE)


# L11: improved DB session wrapper so failed writes rollback properly
def use_db_session(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        session = make_session()
        try:
            result = func(session, *args, **kwargs)
            return result
        except Exception:
            session.rollback()   # L11: rollback on failure
            raise
        finally:
            session.close()
    return wrapper

def _parse_dt(dt_str: str) -> datetime:
    if not dt_str or not isinstance(dt_str, str):
        raise ValueError("timestamp must be a non-empty string")

    s = dt_str.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"

    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)

def _dt_to_z(dt: datetime) -> str:
    if dt is None:
        return None
    dtu = dt.astimezone(timezone.utc).replace(microsecond=0)
    return dtu.isoformat().replace("+00:00", "Z")

def _item_to_dict(e: ChecklistItemEvent) -> dict:
    return {
        "trace_id": str(e.trace_id),
        "sender_id": str(e.sender_id),
        "sent_timestamp": _dt_to_z(e.sent_timestamp),
        "list_name": e.list_name,
        "item_name": e.item_name,
        "completed": bool(e.completed),
        "completed_at": _dt_to_z(e.completed_at),
        "estimated_mins": int(e.estimated_mins),
    }

def _summary_to_dict(e: ChecklistSummaryEvent) -> dict:
    return {
        "trace_id": str(e.trace_id),
        "sender_id": str(e.sender_id),
        "sent_timestamp": _dt_to_z(e.sent_timestamp),
        "list_name": e.list_name,
        "items_completed": int(e.items_completed),
        "items_total": int(e.items_total),
        "summary_at": _dt_to_z(e.summary_at),
    }

@use_db_session
def _insert_checklist_item(session, body: dict):
    trace_id = body["trace_id"]

    event = ChecklistItemEvent(
        trace_id=trace_id,
        sender_id=body["sender_id"],
        sent_timestamp=_parse_dt(body["sent_timestamp"]),
        list_name=body["list_name"],
        item_name=body["item_name"],
        completed=bool(body["completed"]),
        completed_at=_parse_dt(body["completed_at"]),
        estimated_mins=int(body["estimated_mins"]),
    )
    session.add(event)
    session.commit()
    logger.debug("Stored checklist item event with trace id %s", trace_id)

@use_db_session
def _insert_checklist_summary(session, body: dict):
    trace_id = body["trace_id"]

    event = ChecklistSummaryEvent(
        trace_id=trace_id,
        sender_id=body["sender_id"],
        sent_timestamp=_parse_dt(body["sent_timestamp"]),
        list_name=body["list_name"],
        items_completed=int(body["items_completed"]),
        items_total=int(body["items_total"]),
        summary_at=_parse_dt(body["summary_at"]),
    )
    session.add(event)
    session.commit()
    logger.debug("Stored checklist summary event with trace id %s", trace_id)


# L11: added Kafka consumer wrapper with infinite retry logic
# storage is a background service, so infinite retry makes sense here
class KafkaConsumerWrapper:
    def __init__(self, topic: str, host: str, port: int, group_id: str):
        self.topic = topic
        self.bootstrap = f"{host}:{port}"
        self.group_id = group_id
        self.consumer = None

    # L11: keep trying until Kafka becomes available
    def connect(self):
        while True:
            try:
                logger.info("Trying to connect Kafka consumer to %s", self.bootstrap)
                self.consumer = KafkaConsumer(
                    self.topic,
                    bootstrap_servers=[self.bootstrap],
                    group_id=self.group_id,

                    # L11: disable auto commit so offsets only move after DB write succeeds
                    enable_auto_commit=False,

                    auto_offset_reset="earliest",
                    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
                )
                logger.info("Kafka consumer connected successfully")
                return
            except (KafkaError, NoBrokersAvailable, Exception) as e:
                logger.warning("Kafka consumer connection failed: %s", e)
                self.consumer = None
                time.sleep(1)

    # L11: reset consumer if broker fails
    def reset(self):
        try:
            if self.consumer is not None:
                self.consumer.close()
        except Exception:
            pass
        self.consumer = None

    # L11: generator that reconnects if consumer loop breaks
    def messages(self):
        while True:
            if self.consumer is None:
                self.connect()

            try:
                for record in self.consumer:
                    yield record
            except (KafkaError, NoBrokersAvailable, Exception) as e:
                logger.warning("Kafka consumer loop failed: %s", e)
                self.reset()
                time.sleep(1)

    # L11: commit offsets only after successful processing
    def commit(self):
        if self.consumer is not None:
            self.consumer.commit()


# L11: one global Kafka wrapper
kafka_wrapper = KafkaConsumerWrapper(
    topic=KAFKA_TOPIC,
    host=KAFKA_HOST,
    port=KAFKA_PORT,
    group_id="event_group"
)


def process_messages():
    """
    Consume Kafka messages forever and store them to MySQL.
    Commits offsets only after the DB write succeeds.
    """
    logger.info("Storage Kafka consumer thread started")

    for record in kafka_wrapper.messages():
        try:
            msg = record.value
            event_type = msg.get("type")
            payload = msg.get("payload", {})

            if event_type == "checklist_item":
                _insert_checklist_item(payload)
            elif event_type == "checklist_summary":
                _insert_checklist_summary(payload)
            else:
                logger.warning("Unknown event type: %s", event_type)

            # L11: commit offset only after successful DB insert
            kafka_wrapper.commit()

        except (KeyError, ValueError, TypeError) as e:
            logger.exception("Bad event payload: %s", e)

            # L11: commit bad message so it does not block queue forever
            kafka_wrapper.commit()

        except SQLAlchemyError as e:
            # L11: do not commit offset on DB failure
            logger.exception("Database error while storing event: %s", e)
            time.sleep(1)

        except Exception as e:
            # L11: do not commit offset on unexpected processing failure
            logger.exception("Unexpected storage processing error: %s", e)
            time.sleep(1)

def start_kafka_thread():
    t = Thread(target=process_messages, daemon=True)
    t.start()

@use_db_session
def get_checklist_item_events(session, start_timestamp, end_timestamp):
    try:
        start_dt = _parse_dt(start_timestamp)
        end_dt = _parse_dt(end_timestamp)

        if end_dt <= start_dt:
            return {"message": "end_timestamp must be greater than start_timestamp"}, 400

        results = (
            session.query(ChecklistItemEvent)
            .filter(ChecklistItemEvent.date_created >= start_dt)
            .filter(ChecklistItemEvent.date_created < end_dt)
            .order_by(ChecklistItemEvent.date_created.asc())
            .all()
        )

        payload = [_item_to_dict(e) for e in results]
        logger.debug(
            "GET checklist-items date_created in [%s, %s) -> %d events",
            start_timestamp,
            end_timestamp,
            len(payload)
        )
        return payload, 200

    except (ValueError, TypeError) as e:
        return {"message": f"Bad request: {e}"}, 400
    except SQLAlchemyError as e:
        return {"message": f"Database error: {e}"}, 500

@use_db_session
def get_checklist_summary_events(session, start_timestamp, end_timestamp):
    try:
        start_dt = _parse_dt(start_timestamp)
        end_dt = _parse_dt(end_timestamp)

        if end_dt <= start_dt:
            return {"message": "end_timestamp must be greater than start_timestamp"}, 400

        results = (
            session.query(ChecklistSummaryEvent)
            .filter(ChecklistSummaryEvent.date_created >= start_dt)
            .filter(ChecklistSummaryEvent.date_created < end_dt)
            .order_by(ChecklistSummaryEvent.date_created.asc())
            .all()
        )

        payload = [_summary_to_dict(e) for e in results]
        logger.debug(
            "GET checklist-summaries date_created in [%s, %s) -> %d events",
            start_timestamp,
            end_timestamp,
            len(payload)
        )
        return payload, 200

    except (ValueError, TypeError) as e:
        return {"message": f"Bad request: {e}"}, 400
    except SQLAlchemyError as e:
        return {"message": f"Database error: {e}"}, 500

# A1: new health endpoint
def health():
    """
    GET /health
    Returns 200 if storage service is running.
    """
    return {"status": "Storage is healthy"}, 200

app = connexion.FlaskApp(__name__, specification_dir="")

app.add_api(
    "openapi.yml",
    base_path="/storage",  # L12
    strict_validation=True,
    validate_responses=True
)

start_kafka_thread()

if __name__ == "__main__":
    app.run(port=8090, host="0.0.0.0")
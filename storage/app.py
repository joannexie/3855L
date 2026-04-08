import functools
from datetime import datetime, timezone
import yaml
import logging
import logging.config
from threading import Thread
import json

import connexion
from sqlalchemy.exc import SQLAlchemyError
from kafka import KafkaConsumer

from db import make_session, ENGINE
from models import Base, ChecklistItemEvent, ChecklistSummaryEvent


# 
# logging setup
# 
with open("/config/storage_log_config.yml", "r") as f:
    log_config = yaml.safe_load(f.read())
logging.config.dictConfig(log_config)
logger = logging.getLogger("basicLogger")


# 
# load app config (Kafka)
# 
with open("/config/storage_config.yaml", "r") as f:
    app_config = yaml.safe_load(f.read())
KAFKA_HOST = app_config["events"]["hostname"]
KAFKA_PORT = app_config["events"]["port"]
KAFKA_TOPIC = app_config["events"]["topic"]


# Ensure tables exist
Base.metadata.create_all(ENGINE)


# ----------------------------
# DB session decorator
# ----------------------------
def use_db_session(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        session = make_session()
        try:
            return func(session, *args, **kwargs)
        finally:
            session.close()
    return wrapper


# ----------------------------
# timestamp parsing helper
# ----------------------------
def _parse_dt(dt_str: str) -> datetime:
    """
    Returns a timezone-aware datetime in UTC
    """
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
    """
    convert datetime to ISO string ending with Z
    """
    if dt is None:
        return None
    dtu = dt.astimezone(timezone.utc).replace(microsecond=0)
    return dtu.isoformat().replace("+00:00", "Z")


# ----------------------------
# ORM - dict helpers
# ----------------------------
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


# ----------------------------
# DB insert helpers (used by Kafka consumer)
# ----------------------------
@use_db_session
def _insert_checklist_item(session, body: dict):
    """
    Insert one checklist item event into DB.
    body matches ChecklistItemEvent schema.
    """
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
    logger.debug(f"Stored checklist item event with trace id {trace_id}")


@use_db_session
def _insert_checklist_summary(session, body: dict):
    """
    Insert one checklist summary event into DB.
    body matches ChecklistSummaryEvent schema.
    """
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
    logger.debug(f"Stored checklist summary event with trace id {trace_id}")


# ----------------------------
# Kafka consumer thread
# ----------------------------
def process_messages():
    """
    Consume Kafka messages forever and store them to MySQL.
    Receiver produces messages in format:
      { "type": "...", "datetime": "...", "payload": { ...single event... } }
    """
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[f"{KAFKA_HOST}:{KAFKA_PORT}"],
        group_id="event_group",
        enable_auto_commit=False,      # commit only after DB write
        auto_offset_reset="earliest",    # first run only reads new messages
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=1000
    )

    logger.info(f"Kafka consumer started on topic={KAFKA_TOPIC} group_id=event_group")

    while True:
        try:
            for record in consumer:
                msg = record.value
                event_type = msg.get("type")
                payload = msg.get("payload", {})

                if event_type == "checklist_item":
                    _insert_checklist_item(payload)
                elif event_type == "checklist_summary":
                    _insert_checklist_summary(payload)
                else:
                    logger.warning(f"Unknown event type: {event_type}")

                consumer.commit()

        except (KeyError, ValueError, TypeError) as e:
            # bad payload formatting - log and keep going
            logger.exception(f"Bad event payload: {e}")
        except SQLAlchemyError as e:
            logger.exception(f"Database error while storing event: {e}")
        except Exception as e:
            logger.exception(f"Kafka consume loop error: {e}")


def start_kafka_thread():
    t = Thread(target=process_messages, daemon=True)
    t.start()


# ----------------------------
# GET endpoints (unchanged)
# ----------------------------
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
            f"GET checklist-items date_created in [{start_timestamp}, {end_timestamp}) -> {len(payload)} events"
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
            f"GET checklist-summaries date_created in [{start_timestamp}, {end_timestamp}) -> {len(payload)} events"
        )
        return payload, 200

    except (ValueError, TypeError) as e:
        return {"message": f"Bad request: {e}"}, 400
    except SQLAlchemyError as e:
        return {"message": f"Database error: {e}"}, 500


# ----------------------------
# connexion app
# ----------------------------
app = connexion.FlaskApp(__name__, specification_dir="")

app.add_api(
    "openapi.yml",
    strict_validation=True,
    validate_responses=True
)

# start Kafka consumer when the service starts
start_kafka_thread()

if __name__ == "__main__":
    app.run(port=8090, host="0.0.0.0")

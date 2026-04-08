from sqlalchemy.orm import DeclarativeBase, mapped_column
from sqlalchemy import Integer, String, DateTime, Boolean, func

class Base(DeclarativeBase):
    pass

class ChecklistItemEvent(Base):
    __tablename__ = "checklist_item_event"

    id = mapped_column(Integer, primary_key=True)

    # tracing
    trace_id = mapped_column(String(36), nullable=False)

    # batch fields 
    sender_id = mapped_column(String(36), nullable=False)
    sent_timestamp = mapped_column(DateTime, nullable=False)
    list_name = mapped_column(String(250), nullable=False)

    # item fields
    item_name = mapped_column(String(250), nullable=False)
    completed = mapped_column(Boolean, nullable=False)
    completed_at = mapped_column(DateTime, nullable=False)
    estimated_mins = mapped_column(Integer, nullable=False)

    date_created = mapped_column(DateTime, nullable=False, default=func.now())


class ChecklistSummaryEvent(Base):
    __tablename__ = "checklist_summary_event"

    id = mapped_column(Integer, primary_key=True)

    # tracing
    trace_id = mapped_column(String(36), nullable=False)

    # batch fields
    sender_id = mapped_column(String(36), nullable=False)
    sent_timestamp = mapped_column(DateTime, nullable=False)
    list_name = mapped_column(String(250), nullable=False)

    # summary fields 
    items_completed = mapped_column(Integer, nullable=False)
    items_total = mapped_column(Integer, nullable=False)
    summary_at = mapped_column(DateTime, nullable=False)

    date_created = mapped_column(DateTime, nullable=False, default=func.now())

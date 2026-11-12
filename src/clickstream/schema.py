"""Clickstream event contract and validation rules.

The same rules are expressed twice: here in plain Python for the reference engine, and as
Spark SQL expressions in ``spark_job.py``. ``tests/test_schema.py`` pins the Python side and the
Spark parity test (``tests/spark``) checks that both sides reject the same records.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

EVENT_TYPES = ("page_view", "add_to_cart", "checkout", "purchase")
CONTROL_TYPES = ("heartbeat",)  # advances the watermark, never aggregated
REQUIRED = ("event_id", "user_id", "event_type", "event_time", "sent_at")
FUNNEL = EVENT_TYPES

# Rejection reasons, in the order they are evaluated.
INVALID_JSON = "invalid_json"
MISSING_FIELD = "missing_field"
UNKNOWN_EVENT_TYPE = "unknown_event_type"
BAD_TIMESTAMP = "bad_timestamp"
NEGATIVE_PRICE = "negative_price"
FUTURE_TIMESTAMP = "future_timestamp"
REASONS = (INVALID_JSON, MISSING_FIELD, UNKNOWN_EVENT_TYPE, BAD_TIMESTAMP, NEGATIVE_PRICE, FUTURE_TIMESTAMP)

_ISO = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?(Z|[+-]\d{2}:\d{2})$")


@dataclass(frozen=True)
class Event:
    event_id: str
    user_id: str
    event_type: str
    event_time: datetime
    sent_at: datetime
    page: str | None = None
    product_id: str | None = None
    price: float | None = None
    device: str | None = None

    @property
    def revenue(self) -> float:
        return float(self.price or 0.0) if self.event_type == "purchase" else 0.0

    @property
    def is_control(self) -> bool:
        return self.event_type in CONTROL_TYPES


def parse_ts(value) -> datetime | None:
    """Parse an ISO-8601 timestamp with an explicit offset into an aware UTC datetime."""
    if not isinstance(value, str) or not _ISO.match(value):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def format_ts(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def validate(raw: str, max_future_skew: timedelta = timedelta(minutes=5)) -> tuple[Event | None, str | None]:
    """Return ``(event, None)`` for a valid record or ``(None, reason)`` for a rejected one."""
    try:
        obj = json.loads(raw)
    except (TypeError, ValueError):
        return None, INVALID_JSON
    if not isinstance(obj, dict):
        return None, INVALID_JSON
    if any(obj.get(k) in (None, "") for k in REQUIRED):
        return None, MISSING_FIELD
    if obj["event_type"] not in EVENT_TYPES + CONTROL_TYPES:
        return None, UNKNOWN_EVENT_TYPE
    event_time, sent_at = parse_ts(obj["event_time"]), parse_ts(obj["sent_at"])
    if event_time is None or sent_at is None:
        return None, BAD_TIMESTAMP
    price = obj.get("price")
    if price is not None:
        try:
            price = float(price)
        except (TypeError, ValueError):
            return None, NEGATIVE_PRICE
        if price < 0:
            return None, NEGATIVE_PRICE
    # A device clock in the future would drag the watermark forward and make the engine drop
    # every on-time event as "late", so such events are quarantined instead.
    if event_time - sent_at > max_future_skew:
        return None, FUTURE_TIMESTAMP
    return Event(
        event_id=str(obj["event_id"]), user_id=str(obj["user_id"]), event_type=obj["event_type"],
        event_time=event_time, sent_at=sent_at, page=obj.get("page"), product_id=obj.get("product_id"),
        price=price, device=obj.get("device"),
    ), None

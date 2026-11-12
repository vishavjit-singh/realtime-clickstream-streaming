import json
from datetime import timedelta

import pytest

from clickstream import schema as s


def payload(**over):
    base = {"event_id": "e1", "user_id": "u1", "event_type": "purchase", "event_time": "2026-01-05T09:00:00.000Z",
            "page": "/checkout/success", "product_id": "SH-001", "price": 1499.0, "device": "web",
            "sent_at": "2026-01-05T09:00:01.250Z"}
    base.update(over)
    return json.dumps(base)


def test_valid_event_is_parsed_to_utc():
    ev, reason = s.validate(payload())
    assert reason is None
    assert ev.event_time.isoformat() == "2026-01-05T09:00:00+00:00"
    assert ev.revenue == 1499.0


def test_offsets_are_normalised_to_utc():
    ev, _ = s.validate(payload(event_time="2026-01-05T14:30:00+05:30", sent_at="2026-01-05T09:00:02Z"))
    assert ev.event_time.isoformat() == "2026-01-05T09:00:00+00:00"


def test_revenue_only_counts_purchases():
    ev, _ = s.validate(payload(event_type="add_to_cart"))
    assert ev.revenue == 0.0


@pytest.mark.parametrize("raw, reason", [
    ('{"event_id": "e1", "user_id"', s.INVALID_JSON),
    ("[1, 2, 3]", s.INVALID_JSON),
    (payload(user_id=None), s.MISSING_FIELD),
    (payload(event_id=""), s.MISSING_FIELD),
    (payload(event_type="pageview"), s.UNKNOWN_EVENT_TYPE),
    (payload(event_time="05/01/2026 09:12"), s.BAD_TIMESTAMP),
    (payload(sent_at="yesterday"), s.BAD_TIMESTAMP),
    (payload(event_time="2026-01-05T09:00:00"), s.BAD_TIMESTAMP),  # no offset: ambiguous
    (payload(price=-10), s.NEGATIVE_PRICE),
    (payload(event_time="2026-01-05T15:00:00Z"), s.FUTURE_TIMESTAMP),
])
def test_rejections(raw, reason):
    ev, got = s.validate(raw)
    assert ev is None and got == reason


def test_rule_order_is_stable():
    # missing a field wins over a bad timestamp
    assert s.validate(payload(user_id=None, event_time="nope"))[1] == s.MISSING_FIELD


def test_future_skew_threshold_is_configurable():
    raw = payload(event_time="2026-01-05T09:04:00Z", sent_at="2026-01-05T09:00:00Z")
    assert s.validate(raw, timedelta(minutes=5))[1] is None
    assert s.validate(raw, timedelta(minutes=3))[1] == s.FUTURE_TIMESTAMP


def test_heartbeat_is_a_valid_control_event():
    ev, reason = s.validate(payload(event_type="heartbeat", price=None))
    assert reason is None and ev.is_control


def test_format_ts_round_trips():
    ev, _ = s.validate(payload())
    assert s.parse_ts(s.format_ts(ev.event_time)) == ev.event_time

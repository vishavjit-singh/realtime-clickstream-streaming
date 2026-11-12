import json
from datetime import datetime, timedelta, timezone

from clickstream.operators import new_state, process_batch, window_start

T0 = datetime(2026, 1, 5, 9, 0, tzinfo=timezone.utc)
_ids = iter(range(10**6))


def rec(minute: float, etype="page_view", user="u1", price=None, event_id=None, sent_delay_s=1.0):
    t = T0 + timedelta(minutes=minute)
    value = json.dumps({
        "event_id": event_id or f"e{next(_ids)}", "user_id": user, "event_type": etype,
        "event_time": t.isoformat(), "price": price,
        "sent_at": (t + timedelta(seconds=sent_delay_s)).isoformat(),
    })
    return {"partition": 0, "offset": 0, "value": value}


def test_window_start_floors_to_the_window():
    t = datetime(2026, 1, 5, 9, 7, 59, tzinfo=timezone.utc)
    assert window_start(t, timedelta(minutes=1)) == datetime(2026, 1, 5, 9, 7, tzinfo=timezone.utc)
    assert window_start(t, timedelta(minutes=5)) == datetime(2026, 1, 5, 9, 5, tzinfo=timezone.utc)


def test_windows_are_emitted_only_after_the_watermark_passes(cfg):
    state = new_state()
    r1 = process_batch(state, [rec(0.5), rec(0.7, "purchase", price=100.0)], cfg)
    assert r1.metrics == []  # watermark is 08:50:42, window 09:00-09:01 still open
    r2 = process_batch(state, [rec(10.9)], cfg)  # watermark → 09:00:54, window end 09:01 not passed
    assert r2.metrics == []
    r3 = process_batch(state, [rec(11.2)], cfg)  # watermark → 09:01:12
    rows = {m["event_type"]: m for m in r3.metrics}
    assert rows["page_view"]["events"] == 1 and rows["purchase"]["revenue"] == 100.0
    assert all(m["window_start"] == T0 for m in r3.metrics)


def test_late_rows_are_dropped_using_the_previous_watermark(cfg):
    state = new_state()
    process_batch(state, [rec(30)], cfg)            # watermark → 09:20
    res = process_batch(state, [rec(19), rec(21)], cfg)
    assert res.counters["late_dropped"] == 1
    assert res.counters["aggregated"] == 1
    assert len(res.events) == 2  # bronze keeps the late event for later backfill


def test_duplicates_are_dropped_across_batches(cfg):
    state = new_state()
    process_batch(state, [rec(1, event_id="same")], cfg)
    res = process_batch(state, [rec(1, event_id="same"), rec(1, event_id="same")], cfg)
    assert res.counters["duplicates"] == 2 and res.counters["aggregated"] == 0


def test_dead_letters_do_not_move_the_watermark(cfg):
    state = new_state()
    process_batch(state, [rec(5)], cfg)
    skewed = rec(5)
    obj = json.loads(skewed["value"])
    obj["event_time"] = (T0 + timedelta(hours=5)).isoformat()  # broken device clock
    skewed["value"] = json.dumps(obj)
    res = process_batch(state, [skewed], cfg)
    assert res.dead_letters[0]["reason"] == "future_timestamp"
    assert state["watermark"] == (T0 + timedelta(minutes=5) - cfg.watermark).isoformat()


def test_watermark_never_moves_backwards(cfg):
    state = new_state()
    process_batch(state, [rec(40)], cfg)
    before = state["watermark"]
    process_batch(state, [rec(35)], cfg)
    assert state["watermark"] == before


def test_session_window_merges_events_within_the_gap(cfg):
    state = new_state()
    batch = [rec(0, user="a"), rec(20, user="a"), rec(45, "purchase", user="a", price=50.0),  # one session
             rec(120, user="a")]  # gap > 30 min → second session
    emitted = process_batch(state, batch, cfg).sessions + process_batch(state, [rec(400, user="z")], cfg).sessions
    a = sorted((s for s in emitted if s["user_id"] == "a"), key=lambda s: s["session_start"])
    assert [s["events"] for s in a] == [3, 1]
    assert a[0]["session_end"] == T0 + timedelta(minutes=45) + cfg.session_gap
    assert a[0]["duration_s"] == 45 * 60
    assert a[0]["converted"] and a[0]["revenue"] == 50.0


def test_late_arriving_event_bridges_two_open_sessions(cfg):
    state = new_state()
    process_batch(state, [rec(0, user="b"), rec(35, user="b")], cfg)  # 35 min apart: two sessions
    assert len(state["sessions"]["b"]) == 2  # watermark 09:25, neither session is closed yet
    process_batch(state, [rec(27, user="b", sent_delay_s=60 * 9)], cfg)  # on time, fills the gap
    assert len(state["sessions"]["b"]) == 1
    assert state["sessions"]["b"][0]["events"] == 3


def test_heartbeat_advances_watermark_but_is_not_counted(cfg):
    state = new_state()
    process_batch(state, [rec(1)], cfg)
    res = process_batch(state, [rec(200, "heartbeat", user="__system__")], cfg)
    assert res.counters["aggregated"] == 0
    assert sum(m["events"] for m in res.metrics) == 1
    assert res.events == []  # control events never reach bronze
    assert state["windows"] == {} and state["sessions"] == {}

from clickstream.generator import generate
from clickstream.kafka_io import close_stream, replay
from clickstream.schema import validate


class FakeProducer:
    def __init__(self):
        self.sent, self.flushes = [], 0

    def produce(self, topic, key, value, partition=None):
        self.sent.append((topic, key.decode(), value.decode(), partition))

    def poll(self, _):
        return 0

    def flush(self, _=None):
        self.flushes += 1
        return 0


def test_replay_preserves_arrival_order_and_paces_by_event_time(cfg):
    records = generate(cfg.generator)[:300]
    producer, sleeps = FakeProducer(), []
    n = replay(records, producer, "clickstream", speedup=1e9, sleep=sleeps.append)
    assert n == 300 and [s[2] for s in producer.sent] == [r.value for r in records]
    assert all(s[1] == r.key for s, r in zip(producer.sent, records))  # keyed by user → per-user ordering
    assert producer.flushes == 1


def test_close_stream_sends_two_heartbeat_rounds_to_every_partition(cfg):
    producer, sleeps = FakeProducer(), []
    last = generate(cfg.generator)[-1].arrival
    close_stream(producer, "clickstream", last, partitions=3, pause_s=7, sleep=sleeps.append)
    assert sleeps == [7]
    assert sorted(p for *_, p in producer.sent) == [0, 0, 1, 1, 2, 2]
    times = sorted(validate(v)[0].event_time for _, _, v, _ in producer.sent)
    assert times[0] > last and times[-1] > times[0]

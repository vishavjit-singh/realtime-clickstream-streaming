"""Replay the synthetic stream into Kafka at accelerated real time.

Used by the docker-compose stack. ``confluent-kafka`` is imported lazily so the rest of the
package (and its tests) does not need a broker client.
"""
from __future__ import annotations

import logging
import time
from datetime import timedelta
from typing import Callable, Iterable

from .generator import Record

log = logging.getLogger(__name__)


def make_producer(bootstrap: str):  # pragma: no cover - needs a broker
    from confluent_kafka import Producer

    return Producer({
        "bootstrap.servers": bootstrap,
        "enable.idempotence": True,   # no duplicates from the producer's own retries
        "acks": "all",
        "linger.ms": 20,
        "compression.type": "lz4",
    })


def replay(records: Iterable[Record], producer, topic: str, speedup: float = 60.0,
           sleep: Callable[[float], None] = time.sleep) -> int:
    """Send records in arrival order, pacing them so ``speedup`` event-seconds pass per second."""
    sent, first, wall0 = 0, None, time.monotonic()
    for rec in records:
        if first is None:
            first = rec.arrival
        due = (rec.arrival - first).total_seconds() / speedup
        ahead = due - (time.monotonic() - wall0)
        if ahead > 0:
            sleep(ahead)
        kwargs = {"key": rec.key.encode(), "value": rec.value.encode()}
        if rec.partition is not None:
            kwargs["partition"] = rec.partition
        producer.produce(topic, **kwargs)
        producer.poll(0)
        sent += 1
        if sent % 2000 == 0:
            log.info("sent %s records (event clock %s)", sent, rec.arrival.isoformat())
    producer.flush(30)
    return sent


def close_stream(producer, topic: str, last_arrival, partitions: int, pause_s: float = 15.0,
                 sleep: Callable[[float], None] = time.sleep) -> None:
    """Send two rounds of heartbeats so the final windows and sessions are emitted."""
    from .generator import heartbeats

    for i, offset in enumerate((timedelta(hours=1), timedelta(hours=2))):
        if i:
            sleep(pause_s)  # let the first round land in its own micro-batch
        for rec in heartbeats(last_arrival, partitions, offset):
            producer.produce(topic, key=rec.key.encode(), value=rec.value.encode(), partition=rec.partition)
        producer.flush(30)

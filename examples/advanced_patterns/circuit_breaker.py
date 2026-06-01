"""Circuit Breaker -- KubeMQ FastStream Advanced Patterns.

Circuit breaker state machine wrapping a queue subscriber.
States: CLOSED (normal), OPEN (rejecting), HALF_OPEN (testing).
After ``failure_threshold`` consecutive failures, the circuit opens
for ``recovery_timeout`` seconds, then transitions to half-open
to test a single message.

Usage:
    python examples/advanced_patterns/circuit_breaker.py

Prerequisites:
    - KubeMQ broker running (default: localhost:50000)
    - pip install kubemq-faststream

Expected output:
    [cb] CLOSED: processing msg 1
    [cb] CLOSED: processing msg 2 -- simulated failure
    [cb] CLOSED: processing msg 3 -- simulated failure
    [cb] CLOSED: processing msg 4 -- simulated failure
    [cb] Circuit OPEN -- rejecting messages
    [cb] Circuit HALF_OPEN -- testing recovery
    [cb] HALF_OPEN: processing msg -- success, circuit CLOSED
"""

from __future__ import annotations

import asyncio
import enum
import logging
import os
import time

from faststream import FastStream

from kubemq_faststream import KubeMQBroker

logging.basicConfig(level=logging.INFO)

KUBEMQ_ADDRESS = os.environ.get("KUBEMQ_ADDRESS", "kubemq://localhost:50000")

broker = KubeMQBroker(KUBEMQ_ADDRESS)
app = FastStream(broker)


class State(enum.Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitBreaker:
    """Simple circuit breaker with failure counting and timed recovery."""

    def __init__(self, failure_threshold: int = 3, recovery_timeout: float = 5.0) -> None:
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.state = State.CLOSED
        self.failure_count = 0
        self.last_failure_time: float = 0.0

    def allow_request(self) -> bool:
        if self.state == State.CLOSED:
            return True
        if self.state == State.OPEN:
            if time.monotonic() - self.last_failure_time >= self.recovery_timeout:
                self.state = State.HALF_OPEN
                print("[cb] Circuit HALF_OPEN -- testing recovery")
                return True
            return False
        # HALF_OPEN: allow one test request
        return True

    def record_success(self) -> None:
        self.failure_count = 0
        if self.state == State.HALF_OPEN:
            print("[cb] HALF_OPEN: processing msg -- success, circuit CLOSED")
        self.state = State.CLOSED

    def record_failure(self) -> None:
        self.failure_count += 1
        self.last_failure_time = time.monotonic()
        if self.failure_count >= self.failure_threshold:
            self.state = State.OPEN
            print("[cb] Circuit OPEN -- rejecting messages")


cb = CircuitBreaker(failure_threshold=3, recovery_timeout=3.0)
message_counter = {"count": 0}


@broker.subscriber(queues="example.advanced.circuit")
async def handle(msg: dict) -> None:
    if not cb.allow_request():
        print(f"[cb] {cb.state.value}: rejecting msg")
        return

    message_counter["count"] += 1
    count = message_counter["count"]

    try:
        # Simulate: messages 2-4 fail, others succeed
        if 2 <= count <= 4:
            raise RuntimeError("simulated failure")
        print(f"[cb] {cb.state.value}: processing msg {count}")
        cb.record_success()
    except RuntimeError:
        print(f"[cb] {cb.state.value}: processing msg {count} -- simulated failure")
        cb.record_failure()


@app.after_startup
async def run_demo() -> None:
    """Publish messages to demonstrate circuit breaker transitions."""
    for i in range(6):
        await broker.publish({"task": i + 1}, queues="example.advanced.circuit")
        await asyncio.sleep(0.5)

    # Wait for recovery timeout, then send recovery test
    await asyncio.sleep(4)
    await broker.publish({"task": "recovery"}, queues="example.advanced.circuit")
    await asyncio.sleep(2)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())

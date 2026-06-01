"""Event Sourcing -- KubeMQ FastStream Advanced Patterns.

Uses Events Store as an append-only event log.  Domain events are
published to an events_store channel, and a subscriber with
``start_position=StartPosition.START_FROM_FIRST`` replays the entire
history to rebuild state.

Usage:
    python examples/advanced_patterns/event_sourcing.py

Prerequisites:
    - KubeMQ broker running (default: localhost:50000)
    - pip install kubemq-faststream

Expected output:
    Published event: account_opened
    Published event: deposit
    Published event: withdrawal
    Published event: deposit
    [replay] Event 1: account_opened -- balance=0
    [replay] Event 2: deposit -- balance=500
    [replay] Event 3: withdrawal -- balance=300
    [replay] Event 4: deposit -- balance=550
    [replay] Final reconstructed balance: 550
"""

from __future__ import annotations

import asyncio
import logging
import os

from faststream import FastStream

from kubemq_faststream import KubeMQBroker, StartPosition

logging.basicConfig(level=logging.INFO)

KUBEMQ_ADDRESS = os.environ.get("KUBEMQ_ADDRESS", "kubemq://localhost:50000")

broker = KubeMQBroker(KUBEMQ_ADDRESS)
app = FastStream(broker)

CHANNEL = "example.advanced.eventsourcing"

# Reconstructed state
account_state = {"balance": 0, "event_count": 0}


@broker.subscriber(
    events_store=CHANNEL,
    start_position=StartPosition.START_FROM_FIRST,
    group="event-sourcing-replay",
)
async def replay_events(msg: dict) -> None:
    """Replay events from the store to reconstruct account state."""
    event_type = msg.get("type", "unknown")
    amount = msg.get("amount", 0)
    account_state["event_count"] += 1
    count = account_state["event_count"]

    if event_type == "account_opened":
        account_state["balance"] = 0
    elif event_type == "deposit":
        account_state["balance"] += amount
    elif event_type == "withdrawal":
        account_state["balance"] -= amount

    balance = account_state["balance"]
    print(f"[replay] Event {count}: {event_type} -- balance={balance}")

    if count >= 4:
        print(f"[replay] Final reconstructed balance: {balance}")


@app.after_startup
async def run_demo() -> None:
    """Publish a sequence of domain events to the event store."""
    events = [
        {"type": "account_opened", "account_id": "acct-100"},
        {"type": "deposit", "account_id": "acct-100", "amount": 500},
        {"type": "withdrawal", "account_id": "acct-100", "amount": 200},
        {"type": "deposit", "account_id": "acct-100", "amount": 250},
    ]

    for event in events:
        try:
            await broker.publish(event, events_store=CHANNEL)
            print(f"Published event: {event['type']}")
        except Exception as exc:
            print(f"Publish error: {exc}")

    await asyncio.sleep(4)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())

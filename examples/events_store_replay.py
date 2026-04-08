"""Events Store with replay from first message.

Demonstrates persistent pub/sub with KubeMQ EventsStore.
Messages are stored by the broker and can be replayed from any position.

Usage:
    python examples/events_store_replay.py

Requires a KubeMQ broker on localhost:50000 (or set KUBEMQ_ADDRESS).
"""

from __future__ import annotations

import asyncio
import logging

from kubemq_faststream import KubeMQBroker, StartPosition

logging.basicConfig(level=logging.INFO)


async def main() -> None:
    # Phase 1: Publish some stored events
    print("--- Phase 1: Publishing stored events ---")
    publisher = KubeMQBroker("kubemq://localhost:50000")
    async with publisher:
        for i in range(5):
            await publisher.publish(
                {"seq": i, "data": f"event-{i}"},
                events_store="audit-log",
            )
            print(f"Published stored event #{i}")

    await asyncio.sleep(1.0)

    # Phase 2: Subscribe with replay from first
    print("\n--- Phase 2: Replaying from first ---")
    received: list[dict] = []
    done = asyncio.Event()

    replay_broker = KubeMQBroker("kubemq://localhost:50000")

    @replay_broker.subscriber(
        events_store="audit-log",
        start_position=StartPosition.START_FROM_FIRST,
    )
    async def on_audit(msg: dict) -> None:
        received.append(msg)
        print(f"Replayed: {msg}")
        if len(received) >= 5:
            done.set()

    async with replay_broker:
        try:
            await asyncio.wait_for(done.wait(), timeout=10.0)
        except TimeoutError:
            print(f"Received {len(received)}/5 messages before timeout")

    print(f"\nTotal replayed: {len(received)} events")


if __name__ == "__main__":
    asyncio.run(main())

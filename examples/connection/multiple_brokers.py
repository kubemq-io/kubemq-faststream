"""Multiple Brokers -- KubeMQ FastStream Connection.

Demonstrates two independent ``KubeMQBroker`` instances, each with its
own ``FastStream`` app and subscriber.  This pattern is useful when an
application needs to communicate with two different KubeMQ clusters or
when separating concerns across broker connections.

Each broker has an independent lifecycle (connect, subscribe, disconnect).

Usage:
    python examples/connection/multiple_brokers.py

Prerequisites:
    - KubeMQ broker running (default: localhost:50000)
    - pip install kubemq-faststream

Expected output:
    [broker-a] Received: {'source': 'broker_a'}
    [broker-b] Received: {'source': 'broker_b'}
"""

from __future__ import annotations

import asyncio
import logging
import os

from faststream import FastStream

from kubemq_faststream import KubeMQBroker

logging.basicConfig(level=logging.INFO)

KUBEMQ_ADDRESS = os.environ.get("KUBEMQ_ADDRESS", "kubemq://localhost:50000")

# Two independent brokers -- in production these could point to different clusters.
broker_a = KubeMQBroker(KUBEMQ_ADDRESS, client_id="broker-a")
broker_b = KubeMQBroker(KUBEMQ_ADDRESS, client_id="broker-b")

app = FastStream(broker_a)


@broker_a.subscriber(events="example.connection.multi_a")
async def handle_a(msg: dict) -> None:
    print(f"[broker-a] Received: {msg}")


@broker_b.subscriber(events="example.connection.multi_b")
async def handle_b(msg: dict) -> None:
    print(f"[broker-b] Received: {msg}")


@app.after_startup
async def run_demo() -> None:
    """Start broker_b manually, publish to both, then shut down."""
    # broker_a is started by FastStream(broker_a).run()
    # broker_b must be started explicitly.
    await broker_b.start()

    try:
        await broker_a.publish(
            {"source": "broker_a"},
            events="example.connection.multi_a",
        )
        await broker_b.publish(
            {"source": "broker_b"},
            events="example.connection.multi_b",
        )
    except Exception as exc:
        print(f"Publish error: {exc}")

    await asyncio.sleep(2)

    # Stop broker_b before the main app stops broker_a.
    await broker_b.stop()
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())

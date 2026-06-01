"""FastStream lifespan hooks for resource setup and teardown.

Shows the three lifecycle hooks:
- ``@app.on_startup``:  runs before the broker connects (setup resources)
- ``@app.after_startup``: runs after the broker is connected (ready to publish)
- ``@app.on_shutdown``: runs during shutdown (cleanup resources)

Usage:
    python examples/lifecycle/lifespan_management.py

Prerequisites:
    - KubeMQ broker running (default: localhost:50000)
    - pip install kubemq-faststream

Expected output:
    [Lifecycle] on_startup: resources initialized
    [Lifecycle] after_startup: broker is connected
      Shared state: {'environment': 'production', 'version': '1.0.0'}
    [production] Received: {'action': 'test', 'env': 'production'}
    [Lifecycle] on_shutdown: resources cleaned up
"""

from __future__ import annotations

import asyncio
import logging
import os

from faststream import FastStream

from kubemq_faststream import KubeMQBroker

logging.basicConfig(level=logging.INFO)

KUBEMQ_ADDRESS = os.environ.get("KUBEMQ_ADDRESS", "kubemq://localhost:50000")

broker = KubeMQBroker(KUBEMQ_ADDRESS)
app = FastStream(broker)

CHANNEL = "example.web.lifespan"

shared_state: dict[str, str] = {}


@broker.subscriber(events=CHANNEL)
async def handle_message(msg: dict) -> None:
    """Process messages using shared state initialized on startup."""
    env = shared_state.get("environment", "unknown")
    print(f"[{env}] Received: {msg}")


@app.on_startup
async def setup_resources() -> None:
    """Called BEFORE broker connects — initialize shared resources."""
    shared_state["environment"] = "production"
    shared_state["version"] = "1.0.0"
    print("[Lifecycle] on_startup: resources initialized")


@app.after_startup
async def run_demo() -> None:
    """Called AFTER broker connects — publish demo messages."""
    print("[Lifecycle] after_startup: broker is connected")
    print(f"  Shared state: {shared_state}")

    await broker.publish(
        {"action": "test", "env": shared_state["environment"]},
        events=CHANNEL,
    )

    await asyncio.sleep(2)
    await app.stop()


@app.on_shutdown
async def cleanup_resources() -> None:
    """Called during shutdown — clean up resources."""
    shared_state.clear()
    print("[Lifecycle] on_shutdown: resources cleaned up")


if __name__ == "__main__":
    asyncio.run(app.run())

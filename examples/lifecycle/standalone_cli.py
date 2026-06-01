"""Standalone CLI application using ``faststream run``.

This file is designed to be run with the FastStream CLI:

    faststream run examples.lifecycle.standalone_cli:app

The CLI handles process lifecycle, signal handling, and graceful
shutdown automatically.  When run directly with ``python``, it
falls back to ``asyncio.run(app.run())``.

Usage:
    faststream run examples.lifecycle.standalone_cli:app
    # or
    python examples/lifecycle/standalone_cli.py

Prerequisites:
    - KubeMQ broker running (default: localhost:50000)
    - pip install kubemq-faststream

Expected output:
    App started — publishing demo messages
    [CLI] Received: {'cli_msg': 0}
    [CLI] Received: {'cli_msg': 1}
    [CLI] Received: {'cli_msg': 2}
    Demo complete — use Ctrl+C to stop (or auto-stopping)
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

CHANNEL = "example.web.cli"


@broker.subscriber(events=CHANNEL)
async def handle_message(msg: dict) -> None:
    """Process incoming events."""
    print(f"[CLI] Received: {msg}")


@app.after_startup
async def run_demo() -> None:
    """Publish demo messages after startup."""
    print("App started — publishing demo messages")
    for i in range(3):
        await broker.publish({"cli_msg": i}, events=CHANNEL)

    await asyncio.sleep(2)
    print("Demo complete — use Ctrl+C to stop (or auto-stopping)")
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())

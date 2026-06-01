"""Context Manager -- KubeMQ FastStream Lifecycle.

Use the broker as an async context manager for scripts and one-off
publishers. No FastStream app needed -- direct broker usage.

Usage:
    python examples/lifecycle/context_manager.py

Prerequisites:
    - KubeMQ broker running (default: localhost:50000)
    - pip install kubemq-faststream

Expected output:
    Broker started via context manager
    Published event successfully
    Broker stopped
"""

from __future__ import annotations

import asyncio
import logging
import os

from kubemq_faststream import KubeMQBroker

logging.basicConfig(level=logging.INFO)

KUBEMQ_ADDRESS = os.environ.get("KUBEMQ_ADDRESS", "kubemq://localhost:50000")


async def main() -> None:
    async with KubeMQBroker(KUBEMQ_ADDRESS) as broker:
        print("Broker started via context manager")

        try:
            await broker.publish(
                {"source": "context_manager"},
                events="example.lifecycle.ctx",
            )
            print("Published event successfully")
        except Exception as exc:
            print(f"Error: {exc}")

    print("Broker stopped")


if __name__ == "__main__":
    asyncio.run(main())

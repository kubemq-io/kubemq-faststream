"""Automatic reconnection when the broker restarts.

The KubeMQ SDK handles gRPC reconnection transparently.  This example
publishes messages in a loop, showing that delivery resumes after a
brief broker interruption.  If no interruption occurs the demo simply
completes normally.

Usage:
    python examples/error_handling/reconnection.py

Requires:
    KubeMQ broker on localhost:50000
"""

from __future__ import annotations

import asyncio
import logging

from faststream import FastStream

from kubemq_faststream import KubeMQBroker

logging.basicConfig(level=logging.INFO)

broker = KubeMQBroker("kubemq://localhost:50000")
app = FastStream(broker)

received: list[int] = []


@broker.subscriber(events="example.error.reconnect")
async def handle_message(msg: dict) -> None:
    seq = msg.get("seq", -1)
    received.append(seq)
    print(f"Received #{seq}")


@app.after_startup
async def run_demo() -> None:
    print("Publishing 10 messages (restart the broker mid-way to test reconnection)...")
    for i in range(1, 11):
        try:
            await broker.publish({"seq": i}, events="example.error.reconnect")
            print(f"Published #{i}")
        except Exception as exc:
            print(f"Publish #{i} failed (will retry): {exc}")
        await asyncio.sleep(1)

    print(f"\nReceived {len(received)} / 10 messages: {received}")
    await asyncio.sleep(2)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())

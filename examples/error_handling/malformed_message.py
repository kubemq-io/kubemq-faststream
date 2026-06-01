"""Deserialization error when subscriber receives non-JSON data.

Publishes raw bytes that cannot be decoded as JSON.  The subscriber's
type annotation expects ``dict``, so FastStream raises a deserialization
error which is logged but does not crash the app.

Usage:
    python examples/error_handling/malformed_message.py

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


@broker.subscriber(events="example.error.malformed")
async def handle_message(msg: dict) -> None:
    print(f"Received valid message: {msg}")


@app.after_startup
async def run_demo() -> None:
    print("Publishing a valid JSON message...")
    await broker.publish({"valid": True}, events="example.error.malformed")
    await asyncio.sleep(1)

    print("Publishing malformed (non-JSON) bytes...")
    await broker.publish(b"this is not json {{{", events="example.error.malformed")
    await asyncio.sleep(1)

    print("Publishing another valid message (app should still be running)...")
    await broker.publish({"still": "working"}, events="example.error.malformed")
    await asyncio.sleep(2)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())

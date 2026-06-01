"""Configure max send/receive message size limits.

The default is 4 MB (4_194_304 bytes).  This example shows how to raise
or lower the limit, and what happens when a message exceeds it.

Usage:
    python examples/config/message_size.py

Requires:
    KubeMQ broker on localhost:50000
"""

from __future__ import annotations

import asyncio
import logging

from faststream import FastStream

from kubemq_faststream import KubeMQBroker

logging.basicConfig(level=logging.INFO)

MAX_SIZE = 1_048_576  # 1 MB

broker = KubeMQBroker(
    "kubemq://localhost:50000",
    max_send_size=MAX_SIZE,
    max_receive_size=MAX_SIZE,
)
app = FastStream(broker)


@broker.subscriber(events="example.config.msgsize")
async def handle_message(msg: dict) -> None:
    size = len(str(msg))
    print(f"Received message ({size} chars)")


@app.after_startup
async def run_demo() -> None:
    print(f"Max send size: {MAX_SIZE:,} bytes ({MAX_SIZE // 1024} KB)")

    small_payload = {"data": "x" * 1000}
    print(f"\nSending small message ({len(str(small_payload))} chars)...")
    await broker.publish(small_payload, events="example.config.msgsize")
    await asyncio.sleep(1)

    large_payload = {"data": "x" * (MAX_SIZE + 1000)}
    print(f"\nSending oversized message ({len(str(large_payload)):,} chars)...")
    try:
        await broker.publish(large_payload, events="example.config.msgsize")
        print("Published (broker may still reject if payload exceeds wire limit)")
    except Exception as exc:
        print(f"Size limit error (expected): {type(exc).__name__}: {exc}")

    await asyncio.sleep(2)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())

"""Unhandled exception inside a subscriber handler.

FastStream catches the exception, logs it, and the application continues
processing subsequent messages.  The failing message is not re-delivered
for events (fire-and-forget semantics).

Usage:
    python examples/error_handling/handler_exception.py

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

counter = 0


@broker.subscriber(events="example.error.exception")
async def handle_message(msg: dict) -> None:
    global counter  # noqa: PLW0603
    counter += 1
    if counter == 2:
        print(f"Message #{counter} — raising ValueError!")
        raise ValueError(f"Simulated failure on message #{counter}")
    print(f"Message #{counter} processed OK: {msg}")


@app.after_startup
async def run_demo() -> None:
    for i in range(1, 4):
        await broker.publish({"seq": i}, events="example.error.exception")
        await asyncio.sleep(0.5)

    print("App is still running after the handler exception.")
    await asyncio.sleep(2)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())

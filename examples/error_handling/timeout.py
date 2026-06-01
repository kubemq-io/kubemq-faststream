"""Command/query timeout when the handler is too slow.

Sets a short timeout on the ``broker.request()`` call and has the handler
sleep longer than the deadline, demonstrating the timeout error returned
to the caller.

Usage:
    python examples/error_handling/timeout.py

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


@broker.subscriber(commands="example.error.timeout")
async def slow_handler(msg: dict) -> None:
    delay = msg.get("delay", 5)
    print(f"Handler sleeping for {delay}s (longer than caller timeout)...")
    await asyncio.sleep(delay)
    print("Handler finished (caller may have already timed out)")


@app.after_startup
async def run_demo() -> None:
    await asyncio.sleep(1)

    print("Sending command with 2s timeout to a handler that sleeps 5s...")
    try:
        await broker.request(
            {"delay": 5},
            commands="example.error.timeout",
            timeout=2,
        )
        print("Command completed (unexpected)")
    except Exception as exc:
        print(f"Timeout error (expected): {type(exc).__name__}: {exc}")

    await asyncio.sleep(2)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())

"""Graceful shutdown via SIGTERM/SIGINT with in-flight message draining.

Shows how ``graceful_timeout`` controls the maximum time the app waits for
handlers to finish before stopping.  Press Ctrl-C or send SIGTERM to trigger
the shutdown sequence.

Usage:
    python examples/error_handling/graceful_shutdown.py

Requires:
    KubeMQ broker on localhost:50000
"""

from __future__ import annotations

import asyncio
import logging
import signal

from faststream import FastStream

from kubemq_faststream import KubeMQBroker

logging.basicConfig(level=logging.INFO)

broker = KubeMQBroker("kubemq://localhost:50000", graceful_timeout=10.0)
app = FastStream(broker)


@broker.subscriber(events="example.error.shutdown")
async def handle_message(msg: dict) -> None:
    print(f"Processing: {msg}")
    await asyncio.sleep(0.5)
    print(f"Done: {msg}")


@app.after_startup
async def run_demo() -> None:
    def request_stop(signum: int, _frame: object) -> None:
        sig_name = signal.Signals(signum).name
        print(f"\n{sig_name} received — requesting graceful stop...")
        asyncio.get_event_loop().call_soon_threadsafe(asyncio.ensure_future, app.stop())

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    print("App started. Send SIGTERM or press Ctrl-C to test graceful shutdown.")
    print(f"Graceful timeout: {broker.config.broker_config.graceful_timeout}s")

    for i in range(1, 6):
        await broker.publish({"seq": i}, events="example.error.shutdown")
        await asyncio.sleep(0.3)

    await asyncio.sleep(3)
    print("Demo period over — stopping.")
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())

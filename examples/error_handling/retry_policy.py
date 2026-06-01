"""Application-level retry with exponential backoff.

Demonstrates a manual retry pattern: when a publish fails, the caller
retries with increasing delays.  This is useful for transient network
errors.

Usage:
    python examples/error_handling/retry_policy.py

Requires:
    KubeMQ broker on localhost:50000
"""

from __future__ import annotations

import asyncio
import logging
import random

from faststream import FastStream

from kubemq_faststream import KubeMQBroker

logging.basicConfig(level=logging.INFO)

broker = KubeMQBroker("kubemq://localhost:50000")
app = FastStream(broker)

received: list[int] = []


@broker.subscriber(events="example.error.retry")
async def handle_message(msg: dict) -> None:
    received.append(msg.get("seq", -1))
    print(f"Received: {msg}")


async def publish_with_retry(
    payload: dict,
    channel: str,
    max_retries: int = 3,
    base_delay: float = 0.5,
) -> bool:
    """Publish with exponential backoff on failure."""
    for attempt in range(1, max_retries + 1):
        try:
            await broker.publish(payload, events=channel)
            print(f"  Attempt {attempt}: success")
            return True
        except Exception as exc:
            delay = base_delay * (2 ** (attempt - 1)) + random.uniform(0, 0.1)
            print(f"  Attempt {attempt} failed: {exc} — retrying in {delay:.1f}s")
            await asyncio.sleep(delay)
    print(f"  All {max_retries} attempts failed")
    return False


@app.after_startup
async def run_demo() -> None:
    for i in range(1, 4):
        print(f"\nPublishing message #{i} with retry...")
        await publish_with_retry({"seq": i}, "example.error.retry")

    await asyncio.sleep(2)
    print(f"\nTotal received: {len(received)}")
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())

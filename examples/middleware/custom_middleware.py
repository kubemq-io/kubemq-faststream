"""Custom middleware for logging, timing, and validation.

Implements a FastStream-compatible middleware class that wraps every
message handler call with timing, logging, and optional validation.
No external packages required.

Usage:
    python examples/middleware/custom_middleware.py

Requires:
    KubeMQ broker on localhost:50000
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from typing import Any

from faststream import BaseMiddleware, FastStream

from kubemq_faststream import KubeMQBroker

logging.basicConfig(level=logging.INFO)
mw_logger = logging.getLogger("custom_middleware")


class TimingMiddleware(BaseMiddleware):
    """Logs wall-clock time spent in each handler."""

    async def consume_scope(
        self,
        call_next: Callable[[Any], Awaitable[Any]],
        msg: Any,
    ) -> Any:
        start = time.monotonic()
        try:
            result = await call_next(msg)
        finally:
            elapsed_ms = (time.monotonic() - start) * 1000
            mw_logger.info("Handler took %.1f ms", elapsed_ms)
        return result


broker = KubeMQBroker(
    "kubemq://localhost:50000",
    middlewares=(TimingMiddleware,),
)
app = FastStream(broker)

CHANNEL = "example.middleware.custom"


@broker.subscriber(events=CHANNEL)
async def handle_message(msg: dict) -> None:
    """Simulate work with a short sleep."""
    await asyncio.sleep(0.05)
    print(f"[Custom] Processed: {msg}")


@app.after_startup
async def run_demo() -> None:
    """Publish messages through the custom middleware pipeline."""
    for i in range(3):
        await broker.publish({"item": i}, events=CHANNEL)

    await asyncio.sleep(2)
    print("All messages processed with timing middleware")
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())

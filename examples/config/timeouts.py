"""Configure command/query timeout and graceful shutdown timeout.

``default_cq_timeout`` sets the default deadline for ``broker.request()``
calls.  ``graceful_timeout`` controls how long the app waits for in-flight
handlers to finish when stopping.

Usage:
    python examples/config/timeouts.py

Requires:
    KubeMQ broker on localhost:50000
"""

from __future__ import annotations

import asyncio
import logging

from faststream import FastStream

from kubemq_faststream import KubeMQBroker

logging.basicConfig(level=logging.INFO)

broker = KubeMQBroker(
    "kubemq://localhost:50000",
    default_cq_timeout=10,  # 10-second default for commands/queries
    graceful_timeout=5.0,  # 5-second shutdown window
)
app = FastStream(broker)


@broker.subscriber(queries="example.config.timeout")
async def handle_query(msg: dict) -> dict:
    print(f"Query received: {msg}")
    return {"result": "ok", "cq_timeout": 10}


@app.after_startup
async def run_demo() -> None:
    cfg = broker.config.broker_config
    print(f"Default CQ timeout:  {cfg.default_cq_timeout}s")
    print(f"Graceful timeout:    {cfg.graceful_timeout}s")

    result = await broker.request(
        {"question": "config-check"},
        queries="example.config.timeout",
    )
    print(f"Query result: {result}")

    await asyncio.sleep(2)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())

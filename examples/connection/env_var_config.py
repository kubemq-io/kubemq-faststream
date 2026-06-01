"""Configuration via environment variables — no constructor arguments needed.

The broker reads ``KUBEMQ_ADDRESS``, ``KUBEMQ_CLIENT_ID``,
``KUBEMQ_AUTH_TOKEN``, and ``KUBEMQ_TLS_*`` env vars automatically when
they are set.  This example sets them programmatically for demonstration.

Usage:
    KUBEMQ_ADDRESS=localhost:50000 python examples/connection/env_var_config.py

Prerequisites:
    - KubeMQ broker running (default: localhost:50000)
    - pip install kubemq-faststream

Expected output:
    KUBEMQ_ADDRESS  = localhost:50000
    KUBEMQ_CLIENT_ID = env-var-demo
    Received: {'source': 'env_var_config'}
"""

from __future__ import annotations

import asyncio
import logging
import os

from faststream import FastStream

from kubemq_faststream import KubeMQBroker

logging.basicConfig(level=logging.INFO)

os.environ.setdefault("KUBEMQ_ADDRESS", "localhost:50000")
os.environ.setdefault("KUBEMQ_CLIENT_ID", "env-var-demo")

broker = KubeMQBroker()
app = FastStream(broker)


@broker.subscriber(events="example.connection.envvar")
async def handle_message(msg: dict) -> None:
    print(f"Received: {msg}")


@app.after_startup
async def run_demo() -> None:
    print(f"KUBEMQ_ADDRESS  = {os.environ.get('KUBEMQ_ADDRESS')}")
    print(f"KUBEMQ_CLIENT_ID = {os.environ.get('KUBEMQ_CLIENT_ID')}")
    await broker.publish({"source": "env_var_config"}, events="example.connection.envvar")
    await asyncio.sleep(2)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())

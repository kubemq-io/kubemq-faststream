"""All supported broker URL formats.

Demonstrates the two URL schemes (``kubemq://`` and ``kubemq+tls://``)
and how the env var ``KUBEMQ_ADDRESS`` overrides the constructor URL.

Usage:
    python examples/config/broker_url_formats.py

Requires:
    KubeMQ broker on localhost:50000
"""

from __future__ import annotations

import asyncio
import logging

from faststream import FastStream

from kubemq_faststream import KubeMQBroker

logging.basicConfig(level=logging.INFO)

url_formats = [
    "kubemq://localhost:50000",
    "kubemq://127.0.0.1:50000",
]

tls_formats = [
    "kubemq+tls://localhost:50000",
    "kubemq+tls://my-broker.example.com:50000",
]

print("Supported URL formats:")
for url in url_formats:
    print(f"  Plain:  {url}")
for url in tls_formats:
    print(f"  TLS:    {url}")

broker = KubeMQBroker(url_formats[0])
app = FastStream(broker)


@broker.subscriber(events="example.config.urlformats")
async def handle_message(msg: dict) -> None:
    print(f"Received: {msg}")


@app.after_startup
async def run_demo() -> None:
    print(f"\nActive URL: {broker.config.broker_config.url}")
    await broker.publish({"format": "kubemq://"}, events="example.config.urlformats")
    await asyncio.sleep(2)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())

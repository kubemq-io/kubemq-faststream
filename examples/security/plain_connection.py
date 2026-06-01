"""Plain Connection -- KubeMQ FastStream Security.

Establish a plain (non-TLS) connection to a KubeMQ broker using the
explicit ``kubemq://`` URL scheme.  This is the simplest connection mode
and should be used in development only -- never expose an unencrypted
broker to untrusted networks.

A ping health check confirms the connection is alive.

Usage:
    python examples/security/plain_connection.py

Prerequisites:
    - KubeMQ broker running (default: localhost:50000)
    - pip install kubemq-faststream

Expected output:
    Broker connected (plain): True
    Received: {'mode': 'plain', 'encrypted': False}
    Published over plain connection
"""

from __future__ import annotations

import asyncio
import logging
import os

from faststream import FastStream

from kubemq_faststream import KubeMQBroker

logging.basicConfig(level=logging.INFO)

# Use in development only -- plain connections are NOT encrypted.
KUBEMQ_ADDRESS = os.environ.get("KUBEMQ_ADDRESS", "kubemq://localhost:50000")

broker = KubeMQBroker(KUBEMQ_ADDRESS)
app = FastStream(broker)


@broker.subscriber(events="example.security.plain")
async def handle_message(msg: dict) -> None:
    print(f"Received: {msg}")


@app.after_startup
async def run_demo() -> None:
    """Verify plain connection and publish a test message."""
    try:
        is_connected = await broker.ping(timeout=5.0)
        print(f"Broker connected (plain): {is_connected}")

        await broker.publish(
            {"mode": "plain", "encrypted": False},
            events="example.security.plain",
        )
        print("Published over plain connection")
        await asyncio.sleep(2)
    except Exception as exc:
        logging.error("Plain connection failed: %s", exc)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())

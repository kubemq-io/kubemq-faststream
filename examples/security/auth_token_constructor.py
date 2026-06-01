"""Auth Token (Constructor) -- KubeMQ FastStream Security.

Pass a JWT authentication token directly via the ``auth_token``
constructor argument (broker.py:68).  Every gRPC call includes the
token in its metadata.  If the broker rejects the token the connection
will fail with an authentication error.

Usage:
    export KUBEMQ_ADDRESS="kubemq://localhost:50000"
    python examples/security/auth_token_constructor.py

Prerequisites:
    - KubeMQ broker on localhost:50000 with auth enabled
    - A valid JWT token
    - pip install kubemq-faststream

Expected output:
    Received: {'secured': True, 'method': 'constructor'}
    Published with auth token (constructor)
"""

from __future__ import annotations

import asyncio
import logging
import os

from faststream import FastStream

from kubemq_faststream import KubeMQBroker

logging.basicConfig(level=logging.INFO)

KUBEMQ_ADDRESS = os.environ.get("KUBEMQ_ADDRESS", "kubemq://localhost:50000")

# Replace with a real JWT token in production.
AUTH_TOKEN = "my-jwt-token-here"

broker = KubeMQBroker(
    KUBEMQ_ADDRESS,
    auth_token=AUTH_TOKEN,  # broker.py:68
)
app = FastStream(broker)


@broker.subscriber(events="example.security.auth-constructor")
async def handle_message(msg: dict) -> None:
    print(f"Received: {msg}")


@app.after_startup
async def run_demo() -> None:
    """Publish and receive a message using token-based auth."""
    try:
        await broker.publish(
            {"secured": True, "method": "constructor"},
            events="example.security.auth-constructor",
        )
        print("Published with auth token (constructor)")
        await asyncio.sleep(2)
    except Exception as exc:
        logging.error("Auth error (expected if token is invalid): %s", exc)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())

"""TLS with Auth Token -- KubeMQ FastStream Security.

Combine TLS encryption with JWT authentication in a single broker
connection.  Uses the ``kubemq+tls://`` URL scheme (security.py:33)
for transport encryption and the ``auth_token`` constructor parameter
(broker.py:68) for request-level authentication.

This is the recommended production configuration: encrypted transport
plus identity verification on every gRPC call.

Usage:
    export KUBEMQ_ADDRESS="kubemq+tls://your-broker:50000"
    export KUBEMQ_TLS_CA_FILE="/path/to/ca.pem"
    python examples/security/tls_with_auth.py

Prerequisites:
    - KubeMQ broker with TLS and auth enabled
    - Valid CA certificate and JWT token
    - pip install kubemq-faststream

Expected output:
    Received: {'encrypted': True, 'authenticated': True}
    Published with TLS + auth token
"""

from __future__ import annotations

import asyncio
import logging
import os

from faststream import FastStream

from kubemq_faststream import KubeMQBroker

logging.basicConfig(level=logging.INFO)

# kubemq+tls:// enables TLS on the gRPC channel (security.py:33).
KUBEMQ_ADDRESS = os.environ.get("KUBEMQ_ADDRESS", "kubemq+tls://localhost:50000")

# Auth token -- in production, load from a secrets manager or env var.
# The SDK also checks KUBEMQ_AUTH_TOKEN (security.py:68).
AUTH_TOKEN = os.environ.get("KUBEMQ_AUTH_TOKEN", "my-jwt-token-here")

# CA cert to verify the server certificate.
TLS_CA_FILE = os.environ.get("KUBEMQ_TLS_CA_FILE", "/path/to/ca.pem")

broker = KubeMQBroker(
    KUBEMQ_ADDRESS,
    auth_token=AUTH_TOKEN,  # broker.py:68
    tls_ca_file=TLS_CA_FILE,  # broker.py:73
)
app = FastStream(broker)


@broker.subscriber(events="example.security.tls-auth")
async def handle_message(msg: dict) -> None:
    print(f"Received: {msg}")


@app.after_startup
async def run_demo() -> None:
    """Publish and receive a message over TLS with auth."""
    try:
        await broker.publish(
            {"encrypted": True, "authenticated": True},
            events="example.security.tls-auth",
        )
        print("Published with TLS + auth token")
        await asyncio.sleep(2)
    except Exception as exc:
        logging.error("TLS+auth error: %s", exc)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())

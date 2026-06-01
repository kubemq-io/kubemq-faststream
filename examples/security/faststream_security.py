"""FastStream BaseSecurity -- KubeMQ FastStream Security.

Use FastStream's built-in ``BaseSecurity`` class with a standard-library
``ssl.SSLContext`` to configure TLS.  The ``security`` parameter on the
broker constructor (broker.py:86) accepts any ``BaseSecurity`` instance,
making it compatible with the same security API used by other FastStream
brokers (Kafka, RabbitMQ, etc.).

This approach is useful when you need fine-grained control over TLS
settings (cipher suites, hostname checking, protocol versions) beyond
what the ``tls_*`` constructor parameters offer.

Usage:
    export KUBEMQ_ADDRESS="kubemq+tls://your-broker:50000"
    python examples/security/faststream_security.py

Prerequisites:
    - KubeMQ broker with TLS enabled
    - Valid certificate files at the configured paths
    - pip install kubemq-faststream

Expected output:
    Received: {'security': 'BaseSecurity', 'ssl': True}
    Published with FastStream BaseSecurity
"""

from __future__ import annotations

import asyncio
import logging
import os
import ssl

from faststream import FastStream
from faststream.security import BaseSecurity

from kubemq_faststream import KubeMQBroker

logging.basicConfig(level=logging.INFO)

KUBEMQ_ADDRESS = os.environ.get("KUBEMQ_ADDRESS", "kubemq+tls://localhost:50000")

# ---- Build an SSLContext with fine-grained settings ----
# In production, replace these placeholder paths with real cert files.
CA_FILE = os.environ.get("KUBEMQ_TLS_CA_FILE", "/path/to/ca.pem")
CERT_FILE = os.environ.get("KUBEMQ_TLS_CERT_FILE", "/path/to/client.pem")
KEY_FILE = os.environ.get("KUBEMQ_TLS_KEY_FILE", "/path/to/client-key.pem")

ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ssl_ctx.minimum_version = ssl.TLSVersion.TLSv1_2

# Load CA cert for server verification.
# ssl_ctx.load_verify_locations(CA_FILE)

# Load client cert + key for mTLS (optional).
# ssl_ctx.load_cert_chain(certfile=CERT_FILE, keyfile=KEY_FILE)

# For development / self-signed certs only:
ssl_ctx.check_hostname = False
ssl_ctx.verify_mode = ssl.CERT_NONE

# Wrap in FastStream's BaseSecurity so the broker can consume it.
security = BaseSecurity(ssl_context=ssl_ctx)

broker = KubeMQBroker(
    KUBEMQ_ADDRESS,
    security=security,  # broker.py:86
)
app = FastStream(broker)


@broker.subscriber(events="example.security.faststream-sec")
async def handle_message(msg: dict) -> None:
    print(f"Received: {msg}")


@app.after_startup
async def run_demo() -> None:
    """Publish and receive using FastStream BaseSecurity."""
    try:
        await broker.publish(
            {"security": "BaseSecurity", "ssl": True},
            events="example.security.faststream-sec",
        )
        print("Published with FastStream BaseSecurity")
        await asyncio.sleep(2)
    except Exception as exc:
        logging.error("BaseSecurity connection error: %s", exc)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())

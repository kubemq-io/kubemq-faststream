"""TLS Server Certificate -- KubeMQ FastStream Security.

Connect to a KubeMQ broker over TLS using the ``kubemq+tls://`` URL
scheme and a custom CA certificate to verify the server identity.

The ``tls_ca_file`` parameter (broker.py:73) points to the CA bundle
that signed the server certificate.  The ``kubemq+tls://`` scheme
(security.py:33) automatically enables TLS on the gRPC channel.

The CA file path can also be supplied via the ``KUBEMQ_TLS_CA_FILE``
environment variable which takes precedence over the constructor arg.

Usage:
    export KUBEMQ_ADDRESS="kubemq+tls://your-broker:50000"
    export KUBEMQ_TLS_CA_FILE="/path/to/ca.pem"
    python examples/security/tls_server_cert.py

Prerequisites:
    - KubeMQ broker with TLS enabled
    - CA certificate file at the configured path
    - pip install kubemq-faststream

Expected output:
    Received over TLS: {'encrypted': True, 'mode': 'tls_server_cert'}
    Published over TLS connection
"""

from __future__ import annotations

import asyncio
import logging
import os

from faststream import FastStream

from kubemq_faststream import KubeMQBroker

logging.basicConfig(level=logging.INFO)

KUBEMQ_ADDRESS = os.environ.get("KUBEMQ_ADDRESS", "kubemq+tls://localhost:50000")

# CA certificate path -- override via KUBEMQ_TLS_CA_FILE env var.
# The env var is resolved internally by security.py:70.
TLS_CA_FILE = os.environ.get("KUBEMQ_TLS_CA_FILE", "/path/to/ca.pem")

broker = KubeMQBroker(
    KUBEMQ_ADDRESS,
    tls_ca_file=TLS_CA_FILE,  # broker.py:73
)
app = FastStream(broker)


@broker.subscriber(events="example.security.tls")
async def handle_message(msg: dict) -> None:
    print(f"Received over TLS: {msg}")


@app.after_startup
async def run_demo() -> None:
    """Publish and receive a message over a TLS connection."""
    try:
        await broker.publish(
            {"encrypted": True, "mode": "tls_server_cert"},
            events="example.security.tls",
        )
        print("Published over TLS connection")
        await asyncio.sleep(2)
    except Exception as exc:
        logging.error("TLS connection error (expected without valid certs): %s", exc)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())

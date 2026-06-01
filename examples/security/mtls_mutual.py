"""Mutual TLS (mTLS) -- KubeMQ FastStream Security.

Establish a mutual-TLS connection where both client and server present
certificates for authentication.  The broker validates the client
certificate, and the client validates the server certificate against
the provided CA.

Constructor parameters used (broker.py):
    - tls_enabled=True   (line 70) -- explicitly enable TLS
    - tls_cert_file=     (line 71) -- client certificate
    - tls_key_file=      (line 72) -- client private key
    - tls_ca_file=       (line 73) -- CA certificate to verify server

All three cert paths can be supplied via environment variables which
take precedence over constructor args:
    - KUBEMQ_TLS_CERT_FILE  (security.py:70)
    - KUBEMQ_TLS_KEY_FILE   (security.py:71)
    - KUBEMQ_TLS_CA_FILE    (security.py:72)

Usage:
    export KUBEMQ_ADDRESS="kubemq://localhost:50000"
    export KUBEMQ_TLS_CERT_FILE="/path/to/client.pem"
    export KUBEMQ_TLS_KEY_FILE="/path/to/client-key.pem"
    export KUBEMQ_TLS_CA_FILE="/path/to/ca.pem"
    python examples/security/mtls_mutual.py

Prerequisites:
    - KubeMQ broker with mTLS enabled
    - Valid client certificate, key, and CA files
    - pip install kubemq-faststream

Expected output:
    Received over mTLS: {'mutual_tls': True, 'mode': 'mtls'}
    Published over mTLS connection
"""

from __future__ import annotations

import asyncio
import logging
import os

from faststream import FastStream

from kubemq_faststream import KubeMQBroker

logging.basicConfig(level=logging.INFO)

KUBEMQ_ADDRESS = os.environ.get("KUBEMQ_ADDRESS", "kubemq://localhost:50000")

# Certificate paths -- override via KUBEMQ_TLS_* env vars.
TLS_CERT_FILE = os.environ.get("KUBEMQ_TLS_CERT_FILE", "/path/to/client.pem")
TLS_KEY_FILE = os.environ.get("KUBEMQ_TLS_KEY_FILE", "/path/to/client-key.pem")
TLS_CA_FILE = os.environ.get("KUBEMQ_TLS_CA_FILE", "/path/to/ca.pem")

broker = KubeMQBroker(
    KUBEMQ_ADDRESS,
    tls_enabled=True,  # broker.py:70
    tls_cert_file=TLS_CERT_FILE,  # broker.py:71
    tls_key_file=TLS_KEY_FILE,  # broker.py:72
    tls_ca_file=TLS_CA_FILE,  # broker.py:73
)
app = FastStream(broker)


@broker.subscriber(events="example.security.mtls")
async def handle_message(msg: dict) -> None:
    print(f"Received over mTLS: {msg}")


@app.after_startup
async def run_demo() -> None:
    """Publish and receive a message over a mutual-TLS connection."""
    try:
        await broker.publish(
            {"mutual_tls": True, "mode": "mtls"},
            events="example.security.mtls",
        )
        print("Published over mTLS connection")
        await asyncio.sleep(2)
    except Exception as exc:
        logging.error("mTLS connection error (expected without valid certs): %s", exc)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())

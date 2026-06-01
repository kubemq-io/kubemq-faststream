"""TLS via Environment Variables -- KubeMQ FastStream Security.

Configure the full TLS stack entirely through environment variables
with no constructor arguments.  The SDK resolves these in
``security.py:58-72``:

    - KUBEMQ_TLS_ENABLED   (security.py:58) -- "true" / "false"
    - KUBEMQ_TLS_CERT_FILE (security.py:70) -- client certificate path
    - KUBEMQ_TLS_KEY_FILE  (security.py:71) -- client private key path
    - KUBEMQ_TLS_CA_FILE   (security.py:72) -- CA certificate path

This approach keeps all security configuration in the deployment
environment (e.g., Kubernetes secrets, Docker env files) rather than
in application code.

Usage:
    export KUBEMQ_ADDRESS="kubemq://localhost:50000"
    export KUBEMQ_TLS_ENABLED="true"
    export KUBEMQ_TLS_CERT_FILE="/path/to/client.pem"
    export KUBEMQ_TLS_KEY_FILE="/path/to/client-key.pem"
    export KUBEMQ_TLS_CA_FILE="/path/to/ca.pem"
    python examples/security/tls_env_vars.py

Prerequisites:
    - KubeMQ broker with TLS/mTLS enabled
    - Valid certificate files at the configured paths
    - pip install kubemq-faststream

Expected output:
    TLS enabled (env): true
    Cert file: /path/to/client.pem
    Key file:  /path/to/client-key.pem
    CA file:   /path/to/ca.pem
    Received: {'source': 'tls_env_vars', 'all_env': True}
    Published with TLS (env var config)
"""

from __future__ import annotations

import asyncio
import logging
import os

from faststream import FastStream

from kubemq_faststream import KubeMQBroker

logging.basicConfig(level=logging.INFO)

KUBEMQ_ADDRESS = os.environ.get("KUBEMQ_ADDRESS", "kubemq://localhost:50000")

# No TLS constructor args -- everything resolved from env vars by the
# SDK's resolve_env_config() in security.py:58-72.
broker = KubeMQBroker(KUBEMQ_ADDRESS)
app = FastStream(broker)


@broker.subscriber(events="example.security.tls-envvars")
async def handle_message(msg: dict) -> None:
    print(f"Received: {msg}")


@app.after_startup
async def run_demo() -> None:
    """Print resolved TLS env vars and publish a test message."""
    tls_enabled = os.environ.get("KUBEMQ_TLS_ENABLED", "not set")
    cert_file = os.environ.get("KUBEMQ_TLS_CERT_FILE", "not set")
    key_file = os.environ.get("KUBEMQ_TLS_KEY_FILE", "not set")
    ca_file = os.environ.get("KUBEMQ_TLS_CA_FILE", "not set")

    print(f"TLS enabled (env): {tls_enabled}")
    print(f"Cert file: {cert_file}")
    print(f"Key file:  {key_file}")
    print(f"CA file:   {ca_file}")

    if tls_enabled == "not set":
        logging.warning("KUBEMQ_TLS_ENABLED not set -- TLS will not be enabled.")

    try:
        await broker.publish(
            {"source": "tls_env_vars", "all_env": True},
            events="example.security.tls-envvars",
        )
        print("Published with TLS (env var config)")
        await asyncio.sleep(2)
    except Exception as exc:
        logging.error("TLS connection error: %s", exc)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())

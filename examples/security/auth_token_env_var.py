"""Auth Token (Env Var) -- KubeMQ FastStream Security.

Supply the JWT authentication token exclusively via the
``KUBEMQ_AUTH_TOKEN`` environment variable -- no constructor argument
needed.  The SDK reads the env var in ``security.py:68``::

    auth_token=os.environ.get("KUBEMQ_AUTH_TOKEN", auth_token)

This keeps secrets out of source code and works well with container
orchestrators that inject env vars from a secrets store.

Usage:
    export KUBEMQ_ADDRESS="kubemq://localhost:50000"
    export KUBEMQ_AUTH_TOKEN="my-jwt-token-here"
    python examples/security/auth_token_env_var.py

Prerequisites:
    - KubeMQ broker with auth enabled
    - KUBEMQ_AUTH_TOKEN environment variable set
    - pip install kubemq-faststream

Expected output:
    Token configured: True
    Received: {'secured': True, 'method': 'env_var'}
    Published with auth token (env var)
"""

from __future__ import annotations

import asyncio
import logging
import os

from faststream import FastStream

from kubemq_faststream import KubeMQBroker

logging.basicConfig(level=logging.INFO)

KUBEMQ_ADDRESS = os.environ.get("KUBEMQ_ADDRESS", "kubemq://localhost:50000")

# No auth_token= constructor arg -- the SDK resolves KUBEMQ_AUTH_TOKEN
# automatically at security.py:68.
broker = KubeMQBroker(KUBEMQ_ADDRESS)
app = FastStream(broker)


@broker.subscriber(events="example.security.auth-envvar")
async def handle_message(msg: dict) -> None:
    print(f"Received: {msg}")


@app.after_startup
async def run_demo() -> None:
    """Verify the env-var token is picked up and publish a test message."""
    token = os.environ.get("KUBEMQ_AUTH_TOKEN")
    print(f"Token configured: {token is not None}")

    if not token:
        logging.warning("KUBEMQ_AUTH_TOKEN not set -- broker will connect without auth.")

    try:
        await broker.publish(
            {"secured": True, "method": "env_var"},
            events="example.security.auth-envvar",
        )
        print("Published with auth token (env var)")
        await asyncio.sleep(2)
    except Exception as exc:
        logging.error("Auth error: %s", exc)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())

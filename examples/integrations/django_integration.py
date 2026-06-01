"""Django Integration -- KubeMQ FastStream Integrations.

Standalone Django script with inline settings that runs a KubeMQ
FastStream broker alongside Django. Uses django.core.management
to define a custom management command that starts the broker.

This is a self-contained script -- no Django project structure
required. It configures Django settings inline and demonstrates
how to bridge Django's synchronous world with the async FastStream
broker.

Usage:
    python examples/integrations/django_integration.py

Prerequisites:
    - KubeMQ broker running (default: localhost:50000)
    - pip install kubemq-faststream django

Expected output:
    [Django] Configuring inline settings
    [Django] Starting KubeMQ broker
    [KubeMQ] Received: {'source': 'django', 'action': 'demo'}
    [Django] Demo complete
"""

from __future__ import annotations

import asyncio
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    import django
    from django.conf import settings
except ImportError as _err:
    raise SystemExit("This example requires 'django'.\nInstall with:  pip install django") from _err

# -- Inline Django settings -------------------------------------------------

print("[Django] Configuring inline settings")

if not settings.configured:
    settings.configure(
        DEBUG=True,
        SECRET_KEY="example-secret-key-not-for-production",
        INSTALLED_APPS=[
            "django.contrib.contenttypes",
        ],
        DATABASES={
            "default": {
                "ENGINE": "django.db.backends.sqlite3",
                "NAME": ":memory:",
            }
        },
        DEFAULT_AUTO_FIELD="django.db.models.BigAutoField",
    )
    django.setup()

# -- FastStream / KubeMQ setup ---------------------------------------------

from faststream import FastStream  # noqa: E402

from kubemq_faststream import KubeMQBroker  # noqa: E402

KUBEMQ_ADDRESS = os.environ.get("KUBEMQ_ADDRESS", "kubemq://localhost:50000")
CHANNEL = "example.integrations.django"

broker = KubeMQBroker(KUBEMQ_ADDRESS)
faststream_app = FastStream(broker)


@broker.subscriber(events=CHANNEL)
async def handle_message(msg: dict) -> None:
    """Process KubeMQ messages in Django context."""
    logger.info("[KubeMQ] Received: %s", msg)
    print(f"[KubeMQ] Received: {msg}")


# -- Management command equivalent ------------------------------------------


async def run_broker() -> None:
    """Start the broker, publish demo messages, then stop."""
    print("[Django] Starting KubeMQ broker")
    await faststream_app.start()

    try:
        # Publish demo messages
        for i in range(3):
            payload = {"source": "django", "action": "demo", "index": i}
            await broker.publish(payload, events=CHANNEL)
            logger.info("Published: %s", payload)

        await asyncio.sleep(2)
        print("[Django] Demo complete")
    finally:
        await faststream_app.stop()
        logger.info("Broker stopped")


if __name__ == "__main__":
    asyncio.run(run_broker())

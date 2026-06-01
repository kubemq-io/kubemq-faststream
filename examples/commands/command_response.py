"""Command response inspection.

Shows how to return structured data from a command handler and
inspect the response on the caller side.  The handler returns a
dict with execution details that the caller can examine.

Usage:
    python examples/commands/command_response.py

Requires:
    KubeMQ broker on localhost:50000
"""

from __future__ import annotations

import asyncio
import logging

from faststream import FastStream

from kubemq_faststream import KubeMQBroker

logging.basicConfig(level=logging.INFO)

broker = KubeMQBroker("kubemq://localhost:50000")
app = FastStream(broker)

CHANNEL = "example.commands.response"


@broker.subscriber(commands=CHANNEL)
async def handle_deploy(msg: dict) -> dict:
    """Simulate a deployment command with detailed response."""
    service = msg.get("service", "unknown")
    version = msg.get("version", "0.0.0")
    print(f"Deploying {service} v{version}...")

    return {
        "status": "deployed",
        "service": service,
        "version": version,
        "replicas": 3,
        "healthy": True,
    }


@app.after_startup
async def run_demo() -> None:
    """Send a deploy command and inspect the full response."""
    print("Sending deploy command...")
    response = await broker.request(
        {"service": "payment-api", "version": "2.1.0"},
        commands=CHANNEL,
    )
    print(f"Raw response: {response}")

    print("\nParsed response details:")
    if isinstance(response, dict):
        print(f"  Status:   {response.get('status')}")
        print(f"  Service:  {response.get('service')}")
        print(f"  Version:  {response.get('version')}")
        print(f"  Replicas: {response.get('replicas')}")
        print(f"  Healthy:  {response.get('healthy')}")

    await asyncio.sleep(2)
    print("\nDemo complete")
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())

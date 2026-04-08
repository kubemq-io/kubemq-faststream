"""Command sender + handler (RPC-style).

Demonstrates request-reply messaging with KubeMQ Commands.
Commands are fire-and-wait: the sender blocks until the handler
confirms execution (no return data, just success/failure).

Usage:
    python examples/commands_rpc.py

Requires a KubeMQ broker on localhost:50000 (or set KUBEMQ_ADDRESS).
"""

from __future__ import annotations

import asyncio
import logging

from faststream import FastStream

from kubemq_faststream import KubeMQBroker

logging.basicConfig(level=logging.INFO)

broker = KubeMQBroker("kubemq://localhost:50000")
app = FastStream(broker)


@broker.subscriber(commands="device.restart")
async def handle_restart(msg: dict) -> None:
    """Handle a device restart command.

    The handler processes the command and returns None (void).
    The broker automatically sends a success/failure response
    back to the requester.
    """
    device_id = msg.get("device_id", "unknown")
    print(f"Restarting device: {device_id}")
    # Simulate restart operation
    await asyncio.sleep(0.5)
    print(f"Device {device_id} restarted successfully")


@app.after_startup
async def send_commands() -> None:
    """Send commands after startup."""
    # Wait for subscription to be established
    await asyncio.sleep(1.0)

    devices = ["sensor-01", "sensor-02", "gateway-03"]
    for device_id in devices:
        print(f"\nSending restart command for {device_id}...")
        result = await broker.request(
            {"device_id": device_id, "reason": "maintenance"},
            commands="device.restart",
            timeout=10,
        )
        print(f"Command completed for {device_id}: {result}")

    await asyncio.sleep(1)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())

"""Live broker fixtures for integration tests.

Requires a KubeMQ broker on KUBEMQ_ADDRESS (default localhost:50000).
All integration tests are skipped when the broker is unreachable.
"""

from __future__ import annotations

import os
import socket
import uuid
from collections.abc import AsyncIterator

import pytest

from kubemq_faststream import KubeMQBroker


def _broker_available() -> bool:
    """Check if a KubeMQ broker is reachable via TCP."""
    address = os.environ.get("KUBEMQ_ADDRESS", "localhost:50000")
    # Strip scheme if present
    for prefix in ("kubemq+tls://", "kubemq://"):
        if address.startswith(prefix):
            address = address[len(prefix):]
            break
    host, _, port_str = address.rpartition(":")
    if not host:
        host = address
        port_str = "50000"
    try:
        port = int(port_str)
    except ValueError:
        return False
    try:
        sock = socket.create_connection((host, port), timeout=2.0)
        sock.close()
        return True
    except OSError:
        return False


BROKER_AVAILABLE = _broker_available()

pytestmark = pytest.mark.integration


def unique_channel(prefix: str = "test") -> str:
    """Generate a unique channel name to avoid inter-test conflicts."""
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


@pytest.fixture
def broker_url() -> str:
    """Return the broker URL from env or default."""
    return os.environ.get("KUBEMQ_ADDRESS", "kubemq://localhost:50000")


@pytest.fixture
def channel_name() -> str:
    """Return a unique channel name for each test."""
    return unique_channel()


@pytest.fixture
async def broker(broker_url: str) -> AsyncIterator[KubeMQBroker]:
    """Create, connect, and yield a broker; close on teardown."""
    if not BROKER_AVAILABLE:
        pytest.skip("KubeMQ broker not available")
    br = KubeMQBroker(broker_url)
    await br.connect()
    yield br
    await br.close()

"""Shared test fixtures for kubemq-faststream."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from kubemq_faststream.broker import KubeMQBroker
from kubemq_faststream.config import KubeMQConnection
from kubemq_faststream.testing import TestKubeMQBroker, _build_mock_connection


@pytest.fixture
def broker():
    """Create a fresh KubeMQBroker instance."""
    return KubeMQBroker("kubemq://localhost:50000")


@pytest.fixture
async def test_broker(broker):
    """Create a TestKubeMQBroker context."""
    async with TestKubeMQBroker(broker) as br:
        yield br


@pytest.fixture
def mock_connection():
    """Create a mock KubeMQConnection with all methods as AsyncMock."""
    return _build_mock_connection()

"""Tests for kubemq_faststream/__init__.py lazy import and __getattr__."""

from __future__ import annotations

from unittest.mock import patch

import pytest


def test_getattr_kubemq_broker():
    """__getattr__('KubeMQBroker') returns the broker class."""
    import kubemq_faststream

    cls = kubemq_faststream.KubeMQBroker
    from kubemq_faststream.broker import KubeMQBroker

    assert cls is KubeMQBroker


def test_getattr_kubemq_message():
    """__getattr__('KubeMQMessage') returns the message class."""
    import kubemq_faststream

    cls = kubemq_faststream.KubeMQMessage
    from kubemq_faststream.message import KubeMQMessage

    assert cls is KubeMQMessage


def test_getattr_kubemq_publish_command():
    """__getattr__('KubeMQPublishCommand') returns the publish command class."""
    import kubemq_faststream

    cls = kubemq_faststream.KubeMQPublishCommand
    from kubemq_faststream.response import KubeMQPublishCommand

    assert cls is KubeMQPublishCommand


def test_getattr_kubemq_router():
    """__getattr__('KubeMQRouter') returns the router class."""
    import kubemq_faststream

    cls = kubemq_faststream.KubeMQRouter
    from kubemq_faststream.router import KubeMQRouter

    assert cls is KubeMQRouter


def test_getattr_kubemq_publisher():
    """__getattr__('KubeMQPublisher') returns the publisher class."""
    import kubemq_faststream

    cls = kubemq_faststream.KubeMQPublisher
    from kubemq_faststream.publisher import KubeMQPublisher

    assert cls is KubeMQPublisher


def test_getattr_test_kubemq_broker():
    """__getattr__('TestKubeMQBroker') returns the testing broker class."""
    import kubemq_faststream

    cls = kubemq_faststream.TestKubeMQBroker
    from kubemq_faststream.testing import TestKubeMQBroker

    assert cls is TestKubeMQBroker


def test_getattr_unknown_raises():
    """__getattr__ raises AttributeError for unknown names."""
    import kubemq_faststream

    with pytest.raises(AttributeError, match="no attribute 'NonExistentThing'"):
        _ = kubemq_faststream.NonExistentThing


def test_version_exists():
    """__version__ is set (either from package metadata or dev fallback)."""
    import kubemq_faststream

    assert isinstance(kubemq_faststream.__version__, str)
    assert len(kubemq_faststream.__version__) > 0


def test_all_exports():
    """__all__ contains expected public names."""
    import kubemq_faststream

    assert "KubeMQBroker" in kubemq_faststream.__all__
    assert "KubeMQMessage" in kubemq_faststream.__all__
    assert "KubeMQPattern" in kubemq_faststream.__all__
    assert "KubeMQPublishCommand" in kubemq_faststream.__all__
    assert "KubeMQPublisher" in kubemq_faststream.__all__
    assert "KubeMQRouter" in kubemq_faststream.__all__
    assert "TestKubeMQBroker" in kubemq_faststream.__all__
    assert "AckPolicy" in kubemq_faststream.__all__
    assert "StartPosition" in kubemq_faststream.__all__
    assert "__version__" in kubemq_faststream.__all__


def test_ensure_kubemq_raises_when_not_installed():
    """_ensure_kubemq raises ImportError when kubemq SDK not available."""
    import kubemq_faststream

    with patch.dict("sys.modules", {"kubemq": None}):
        with pytest.raises(ImportError, match="kubemq-faststream requires"):
            kubemq_faststream._ensure_kubemq()


def test_directly_imported_enums():
    """AckPolicy, KubeMQPattern, StartPosition are directly importable."""
    from kubemq_faststream import AckPolicy, KubeMQPattern, StartPosition

    assert KubeMQPattern.EVENTS == "events"
    assert StartPosition.START_FROM_NEW == "start_from_new"
    # AckPolicy is re-exported from faststream
    assert hasattr(AckPolicy, "ACK")

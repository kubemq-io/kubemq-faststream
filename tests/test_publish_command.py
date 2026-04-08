"""KubeMQPublishCommand construction, from_cmd() tests.

Tests: U-20 through U-22.
"""

from __future__ import annotations

import pytest

from faststream.response import PublishCommand, PublishType

from kubemq_faststream.response import KubeMQPublishCommand
from kubemq_faststream.schemas import KubeMQPattern


# U-20: test_init_delegates_to_base
def test_init_delegates_to_base():
    """KubeMQPublishCommand.__init__ sets all fields via explicit init (H-3)."""
    cmd = KubeMQPublishCommand(
        b"hello",
        destination="test-channel",
        pattern=KubeMQPattern.EVENTS,
        metadata="test-meta",
        timeout=10,
        cache_key="ckey",
        cache_ttl=300,
        correlation_id="corr-123",
        headers={"key": "value"},
        reply_to="reply-ch",
    )

    assert cmd.destination == "test-channel"
    assert cmd.pattern == KubeMQPattern.EVENTS
    assert cmd.metadata == "test-meta"
    assert cmd.timeout == 10
    assert cmd.cache_key == "ckey"
    assert cmd.cache_ttl == 300
    assert cmd.correlation_id == "corr-123"
    assert cmd.headers == {"key": "value"}
    assert cmd.reply_to == "reply-ch"


def test_init_defaults():
    """Default values are applied when optional fields are omitted."""
    cmd = KubeMQPublishCommand(
        b"test",
        pattern=KubeMQPattern.QUEUES,
    )
    assert cmd.destination == ""
    assert cmd.metadata == ""
    assert cmd.timeout is None
    assert cmd.cache_key is None
    assert cmd.cache_ttl is None
    assert cmd._batch_bodies is None


# U-21: test_from_cmd_generic_publish_command
def test_from_cmd_generic_publish_command():
    """from_cmd() wraps a generic PublishCommand as KubeMQPublishCommand."""
    generic_cmd = PublishCommand(
        b"generic-body",
        destination="dest-channel",
        correlation_id="corr-456",
        headers={"h1": "v1"},
        reply_to="rp",
        _publish_type=PublishType.PUBLISH,
    )
    kubemq_cmd = KubeMQPublishCommand.from_cmd(
        generic_cmd, default_pattern=KubeMQPattern.QUEUES
    )

    assert isinstance(kubemq_cmd, KubeMQPublishCommand)
    assert kubemq_cmd.pattern == KubeMQPattern.QUEUES
    assert kubemq_cmd.destination == "dest-channel"
    assert kubemq_cmd.correlation_id == "corr-456"
    assert kubemq_cmd.headers == {"h1": "v1"}
    assert kubemq_cmd.reply_to == "rp"


# U-22: test_from_cmd_already_kubemq
def test_from_cmd_already_kubemq():
    """from_cmd() returns the same object if already KubeMQPublishCommand."""
    original = KubeMQPublishCommand(
        b"body",
        destination="ch",
        pattern=KubeMQPattern.COMMANDS,
    )
    result = KubeMQPublishCommand.from_cmd(
        original, default_pattern=KubeMQPattern.EVENTS
    )

    assert result is original
    # Pattern should NOT be overridden by default_pattern
    assert result.pattern == KubeMQPattern.COMMANDS


def test_batch_bodies_explicit():
    """Explicit batch_bodies are returned by the property."""
    cmd = KubeMQPublishCommand(
        b"",
        pattern=KubeMQPattern.QUEUES,
        batch_bodies=(b"a", b"b", b"c"),
    )
    assert cmd.batch_bodies == (b"a", b"b", b"c")


def test_batch_bodies_fallback():
    """Without explicit batch_bodies, falls back to parent behavior."""
    cmd = KubeMQPublishCommand(
        b"body",
        pattern=KubeMQPattern.QUEUES,
    )
    # _batch_bodies is None, so it falls back to super().batch_bodies
    assert cmd._batch_bodies is None

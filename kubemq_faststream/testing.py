"""Testing utilities -- TestKubeMQBroker, FakeProducer, build_message."""

from __future__ import annotations

import json
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

from faststream._internal.testing.broker import TestBroker, change_producer
from faststream.message import encode_message
from typing_extensions import override

from kubemq_faststream.broker import KubeMQBroker
from kubemq_faststream.config import KubeMQConnection
from kubemq_faststream.message import KubeMQRawMessage
from kubemq_faststream.producer import KubeMQProducer
from kubemq_faststream.response import KubeMQPublishCommand
from kubemq_faststream.schemas import FeatureNotSupportedException, KubeMQPattern
from kubemq_faststream.subscriber.queues import BatchQueuesSubscriber


class TestKubeMQBroker(TestBroker[KubeMQBroker]):  # type: ignore[type-var]
    """Test context manager for KubeMQ broker.

    Usage::

        async with TestKubeMQBroker(broker) as br:
            await br.publish({"key": "value"}, queues="test")
    """

    __test__ = False

    @contextmanager
    def _patch_producer(self, broker: KubeMQBroker) -> Iterator[None]:
        """Replace broker's producer with FakeProducer."""
        fake = FakeProducer(broker)

        with ExitStack() as es:
            es.enter_context(
                change_producer(broker.config.broker_config, fake),
            )
            yield

    @staticmethod
    async def _fake_connect(broker: KubeMQBroker, *args: Any, **kwargs: Any) -> Any:
        """Provide a mock connection without connecting to real broker."""
        return _build_mock_connection()

    @staticmethod
    def create_publisher_fake_subscriber(
        broker: KubeMQBroker,
        publisher: Any,
    ) -> tuple[Any, bool]:
        """Find or create a subscriber matching the publisher for testing."""
        for sub in broker.subscribers:
            if _is_handler_matches(sub, publisher.channel, publisher.pattern):
                return sub, True
        sub = broker.subscriber(**{publisher.pattern.value: publisher.channel})
        return sub, False


class FakeProducer(KubeMQProducer):
    """In-memory message router for testing.

    Routes publish/request calls to matching subscriber handlers in tests.
    """

    def __init__(self, broker: KubeMQBroker) -> None:
        super().__init__(connection=None)
        self.broker = broker

    def __bool__(self) -> bool:
        return True

    @override
    async def publish(self, cmd: KubeMQPublishCommand) -> Any:
        """Route a publish command to the matching subscriber."""
        incoming = build_message(
            body=cmd.body,
            channel=cmd.destination,
            pattern=cmd.pattern,
            headers=cmd.headers,
            metadata=getattr(cmd, "metadata", ""),
            correlation_id=cmd.correlation_id,
            reply_to=cmd.reply_to,
        )
        for handler in _find_handler(self.broker.subscribers, cmd.destination, cmd.pattern):
            return await handler.process_message(incoming)
        return None

    @override
    async def request(self, cmd: KubeMQPublishCommand) -> Any:
        """Route a request command to the matching subscriber."""
        if cmd.pattern not in (KubeMQPattern.COMMANDS, KubeMQPattern.QUERIES):
            raise FeatureNotSupportedException(
                f"Request not supported for {cmd.pattern.value} pattern"
            )
        return await self.publish(cmd)

    @override
    async def publish_batch(self, cmd: KubeMQPublishCommand) -> Any:
        """Route a batch publish to the matching subscriber."""
        if cmd.pattern != KubeMQPattern.QUEUES:
            raise FeatureNotSupportedException(
                f"Batch publish not supported for {cmd.pattern.value} pattern"
            )
        if not cmd.batch_bodies:
            return None
        for handler in _find_handler(self.broker.subscribers, cmd.destination, cmd.pattern):
            if isinstance(handler, BatchQueuesSubscriber):
                batch = [
                    build_message(body=body, channel=cmd.destination, pattern=cmd.pattern)
                    for body in cmd.batch_bodies
                ]
                return await handler.process_message(batch)
            else:
                for body in cmd.batch_bodies:
                    incoming = build_message(
                        body=body, channel=cmd.destination, pattern=cmd.pattern
                    )
                    await handler.process_message(incoming)
        return None


def _build_mock_connection() -> KubeMQConnection:
    """Build a mock KubeMQConnection with all methods as AsyncMock."""
    mock_pubsub = MagicMock()
    mock_pubsub.ping = AsyncMock()
    mock_pubsub.close = AsyncMock()
    mock_pubsub.connect = AsyncMock()
    mock_pubsub.publish_event = AsyncMock()
    mock_pubsub.send_event_store = AsyncMock()

    async def _empty_async_iter():
        return
        yield  # noqa: F811 -- makes this an async generator

    mock_pubsub.subscribe_to_events = AsyncMock(return_value=_empty_async_iter())
    mock_pubsub.subscribe_to_events_store = AsyncMock(return_value=_empty_async_iter())

    mock_queues = MagicMock()
    mock_queues.close = AsyncMock()
    mock_queues.connect = AsyncMock()
    mock_queues.send_queue_message = AsyncMock()
    mock_queues.receive_queue_messages = AsyncMock()

    mock_cq = MagicMock()
    mock_cq.close = AsyncMock()
    mock_cq.connect = AsyncMock()
    mock_cq.config = MagicMock()
    mock_cq.config.client_id = "test-client"
    mock_cq.send_command = AsyncMock()
    mock_cq.send_query = AsyncMock()
    mock_cq.send_response = AsyncMock()
    mock_cq.subscribe_to_commands = AsyncMock(return_value=_empty_async_iter())
    mock_cq.subscribe_to_queries = AsyncMock(return_value=_empty_async_iter())

    return KubeMQConnection(pubsub=mock_pubsub, queues=mock_queues, cq=mock_cq)


def build_message(
    body: bytes | str | dict | Any,
    channel: str,
    pattern: KubeMQPattern,
    *,
    headers: dict[str, str] | None = None,
    metadata: str = "",
    correlation_id: str | None = None,
    reply_to: str | None = None,
    message_id: str | None = None,
    serializer: Any | None = None,
) -> KubeMQRawMessage:
    """Construct a broker-native message for testing."""
    content_type = "application/json"
    if isinstance(body, str):
        encoded_body = body.encode("utf-8")
    elif isinstance(body, bytes):
        encoded_body = body
    elif serializer is not None:
        encoded_body, ct = encode_message(body, serializer)
        if ct is not None:
            content_type = ct
    else:
        try:
            encoded_body = json.dumps(body).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise TypeError(
                f"Cannot serialize body of type {type(body).__name__}. "
                "Provide a serializer or pass bytes/str/dict."
            ) from exc

    return KubeMQRawMessage(
        body=encoded_body,
        channel=channel,
        pattern=pattern,
        headers=headers or {},
        correlation_id=correlation_id or uuid4().hex,
        reply_to=reply_to or "",
        message_id=message_id or uuid4().hex,
        content_type=content_type,
        metadata=metadata,
        timestamp=datetime.now(tz=UTC),
    )


def _find_handler(
    subscribers: list[Any],
    channel: str,
    pattern: KubeMQPattern,
) -> Iterator[Any]:
    """Find all subscribers matching a channel and pattern."""
    for sub in subscribers:
        if _is_handler_matches(sub, channel, pattern):
            yield sub


def _is_handler_matches(
    handler: Any,
    channel: str,
    pattern: KubeMQPattern,
) -> bool:
    """Check if handler matches the given channel and pattern."""
    if getattr(handler, "pattern", None) != pattern:
        return False
    sub_channel = getattr(handler, "channel", "")
    if sub_channel == channel:
        return True
    return _match_wildcard(sub_channel, channel)


def _match_wildcard(pattern_str: str, channel: str) -> bool:
    """Simple wildcard matching (supports * and >)."""
    if pattern_str == channel:
        return True
    if pattern_str.endswith(".*"):
        prefix = pattern_str[:-2]
        return channel.startswith(prefix + ".") and "." not in channel[len(prefix) + 1 :]
    if pattern_str.endswith(".>"):
        prefix = pattern_str[:-2]
        return channel.startswith(prefix + ".")
    return False

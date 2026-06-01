"""KubeMQ producer -- routes messages to the correct SDK client."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from faststream.message import encode_message
from kubemq.cq.command_message import CommandMessage
from kubemq.cq.query_message import QueryMessage
from kubemq.pubsub.event_message import EventMessage
from kubemq.pubsub.event_store_message import EventStoreMessage
from kubemq.queues.queues_message import QueueMessage

from kubemq_faststream.schemas import FeatureNotSupportedException, KubeMQPattern

if TYPE_CHECKING:
    from kubemq_faststream.config import KubeMQConnection
    from kubemq_faststream.response import KubeMQPublishCommand

logger = logging.getLogger("kubemq_faststream")


class KubeMQProducer:
    """Implements wire-level message sending, routing by KubeMQPattern."""

    @staticmethod
    async def _parse_identity(msg: Any) -> Any:
        """Identity parser -- returns message as-is."""
        return msg

    @staticmethod
    async def _decode_identity(msg: Any) -> Any:
        """Identity decoder -- returns message as-is."""
        return msg

    def __init__(self, connection: KubeMQConnection | None = None) -> None:
        self._connection = connection
        # ProducerProto requires _parser and _decoder attributes (AsyncCallable).
        # KubeMQ handles parsing in the subscriber, so these are async identity
        # functions used only when _basic_request processes the response.
        self._parser = self._parse_identity
        self._decoder = self._decode_identity

    async def publish(self, cmd: KubeMQPublishCommand) -> Any:
        """Publish a message, routing by pattern."""
        conn = self._connection
        if conn is None:
            raise RuntimeError("Producer not connected")

        # M-10: encode message before sending
        body, content_type = encode_message(cmd.body, None)

        match cmd.pattern:
            case KubeMQPattern.EVENTS:
                em_kwargs: dict[str, Any] = {
                    "channel": cmd.destination,
                    "body": body,
                    "tags": cmd.headers or {},
                    "metadata": cmd.metadata,
                }
                if cmd.message_id is not None:
                    em_kwargs["id"] = cmd.message_id
                return await conn.pubsub.publish_event(EventMessage(**em_kwargs))
            case KubeMQPattern.EVENTS_STORE:
                esm_kwargs: dict[str, Any] = {
                    "channel": cmd.destination,
                    "body": body,
                    "tags": cmd.headers or {},
                    "metadata": cmd.metadata,
                }
                if cmd.message_id is not None:
                    esm_kwargs["id"] = cmd.message_id
                return await conn.pubsub.send_event_store(EventStoreMessage(**esm_kwargs))
            case KubeMQPattern.QUEUES:
                qm_kwargs: dict[str, Any] = {
                    "channel": cmd.destination,
                    "body": body,
                    "tags": cmd.headers or {},
                    "metadata": cmd.metadata,
                    "delay_in_seconds": cmd.delay_in_seconds,
                    "expiration_in_seconds": cmd.expiration_in_seconds,
                    "max_receive_count": cmd.max_receive_count,
                    "max_receive_queue": cmd.max_receive_queue,
                }
                if cmd.message_id is not None:
                    qm_kwargs["id"] = cmd.message_id
                return await conn.queues.send_queue_message(QueueMessage(**qm_kwargs))
            case KubeMQPattern.COMMANDS:
                cm_kwargs: dict[str, Any] = {
                    "channel": cmd.destination,
                    "body": body,
                    "tags": cmd.headers or {},
                    "metadata": cmd.metadata,
                    "timeout_in_seconds": cmd.timeout or 30,
                }
                if cmd.message_id is not None:
                    cm_kwargs["id"] = cmd.message_id
                return await conn.cq.send_command(CommandMessage(**cm_kwargs))
            case KubeMQPattern.QUERIES:
                query_kwargs: dict[str, Any] = {
                    "channel": cmd.destination,
                    "body": body,
                    "tags": cmd.headers or {},
                    "metadata": cmd.metadata,
                    "timeout_in_seconds": cmd.timeout or 30,
                    "cache_key": cmd.cache_key or "",
                    "cache_ttl_in_seconds": cmd.cache_ttl or 0,
                }
                if cmd.message_id is not None:
                    query_kwargs["id"] = cmd.message_id
                return await conn.cq.send_query(QueryMessage(**query_kwargs))

    async def request(self, cmd: KubeMQPublishCommand) -> Any:
        """Send a request (commands/queries only)."""
        if cmd.pattern not in (KubeMQPattern.COMMANDS, KubeMQPattern.QUERIES):
            raise FeatureNotSupportedException(f"request() not supported for {cmd.pattern.value}")
        return await self.publish(cmd)

    async def publish_batch(self, cmd: KubeMQPublishCommand) -> Any:
        """Publish a batch of queue messages."""
        conn = self._connection
        if conn is None:
            raise RuntimeError("Producer not connected")
        if cmd.pattern != KubeMQPattern.QUEUES:
            raise FeatureNotSupportedException(
                f"publish_batch() only supported for queues, not {cmd.pattern.value}"
            )
        if not cmd.batch_bodies:
            return None
        messages = [
            QueueMessage(
                channel=cmd.destination,
                body=body,
                tags=cmd.headers or {},
                metadata=cmd.metadata,
                delay_in_seconds=cmd.delay_in_seconds,
                expiration_in_seconds=cmd.expiration_in_seconds,
                max_receive_count=cmd.max_receive_count,
                max_receive_queue=cmd.max_receive_queue,
            )
            for body in cmd.batch_bodies
        ]
        return await conn.queues.send_queue_messages_batch(messages)

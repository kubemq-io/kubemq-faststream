"""KubeMQ raw message and StreamMessage wrapper."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any

from faststream.message import StreamMessage

from kubemq_faststream.schemas import KubeMQPattern

if TYPE_CHECKING:
    from kubemq.queues.queues_message_received import QueueMessageReceived


@dataclass(frozen=True)
class KubeMQRawMessage:
    """Internal raw message representation for all KubeMQ patterns."""

    body: bytes
    channel: str
    pattern: KubeMQPattern
    headers: dict[str, str]
    correlation_id: str
    reply_to: str
    message_id: str
    content_type: str
    metadata: str
    timestamp: datetime
    sequence: int = 0
    cache_hit: bool = False
    receive_count: int = 0
    from_client_id: str = ""
    delayed_to: datetime | None = None
    expired_at: datetime | None = None
    re_route_from_queue: str = ""
    queue_msg: Any = field(default=None, repr=False)

    def __repr__(self) -> str:
        body_repr = f"<{len(self.body)} bytes>"
        return (
            f"KubeMQRawMessage(channel={self.channel!r}, pattern={self.pattern.value!r}, "
            f"body={body_repr}, message_id={self.message_id!r})"
        )


class KubeMQMessage(StreamMessage[KubeMQRawMessage]):
    """Wraps KubeMQ raw messages with ack/nack/reject support.

    Acknowledgement semantics by pattern:
      - Events/EventsStore: ack/nack/reject are no-ops (fire-and-forget).
      - Queues: ack() confirms processing, nack() requeues for retry,
        reject() sends negative acknowledgement (server-configured disposition).
      - Commands/Queries: response handling is automatic via the subscriber.

    SkipMessage on Queues: when FastStream's AcknowledgementMiddleware suppresses
    a SkipMessage, the message disposition depends on the configured AckPolicy.
    Under NACK_ON_ERROR, the message is requeued. Under ACK_FIRST, it was already
    acknowledged before the handler ran.
    """

    _queue_msg: QueueMessageReceived | None = None

    def __init__(
        self,
        *,
        raw_message: KubeMQRawMessage,
        body: bytes,
        headers: dict[str, Any] | None = None,
        reply_to: str = "",
        correlation_id: str | None = None,
        message_id: str | None = None,
        content_type: str | None = None,
        queue_msg: QueueMessageReceived | None = None,
    ) -> None:
        super().__init__(
            raw_message=raw_message,
            body=body,
            headers=headers,
            reply_to=reply_to,
            correlation_id=correlation_id,
            message_id=message_id,
            content_type=content_type,
        )
        self._queue_msg = queue_msg

    async def ack(self) -> None:
        if self.committed is not None:
            return
        if self.raw_message.pattern == KubeMQPattern.QUEUES and self._queue_msg:
            await self._queue_msg.async_ack()
        await super().ack()

    async def nack(self) -> None:
        """Requeue the message (FastStream nack = requeue for retry)."""
        if self.committed is not None:
            return
        if self.raw_message.pattern == KubeMQPattern.QUEUES and self._queue_msg:
            await self._queue_msg.async_re_queue(self.raw_message.channel)
        await super().nack()

    async def reject(self) -> None:
        """Send negative acknowledgement. Final disposition is server-configured.

        Maps to KubeMQ SDK async_nack() which tells the server the message was
        not successfully processed. The server may dead-letter, discard, or
        requeue based on queue policy configuration.
        """
        if self.committed is not None:
            return
        if self.raw_message.pattern == KubeMQPattern.QUEUES and self._queue_msg:
            await self._queue_msg.async_nack()
        await super().reject()

"""SDK message -> StreamMessage converter."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from faststream.message import StreamMessage, decode_message

from kubemq_faststream.message import KubeMQMessage, KubeMQRawMessage
from kubemq_faststream.schemas import KubeMQPattern

if TYPE_CHECKING:
    from kubemq.cq.command_message_received import CommandReceived
    from kubemq.cq.query_message_received import QueryReceived
    from kubemq.pubsub.event_message_received import EventReceived
    from kubemq.pubsub.event_store_message_received import EventStoreReceived
    from kubemq.queues.queues_message_received import QueueMessageReceived


class KubeMQParser:
    """Converts KubeMQ SDK message types to KubeMQRawMessage and StreamMessage."""

    async def parse_message(self, raw: KubeMQRawMessage) -> StreamMessage[KubeMQRawMessage]:
        return KubeMQMessage(
            raw_message=raw,
            body=raw.body,
            headers=dict(raw.headers),  # M-6: copy dict to prevent mutation
            correlation_id=raw.correlation_id,
            reply_to=raw.reply_to,
            message_id=raw.message_id,
            content_type=raw.content_type,
            queue_msg=raw.queue_msg,
        )

    async def decode_message(self, msg: StreamMessage[KubeMQRawMessage]) -> Any:
        """Decode message body using FastStream's standard decoder (auto-detects JSON)."""
        return decode_message(msg)

    @staticmethod
    def from_event_received(event: EventReceived) -> KubeMQRawMessage:
        return KubeMQRawMessage(
            body=event.body or b"",
            channel=event.channel,
            pattern=KubeMQPattern.EVENTS,
            headers=event.tags or {},
            correlation_id=event.id or "",
            reply_to="",
            message_id=event.id or "",
            content_type="",
            metadata=event.metadata or "",
            timestamp=event.timestamp or datetime.now(tz=UTC),
        )

    @staticmethod
    def from_event_store_received(event: EventStoreReceived) -> KubeMQRawMessage:
        return KubeMQRawMessage(
            body=event.body or b"",
            channel=event.channel,
            pattern=KubeMQPattern.EVENTS_STORE,
            headers=event.tags or {},
            correlation_id=event.id or "",
            reply_to="",
            message_id=event.id or "",
            content_type="",
            metadata=event.metadata or "",
            timestamp=event.timestamp or datetime.now(tz=UTC),
            sequence=event.sequence,
        )

    @staticmethod
    def from_queue_message_received(
        msg: QueueMessageReceived,
    ) -> KubeMQRawMessage:
        ts = msg.timestamp if msg.timestamp is not None else datetime.now(tz=UTC)
        if isinstance(ts, datetime) and ts.tzinfo is None:
            ts = ts.replace(tzinfo=UTC)

        def _normalize_optional_dt(val: datetime | None) -> datetime | None:
            if not isinstance(val, datetime):
                return None
            ts_value = val.timestamp() if val.tzinfo else val.replace(tzinfo=UTC).timestamp()
            if ts_value == 0.0:
                return None
            return val if val.tzinfo else val.replace(tzinfo=UTC)

        delayed = _normalize_optional_dt(msg.delayed_to)
        expired = _normalize_optional_dt(msg.expired_at)

        return KubeMQRawMessage(
            body=msg.body or b"",
            channel=msg.channel,
            pattern=KubeMQPattern.QUEUES,
            headers=msg.tags or {},
            correlation_id=msg.id or "",
            reply_to="",
            message_id=msg.id or "",
            content_type="",
            metadata=msg.metadata or "",
            timestamp=ts,
            sequence=getattr(msg, "sequence", 0),
            receive_count=msg.receive_count,
            from_client_id=msg.from_client_id or "",
            delayed_to=delayed,
            expired_at=expired,
            re_route_from_queue=msg.re_route_from_queue or "",
            queue_msg=msg,
        )

    @staticmethod
    def from_command_received(cmd: CommandReceived) -> KubeMQRawMessage:
        return KubeMQRawMessage(
            body=cmd.body or b"",
            channel=cmd.channel,
            pattern=KubeMQPattern.COMMANDS,
            headers=cmd.tags or {},
            correlation_id=cmd.id or "",
            reply_to=cmd.reply_channel or "",
            message_id=cmd.id or "",
            content_type="",
            metadata=cmd.metadata or "",
            timestamp=cmd.timestamp or datetime.now(tz=UTC),
        )

    @staticmethod
    def from_query_received(query: QueryReceived) -> KubeMQRawMessage:
        return KubeMQRawMessage(
            body=query.body or b"",
            channel=query.channel,
            pattern=KubeMQPattern.QUERIES,
            headers=query.tags or {},
            correlation_id=query.id or "",
            reply_to=query.reply_channel or "",
            message_id=query.id or "",
            content_type="",
            metadata=query.metadata or "",
            timestamp=query.timestamp or datetime.now(tz=UTC),
        )

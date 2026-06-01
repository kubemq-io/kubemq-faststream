"""KubeMQ publish command extending FastStream's PublishCommand."""

from __future__ import annotations

from typing import Any

from faststream.response import PublishCommand, PublishType

from kubemq_faststream.schemas import KubeMQPattern


class KubeMQPublishCommand(PublishCommand):
    """Extends FastStream's PublishCommand with KubeMQ pattern routing.

    Uses explicit __init__ instead of @dataclass inheritance to avoid
    the dataclass MRO conflict with PublishCommand (H-3 fix).
    """

    pattern: KubeMQPattern
    metadata: str
    timeout: int | None
    cache_key: str | None
    cache_ttl: int | None
    _batch_bodies: tuple[bytes, ...] | None
    delay_in_seconds: int
    expiration_in_seconds: int
    max_receive_count: int
    max_receive_queue: str
    message_id: str | None

    def __init__(
        self,
        message: Any = None,
        *,
        destination: str = "",
        pattern: KubeMQPattern,
        metadata: str = "",
        timeout: int | None = None,
        cache_key: str | None = None,
        cache_ttl: int | None = None,
        batch_bodies: tuple[bytes, ...] | None = None,
        correlation_id: str | None = None,
        headers: dict[str, Any] | None = None,
        reply_to: str = "",
        _publish_type: PublishType = PublishType.PUBLISH,
        delay_in_seconds: int = 0,
        expiration_in_seconds: int = 0,
        max_receive_count: int = 0,
        max_receive_queue: str = "",
        message_id: str | None = None,
    ) -> None:
        super().__init__(
            message,
            destination=destination,
            correlation_id=correlation_id,
            headers=headers or {},
            reply_to=reply_to,
            _publish_type=_publish_type,
        )
        self.pattern = pattern
        self.metadata = metadata
        self.timeout = timeout
        self.cache_key = cache_key
        self.cache_ttl = cache_ttl
        self._batch_bodies = batch_bodies
        self.delay_in_seconds = delay_in_seconds
        self.expiration_in_seconds = expiration_in_seconds
        self.max_receive_count = max_receive_count
        self.max_receive_queue = max_receive_queue
        self.message_id = message_id

    @property  # type: ignore[override]
    def batch_bodies(self) -> tuple[bytes, ...]:
        """Return explicit batch bodies, or fall back to parent behavior."""
        if self._batch_bodies is not None:
            return self._batch_bodies
        return super().batch_bodies

    @classmethod
    def from_cmd(  # type: ignore[override]
        cls, cmd: PublishCommand, *, default_pattern: KubeMQPattern
    ) -> KubeMQPublishCommand:
        """Convert a generic PublishCommand to KubeMQPublishCommand."""
        if isinstance(cmd, KubeMQPublishCommand):
            return cmd
        return cls(
            message=cmd.body,
            destination=cmd.destination,
            pattern=default_pattern,
            correlation_id=cmd.correlation_id,
            headers=cmd.headers,
            reply_to=cmd.reply_to,
            _publish_type=cmd.publish_type,
        )

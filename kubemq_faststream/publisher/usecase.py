"""KubeMQ publisher -- extends FastStream PublisherUsecase."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from faststream._internal.endpoint.publisher.usecase import PublisherUsecase

from kubemq_faststream.response import KubeMQPublishCommand

if TYPE_CHECKING:
    from faststream._internal.endpoint.publisher import PublisherSpecification

    from kubemq_faststream.publisher.config import KubeMQPublisherConfig


class KubeMQPublisher(PublisherUsecase):
    """Publisher for KubeMQ channels.

    Supports both programmatic publish() and decorator usage:
        @broker.publisher(events="results")
        @broker.subscriber(events="input")
        async def handle(msg) -> dict:
            return {"processed": True}  # auto-published to "results"
    """

    def __init__(
        self,
        config: KubeMQPublisherConfig,
        specification: PublisherSpecification[Any, Any],
    ) -> None:
        super().__init__(config, specification)
        self.channel = config.channel
        self.pattern = config.pattern
        self.headers: dict[str, Any] = {}

    async def publish(
        self,
        message: Any,
        *,
        channel: str | None = None,
        **kwargs: Any,
    ) -> Any:
        """Publish a message to this publisher's channel."""
        cmd = KubeMQPublishCommand(
            message,
            destination=channel or self.channel,
            pattern=self.pattern,
            **kwargs,
        )
        return await self._basic_publish(
            cmd,
            producer=self._outer_config.producer,
            _extra_middlewares=(),
        )

    async def request(
        self,
        message: Any,
        *,
        channel: str | None = None,
        **kwargs: Any,
    ) -> Any:
        """Send a request (command/query) through this publisher's channel."""
        cmd = KubeMQPublishCommand(
            message,
            destination=channel or self.channel,
            pattern=self.pattern,
            **kwargs,
        )
        return await self._basic_request(
            cmd,
            producer=self._outer_config.producer,
        )

    async def _publish(
        self,
        cmd: Any,
        *,
        _extra_middlewares: Any = (),
    ) -> None:
        """Internal publish called by FastStream when handler returns a value."""
        kubemq_cmd = KubeMQPublishCommand.from_cmd(cmd, default_pattern=self.pattern)
        kubemq_cmd.add_headers(self.headers, override=False)
        await self._basic_publish(
            kubemq_cmd,
            producer=self._outer_config.producer,
            _extra_middlewares=_extra_middlewares,
        )

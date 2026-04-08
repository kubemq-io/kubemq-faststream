"""Commands subscriber -- request-reply with void response."""

from __future__ import annotations

import logging
from typing import Any

import anyio

from kubemq_faststream.subscriber.usecase import KubeMQSubscriber

logger = logging.getLogger("kubemq_faststream")


class CommandsSubscriber(KubeMQSubscriber):
    """Subscribe to KubeMQ Commands (request-reply, void response)."""

    def __init__(
        self,
        config: Any,
        specification: Any,
        calls: Any,
    ) -> None:
        super().__init__(config, specification, calls)

    async def _consume(self) -> None:
        """Consume loop: receive commands, run handler, send response."""
        from kubemq.cq.command_response_message import CommandResponse
        from kubemq.cq.commands_subscription import CommandsSubscription

        from kubemq_faststream.parser import KubeMQParser

        subscription = CommandsSubscription(
            channel=self.channel,
            group=self.group or "",
            on_receive_command_callback=lambda _: None,
        )
        cq_client = self._connection.cq  # type: ignore[union-attr]
        client_id = cq_client.config.client_id

        while self.running:
            try:
                async for cmd_received in cq_client.subscribe_to_commands(subscription):
                    if not self.running:
                        break
                    try:
                        raw = KubeMQParser.from_command_received(cmd_received)
                        await self.consume(raw)

                        # Send success response (unless no_reply)
                        if not self._no_reply:
                            response = CommandResponse(
                                command_received=cmd_received,
                                client_id=client_id,  # type: ignore[arg-type]
                                request_id=cmd_received.id,
                                is_executed=True,
                            )
                            await cq_client.send_response(response)
                    except anyio.get_cancelled_exc_class():
                        raise
                    except Exception:
                        logger.exception("Command handler error on channel %s", self.channel)
                        # Send sanitized error response (H-6)
                        if not self._no_reply:
                            try:
                                error_response = CommandResponse(
                                    command_received=cmd_received,
                                    client_id=client_id,  # type: ignore[arg-type]
                                    request_id=cmd_received.id,
                                    is_executed=False,
                                    error="handler execution failed",
                                )
                                await cq_client.send_response(error_response)
                            except Exception:
                                logger.exception(
                                    "Failed to send error response for %s",
                                    self.channel,
                                )
            except anyio.get_cancelled_exc_class():
                raise
            except Exception:
                if self.running:
                    logger.exception(
                        "CQ subscription interrupted on %s, resubscribing",
                        self.channel,
                    )
                    await anyio.sleep(1)

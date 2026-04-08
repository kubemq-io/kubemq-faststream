"""Queues subscriber -- point-to-point with transactional settlement."""

from __future__ import annotations

import logging
from typing import Any

import anyio
from faststream.middlewares import AckPolicy

from kubemq_faststream.subscriber.usecase import KubeMQSubscriber

logger = logging.getLogger("kubemq_faststream")


class QueuesSubscriber(KubeMQSubscriber):
    """Subscribe to KubeMQ Queues via downstream stream API.

    Uses QueuesDownstream bidirectional streaming for transactional
    Get -> Ack/NAck/ReQueue settlement per message batch.
    """

    max_messages: int
    wait_timeout: int

    def __init__(
        self,
        config: Any,
        specification: Any,
        calls: Any,
    ) -> None:
        super().__init__(config, specification, calls)
        self.max_messages = config.max_messages
        self.wait_timeout = config.wait_timeout
        if self.max_messages <= 0:
            raise ValueError("max_messages must be > 0")
        if self.wait_timeout < 0:
            raise ValueError("wait_timeout must be >= 0")

    async def _consume(self) -> None:
        """Poll loop using downstream stream API with AckPolicy enforcement."""
        from kubemq_faststream.parser import KubeMQParser

        while self.running:
            try:
                response = await self._connection.queues.receive_queue_messages(  # type: ignore[union-attr]
                    channel=self.channel,
                    max_messages=self.max_messages,
                    wait_timeout_seconds=self.wait_timeout,
                    auto_ack=(self.ack_policy == AckPolicy.ACK_FIRST),
                )
                if response.is_error:
                    logger.error(
                        "Queue poll error on %s: %s",
                        self.channel,
                        response.error,
                    )
                    await anyio.sleep(1)
                    continue
                if not response.messages:
                    continue
                for queue_msg in response.messages:
                    if not self.running:
                        break
                    try:
                        raw = KubeMQParser.from_queue_message_received(queue_msg)
                        await self.consume(raw)
                    except anyio.get_cancelled_exc_class():
                        raise
                    except Exception:
                        logger.exception("Queue handler error on channel %s", self.channel)
            except anyio.get_cancelled_exc_class():
                raise
            except Exception:
                logger.exception("Queue consume error on channel %s", self.channel)
                await anyio.sleep(1)


class BatchQueuesSubscriber(KubeMQSubscriber):
    """Subscribe to KubeMQ Queues with batch polling.

    Batch ack semantics: when the handler succeeds, all messages in the batch
    are acknowledged atomically. When the handler raises, all messages are
    requeued atomically.
    """

    max_messages: int
    wait_timeout: int

    def __init__(
        self,
        config: Any,
        specification: Any,
        calls: Any,
    ) -> None:
        super().__init__(config, specification, calls)
        self.max_messages = config.max_messages
        self.wait_timeout = config.wait_timeout
        if self.max_messages <= 0:
            raise ValueError("max_messages must be > 0")
        if self.wait_timeout < 0:
            raise ValueError("wait_timeout must be >= 0")

    async def _consume(self) -> None:
        """Poll loop: receive batch, run handler on batch, settle all."""
        from kubemq_faststream.parser import KubeMQParser

        while self.running:
            try:
                response = await self._connection.queues.receive_queue_messages(  # type: ignore[union-attr]
                    channel=self.channel,
                    max_messages=self.max_messages,
                    wait_timeout_seconds=self.wait_timeout,
                    auto_ack=False,
                )
                if response.is_error:
                    logger.error(
                        "Batch queue poll error on %s: %s",
                        self.channel,
                        response.error,
                    )
                    await anyio.sleep(1)
                    continue
                if not response.messages:
                    continue
                # Build batch of raw messages
                batch_raw = []
                for queue_msg in response.messages:
                    raw = KubeMQParser.from_queue_message_received(queue_msg)
                    batch_raw.append(raw)
                try:
                    # Consume each message in the batch through the pipeline
                    for raw in batch_raw:
                        await self.consume(raw)
                    # Batch success: ack all
                    for raw in batch_raw:
                        try:
                            await raw.queue_msg.async_ack()
                        except Exception:
                            logger.exception("Failed to ack queue message on %s", self.channel)
                except anyio.get_cancelled_exc_class():
                    raise
                except Exception:
                    logger.exception(
                        "Batch handler error on channel %s, requeuing batch",
                        self.channel,
                    )
                    # Batch failure: requeue all
                    for raw in batch_raw:
                        try:
                            await raw.queue_msg.async_re_queue(self.channel)
                        except Exception:
                            logger.exception("Failed to requeue message on %s", self.channel)
            except anyio.get_cancelled_exc_class():
                raise
            except Exception:
                logger.exception("Batch queue consume error on channel %s", self.channel)
                await anyio.sleep(1)

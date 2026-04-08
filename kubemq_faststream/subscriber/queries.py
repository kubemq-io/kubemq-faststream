"""Queries subscriber -- request-reply with data response."""

from __future__ import annotations

import logging
from typing import Any

import anyio
from faststream.message import encode_message

from kubemq_faststream.subscriber.usecase import KubeMQSubscriber

logger = logging.getLogger("kubemq_faststream")


class QueriesSubscriber(KubeMQSubscriber):
    """Subscribe to KubeMQ Queries (request-reply with body/metadata response)."""

    def __init__(
        self,
        config: Any,
        specification: Any,
        calls: Any,
    ) -> None:
        super().__init__(config, specification, calls)

    async def _consume(self) -> None:
        """Consume loop: receive queries, run handler, send response with body."""
        from kubemq.cq.queries_subscription import QueriesSubscription
        from kubemq.cq.query_response_message import QueryResponse

        from kubemq_faststream.parser import KubeMQParser

        subscription = QueriesSubscription(
            channel=self.channel,
            group=self.group or "",
            on_receive_query_callback=lambda _: None,
        )
        cq_client = self._connection.cq  # type: ignore[union-attr]
        client_id = cq_client.config.client_id

        while self.running:
            try:
                async for query_received in cq_client.subscribe_to_queries(subscription):
                    if not self.running:
                        break
                    try:
                        raw = KubeMQParser.from_query_received(query_received)
                        result = await self.consume(raw)

                        # Send success response with body (unless no_reply)
                        if not self._no_reply:
                            body, _ = (
                                encode_message(result, None) if result is not None else (b"", None)
                            )
                            response = QueryResponse(
                                query_received=query_received,
                                client_id=client_id,  # type: ignore[arg-type]
                                request_id=query_received.id,
                                is_executed=True,
                                body=body,
                                metadata=query_received.metadata or "",
                                tags=query_received.tags or {},
                                cache_hit=bool(getattr(query_received, "cache_hit", False)),
                            )
                            await cq_client.send_response(response)
                    except anyio.get_cancelled_exc_class():
                        raise
                    except Exception:
                        logger.exception("Query handler error on channel %s", self.channel)
                        # Send sanitized error response (H-6)
                        if not self._no_reply:
                            try:
                                error_response = QueryResponse(
                                    query_received=query_received,
                                    client_id=client_id,  # type: ignore[arg-type]
                                    request_id=query_received.id,
                                    is_executed=False,
                                    error="handler execution failed",
                                    body=b"",
                                    metadata="",
                                    tags={},
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

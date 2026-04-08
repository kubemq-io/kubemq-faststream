"""Modular router for composing subscriber/publisher groups."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

from faststream._internal.broker.router import BrokerRouter

from kubemq_faststream.config import KubeMQBrokerConfig
from kubemq_faststream.message import KubeMQRawMessage
from kubemq_faststream.registrator import KubeMQRegistrator


class KubeMQRouter(KubeMQRegistrator, BrokerRouter[KubeMQRawMessage, KubeMQBrokerConfig]):
    """Modular router for composing subscriber/publisher groups.

    Usage::

        orders_router = KubeMQRouter(prefix="orders.")

        @orders_router.subscriber(queues="new")
        async def handle(order: dict) -> None: ...

        broker.include_router(orders_router)
    """

    def __init__(
        self,
        *,
        prefix: str = "",
        handlers: Sequence[Any] = (),
        dependencies: Sequence[Any] = (),
        middlewares: Sequence[Callable] = (),
        routers: Sequence[Any] = (),
        parser: Any | None = None,
        decoder: Any | None = None,
        include_in_schema: bool | None = None,
    ) -> None:
        super().__init__(
            handlers=handlers,
            config=KubeMQBrokerConfig(
                prefix=prefix,
                include_in_schema=include_in_schema,
                broker_middlewares=middlewares,
                broker_dependencies=dependencies,
                broker_parser=parser,
                broker_decoder=decoder,
            ),
            routers=routers,
        )

"""KubeMQ messaging pattern and start position enums."""

from __future__ import annotations

from enum import StrEnum, unique

from faststream.middlewares import AckPolicy

__all__ = [
    "AckPolicy",
    "FeatureNotSupportedException",
    "KubeMQPattern",
    "StartPosition",
]


@unique
class KubeMQPattern(StrEnum):
    """KubeMQ messaging patterns."""

    EVENTS = "events"
    EVENTS_STORE = "events_store"
    QUEUES = "queues"
    COMMANDS = "commands"
    QUERIES = "queries"


@unique
class StartPosition(StrEnum):
    """EventsStore replay start positions."""

    START_FROM_NEW = "start_from_new"
    START_FROM_FIRST = "start_from_first"
    START_FROM_LAST = "start_from_last"
    START_AT_SEQUENCE = "start_at_sequence"
    START_AT_TIME = "start_at_time"
    START_AT_TIME_DELTA = "start_at_time_delta"


class FeatureNotSupportedException(Exception):
    """Raised when a feature is not supported for a pattern."""

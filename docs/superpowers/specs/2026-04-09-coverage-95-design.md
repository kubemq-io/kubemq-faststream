# kubemq-faststream Coverage Improvement to 95%

**Date:** 2026-04-09
**Target:** Increase overall test coverage from 90.48% to 95%+
**Approach:** Gap-category batches (unit + integration)

## Current State

| File | Coverage | Key Gaps |
|------|:--------:|----------|
| `broker.py` | 72% | Batch ops (2 methods), error cleanup, ping timeout/retry, protocol default, not-connected guards |
| `subscriber/specification.py` | 80% | Fallback name, empty schema |
| `subscriber/events_store.py` | 84% | Start value branches, loop break, cancellation |
| `producer.py` | 85% | message_id (5 branches), not-connected guard, identity stubs |
| `publisher/usecase.py` | 90% | request() method untested |

## Phase 1: Batch Operations (Unit + Integration)

### Unit Tests (in `tests/test_broker.py`)

| Test | Covers |
|------|--------|
| `test_publish_events_batch_success` | broker.py:451-468 — build EventMessage objects, call send_events_message |
| `test_publish_events_batch_not_connected_raises` | broker.py not-connected guard for batch |
| `test_publish_events_batch_with_message_ids` | message_id propagation in batch |
| `test_request_batch_commands_success` | broker.py:480-517 — CommandMessage build + send |
| `test_request_batch_queries_success` | broker.py:480-517 — QueryMessage build + send |
| `test_request_batch_not_connected_raises` | broker.py not-connected guard for request_batch |

### Integration Tests (new `tests/integration/test_batch_e2e.py`)

| Test | Covers |
|------|--------|
| `test_publish_events_batch_e2e` | Publish 3 events via batch, verify all received |
| `test_request_batch_commands_e2e` | Send 2 commands via batch, verify responses |
| `test_request_batch_queries_e2e` | Send 2 queries via batch, verify responses |

## Phase 2: Parameter Branches

### producer.py — message_id (in `tests/test_producer.py`)

| Test | Covers |
|------|--------|
| `test_publish_events_with_message_id` | producer.py:63 |
| `test_publish_events_store_with_message_id` | producer.py:73 |
| `test_publish_queues_with_message_id` | producer.py:87 |
| `test_publish_commands_with_message_id` | producer.py:98 |
| `test_publish_queries_with_message_id` | producer.py:111 |
| `test_publish_batch_not_connected_raises` | producer.py:124 |

### subscriber/events_store.py — start_value (in `tests/subscriber/test_events_store.py`)

| Test | Covers |
|------|--------|
| `test_events_store_consume_start_at_sequence_with_value` | events_store.py:81 |
| `test_events_store_consume_start_at_time_with_value` | events_store.py:83 |
| `test_events_store_consume_start_at_time_delta_with_value` | events_store.py:87 |

### broker.py — constructor params (in `tests/test_broker.py`)

| Test | Covers |
|------|--------|
| `test_broker_init_protocol_default_tls_enabled` | broker.py:128-129 tls=True path |
| `test_broker_init_protocol_default_no_tls` | broker.py:128-129 tls=False path |
| `test_broker_init_specification_url_as_list` | broker.py:133-136 iterable path |

## Phase 3: Error & Lifecycle Paths

### broker.py — error paths (in `tests/test_broker.py`)

| Test | Covers |
|------|--------|
| `test_stop_closes_and_nullifies_connection` | broker.py:217 cleanup |
| `test_stop_ignores_close_exception` | broker.py:220-221 exception suppression |
| `test_connect_cleanup_ignores_individual_close_errors` | broker.py:281-282 partial cleanup |
| `test_ping_times_out` | broker.py:303 cancel_scope branch |
| `test_ping_retries_on_exception` | broker.py:306-310 retry loop |

### broker.py — not-connected guards (in `tests/test_broker.py`)

| Test | Covers |
|------|--------|
| `test_peek_queue_messages_not_connected_raises` | broker.py:425-427 |
| `test_ack_all_queue_messages_not_connected_raises` | broker.py:437-439 |

### subscriber/events_store.py — lifecycle (in `tests/subscriber/test_events_store.py`)

| Test | Covers |
|------|--------|
| `test_events_store_consume_stops_when_not_running` | events_store.py:92 |
| `test_events_store_consume_propagates_cancellation` | events_store.py:97 |

### producer.py — identity stubs (in `tests/test_producer.py`)

| Test | Covers |
|------|--------|
| `test_producer_parse_identity` | producer.py:30 |
| `test_producer_decode_identity` | producer.py:35 |

## Phase 4: Remaining Small Gaps

### subscriber/specification.py (in `tests/subscriber/test_specification.py`)

| Test | Covers |
|------|--------|
| `test_subscriber_specification_name_fallback_to_call_name` | specification.py:19 |
| `test_subscriber_specification_get_schema_returns_empty_dict` | specification.py:23 |

### publisher/usecase.py (in `tests/test_publisher.py`)

| Test | Covers |
|------|--------|
| `test_publisher_request_command` | usecase.py:65-71 |
| `test_publisher_request_query` | usecase.py:65-71 |

## Projected Results

| File | Before | After |
|------|:------:|:-----:|
| `broker.py` | 72% | 95%+ |
| `producer.py` | 85% | 95%+ |
| `subscriber/events_store.py` | 84% | 98%+ |
| `publisher/usecase.py` | 90% | 100% |
| `subscriber/specification.py` | 80% | 100% |
| **Overall** | **90.48%** | **95%+** |

## Constraints

- Follow existing test patterns (AsyncMock, MagicMock, pytest.raises, TestKubeMQBroker)
- Use existing fixtures from conftest.py
- Integration tests use `@pytest.mark.integration` marker
- No production code changes — only test code
- Bump `fail_under` from 90 to 95 in pyproject.toml after tests pass

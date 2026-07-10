# Retry and Dead-Letter Queue

## Overview

RESTAQ provides configurable retry with backoff for asynchronous message delivery. When a receiver fails to deliver a message to the target URL, it retries the delivery with a fixed delay between attempts. After exhausting all retries, the message is rejected and routed to the broker's native dead-letter queue (DLQ).

---

## How It Works

```
Queue ──▶ Receiver ──POST──▶ Target
              │                  │
              │    ◀── Failure ──┘
              │
              ▼
         Wait (backoff)
              │
              ▼
         Redeliver (retry)
              │
              ▼
    Exhausted? ──Yes──▶ Reject ──▶ Broker DLQ
```

### Retry Flow

1. **Consume:** The receiver picks up a message from the queue.
2. **Deliver:** An HTTP POST is made to the configured target URL.
3. **Fail:** The target returns a non-2xx response, times out, or is unreachable.
4. **Wait:** The receiver waits for the configured `backoff-period`.
5. **Redeliver:** The message is delivered again with an incremented `X-Retry-Count`.
6. **Exhaust:** After `max-retries` failed attempts, the message is **rejected**.

### X-Retry-Count Header

Every delivery attempt includes the `X-Retry-Count` header:

| Attempt | X-Retry-Count |
|---------|---------------|
| First delivery | `0` |
| First retry | `1` |
| Second retry | `2` |
| ... | ... |

!!! tip
    Downstream applications can use this header to implement idempotency checks or to log delivery attempts.

---

## Dead-Letter Queue

After exhausting retries, RESTAQ **rejects** the message. The broker then routes it to the configured dead-letter queue using its native mechanism:

| Broker | DLQ Mechanism |
|--------|---------------|
| **RabbitMQ** | Dead Letter Exchange (DLX) configured on the source queue |
| **ActiveMQ Artemis** | Dead Letter Address configured on the address settings |

!!! important
    RESTAQ does **not** implement custom dead-letter handling. DLQ routing is entirely managed by the broker. You must configure the dead-letter exchange or address on the broker side.

---

## Synchronous Mode

Messages in [synchronous mode](synchronous-mode.md) **skip retry entirely**. On failure, the message is rejected immediately and routed to the DLQ. The failure response is returned transparently to the waiting caller.

---

## Configuration

Retry settings are configured per receiver:

```yaml
restqa:
  type: amqp
  receiver:
    orders:
      rest:
        url: http://orders-service:8080/process
      queue:
        name: orders.queue
      retry:
        max-retries: 5
        backoff-period: 10s
      timeout: 30s
```

### Properties

| Property | Description | Default |
|----------|-------------|---------|
| `retry.max-retries` | Maximum number of delivery attempts | `3` |
| `retry.backoff-period` | Fixed delay between retry attempts | `5s` |

---

## Examples

### Conservative Retry (high-value messages)

```yaml
restqa:
  receiver:
    payments:
      rest:
        url: http://payment-service:8080/process
      queue:
        name: payments.queue
      retry:
        max-retries: 10
        backoff-period: 30s
      timeout: 60s
```

### Aggressive Retry (low-latency preference)

```yaml
restqa:
  receiver:
    notifications:
      rest:
        url: http://notification-service:8080/send
      queue:
        name: notifications.queue
      retry:
        max-retries: 3
        backoff-period: 2s
      timeout: 10s
```

### No Retry (fire-and-forget with DLQ)

```yaml
restqa:
  receiver:
    audit:
      rest:
        url: http://audit-service:8080/log
      queue:
        name: audit.queue
      retry:
        max-retries: 1
        backoff-period: 0s
      timeout: 5s
```

!!! note
    Even with `max-retries: 1`, the message gets one delivery attempt. On failure, it goes directly to the DLQ.

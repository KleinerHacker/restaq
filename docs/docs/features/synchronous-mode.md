# Synchronous Mode

## Overview

Synchronous mode allows a sender to wait for the downstream response instead of returning an immediate **202 Accepted**. This enables request-reply patterns over queues while preserving the queue-based architecture for decoupling and buffering.

The sender publishes a message to the queue and blocks until the corresponding receiver processes it and returns a response. The response (status code, headers, and body) is then forwarded transparently back to the original caller.

---

## How It Works

```
Client ──POST──▶ Sender ──▶ Queue ──▶ Receiver ──POST──▶ Target
                   ▲                                │
                   └────────── Response ◀───────────┘
```

1. The sender publishes the message to the queue and generates an `X-Restqa-Correlation-Id`.
2. The sender registers the correlation ID in an **in-memory correlation registry** and waits.
3. The receiver consumes the message, delivers it to the target, and captures the response.
4. The receiver matches the response back to the waiting sender using the correlation ID.
5. The sender returns the downstream response to the original caller.

### Correlation

Correlation between request and response uses the `X-Restqa-Correlation-Id` header, which is automatically generated and attached to the queue message.

!!! warning "Same-JVM Constraint"
    Synchronous mode uses an **in-memory correlation registry**. The sender and receiver must run in the **same JVM instance**. This mode does not work across distributed RESTAQ deployments.

---

## Response Behaviour

| Scenario | Response to Client |
|----------|-------------------|
| Target returns **2xx** | 2xx response forwarded transparently (status, headers, body) |
| Target returns **non-2xx** | Non-2xx response forwarded transparently |
| Timeout exceeded | **504 Gateway Timeout** with RFC 9457 Problem Details |
| Broker error on publish | **502 Bad Gateway** with RFC 9457 Problem Details |

!!! note
    Unlike asynchronous mode, non-2xx responses from the target are **not retried**. They are passed directly back to the caller.

---

## Retry Behaviour

Synchronous messages have **no retry logic**. If delivery to the target fails (connection error, timeout), the message is routed directly to the broker's dead-letter queue.

This design choice reflects that the caller is actively waiting — retrying would only increase latency without providing feedback to the caller.

---

## Configuration

Synchronous mode requires two parts:

1. A **sender** with `synchronous.receiver-ref` pointing to a receiver name.
2. A **receiver** without `rest.url` (sync-only receiver).

```yaml
restqa:
  type: amqp
  sender:
    orders:
      rest:
        path: /api/orders
      queue:
        name: orders.queue
      synchronous:
        receiver-ref: order-processor
      timeout: 30s
  receiver:
    order-processor:
      queue:
        name: orders.queue
      timeout: 30s
```

### Sender Properties

| Property | Description | Required |
|----------|-------------|----------|
| `synchronous.receiver-ref` | Name of the receiver that handles responses | Yes |
| `timeout` | Maximum time to wait for a response | No (default: `30s`) |

### Receiver Constraints

| Constraint | Reason |
|------------|--------|
| `rest.url` must be **omitted** | A sync-only receiver does not deliver proactively |
| No retry configuration | Sync messages skip retry entirely |

!!! danger "Invalid Configuration"
    If a receiver referenced by `synchronous.receiver-ref` has a `rest.url` configured, it will act as an asynchronous receiver and responses will not be correlated back to the sender.

---

## Example: Request-Reply Gateway

```yaml
restqa:
  type: amqp
  sender:
    query:
      rest:
        path: /api/query
      queue:
        name: query.request.queue
      synchronous:
        receiver-ref: query-responder
      timeout: 10s
  receiver:
    query-responder:
      queue:
        name: query.request.queue
      timeout: 10s
```

In this example:

- A client POSTs to `/api/query`.
- The message travels through the queue and is delivered to the downstream target.
- The downstream response is returned to the client within 10 seconds, or a 504 is returned.

# Receiver

## Overview

The receiver is the Queue-to-REST delivery component of RESTAQ. It consumes messages from a queue and delivers them via HTTP POST to a configured target URL. This enables asynchronous message processing with proactive delivery to downstream services.

Multiple receivers can be defined, each consuming from its own queue and delivering to a separate target endpoint.

---

## How It Works

```
Queue (AMQP / JMS) ──▶ RESTAQ Receiver ──POST──▶ Target Application
```

1. The receiver listens on a configured queue.
2. When a message arrives, the queue message body becomes the HTTP POST body.
3. Queue message properties are mapped back to HTTP headers.
4. An `X-Retry-Count` header is injected (zero-based) indicating the current delivery attempt.
5. The message is delivered via HTTP POST to the configured target URL.

### On Success

When the target responds with a **2xx** status code, the message is **acknowledged** and removed from the queue.

### On Failure

When delivery fails (non-2xx response, timeout, or connection error):

1. The receiver waits for the configured backoff period.
2. The message is redelivered with an incremented `X-Retry-Count`.
3. After exhausting all retries, the message is **rejected** and routed to the broker's dead-letter queue.

!!! info
    See [Retry and DLQ](retry-and-dlq.md) for detailed retry behaviour and configuration.

---

## Configuration

Each receiver is defined under `restqa.receiver.<name>`:

```yaml
restqa:
  type: amqp
  receiver:
    notifications:
      rest:
        url: http://downstream:8080/notify
      queue:
        name: notifications.queue
      retry:
        max-retries: 5
        backoff-period: 10s
      time-to-live: 1h
      timeout: 30s
```

### Properties

| Property | Description | Required |
|----------|-------------|----------|
| `rest.url` | Target HTTP POST URL for delivery | Yes (async) |
| `queue.name` | Source queue or destination name | Yes |
| `retry.max-retries` | Maximum delivery attempts | No (default: `3`) |
| `retry.backoff-period` | Delay between retries | No (default: `5s`) |
| `time-to-live` | Maximum message age before discard | No |
| `timeout` | Maximum processing/wait time | No (default: `30s`) |

!!! note
    If `rest.url` is omitted, the receiver operates in [synchronous mode](synchronous-mode.md) only — it does not deliver proactively.

---

## Multiple Receivers

Define as many receivers as needed — each independently consumes from its own queue:

```yaml
restqa:
  type: amqp
  receiver:
    order-processor:
      rest:
        url: http://orders-service:8080/process
      queue:
        name: orders.queue
      retry:
        max-retries: 5
        backoff-period: 10s
      timeout: 30s
    notifications:
      rest:
        url: http://notification-service:8080/notify
      queue:
        name: notifications.queue
      retry:
        max-retries: 3
        backoff-period: 5s
      timeout: 15s
    audit:
      rest:
        url: http://audit-service:8080/log
      queue:
        name: audit.queue
      timeout: 10s
```

Each receiver has its own retry policy, timeout, and target URL.

---

## Headers

The receiver restores queue message properties back to HTTP headers when making the POST request to the target. Additionally, it always injects:

| Header | Description |
|--------|-------------|
| `X-Retry-Count` | Zero-based retry attempt counter (0 on first delivery) |

!!! tip
    Downstream applications can use `X-Retry-Count` to detect redeliveries and implement idempotency logic.

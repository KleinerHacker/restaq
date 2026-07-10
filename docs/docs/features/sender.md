# Sender

## Overview

The sender is the REST-to-Queue gateway component of RESTAQ. It exposes configurable HTTP POST endpoints that accept incoming requests and forward them into a message queue. This allows external clients to communicate asynchronously with downstream systems through a durable messaging layer.

Multiple senders can be defined, each with its own REST path and target queue. Both **AMQP** (RabbitMQ) and **JMS** (ActiveMQ Artemis) are supported as messaging backends.

---

## How It Works

```
Client ──POST──▶ RESTAQ Sender ──▶ Queue (AMQP / JMS)
```

1. A client sends an HTTP POST request to a configured sender endpoint.
2. The request body becomes the queue message body (any content type, forwarded without modification).
3. HTTP request headers are mapped to queue message properties (after [header filtering](header-propagation.md)).
4. The message is published to the configured queue or exchange.

The sender acts as a transparent bridge — it does not interpret or transform the payload.

---

## Response Codes

| Status | Meaning |
|--------|---------|
| **202 Accepted** | Message successfully placed on the queue (no body) |
| **413 Content Too Large** | Payload exceeds configured [size limit](payload-limits.md) — RFC 9457 Problem Details response |
| **502 Bad Gateway** | Broker unreachable or publish failed — RFC 9457 Problem Details response |

!!! note
    In [synchronous mode](synchronous-mode.md), the sender waits for the downstream response instead of returning 202 immediately.

---

## Configuration

Each sender is defined under `restqa.sender.<name>`:

```yaml
restqa:
  type: amqp
  sender:
    orders:
      rest:
        path: /api/orders
      queue:
        name: orders.queue
    events:
      rest:
        path: /api/events
      queue:
        name: events.queue
        exchange: events.exchange
        routingKey: events.routing
```

### Properties

| Property | Description | Required |
|----------|-------------|----------|
| `rest.path` | HTTP endpoint path for this sender | Yes |
| `queue.name` | Target queue or destination name | Yes |
| `queue.exchange` | AMQP exchange name | No |
| `queue.routingKey` | AMQP routing key (defaults to queue name) | No |
| `synchronous.receiver-ref` | Receiver name for synchronous mode | No |
| `timeout` | Max wait time for synchronous response | No (default: `30s`) |

---

## Multiple Senders

Define as many senders as needed — each gets its own REST path and queue target:

```yaml
restqa:
  type: jms
  sender:
    orders:
      rest:
        path: /api/orders
      queue:
        name: orders.queue
    payments:
      rest:
        path: /api/payments
      queue:
        name: payments.queue
    notifications:
      rest:
        path: /api/notifications
      queue:
        name: notifications.queue
```

---

## Broker Support

The sender works identically regardless of the underlying broker technology:

=== "AMQP (RabbitMQ)"

    ```yaml
    restqa:
      type: amqp
      sender:
        orders:
          rest:
            path: /api/orders
          queue:
            name: orders.queue
            exchange: orders.exchange
            routingKey: orders.key
    ```

    AMQP-specific options (`exchange`, `routingKey`) are available for fine-grained routing.

=== "JMS (ActiveMQ Artemis)"

    ```yaml
    restqa:
      type: jms
      sender:
        orders:
          rest:
            path: /api/orders
          queue:
            name: orders.queue
    ```

    JMS senders publish directly to the named destination.

!!! tip
    Broker connectivity is configured using standard Spring Boot properties (`spring.rabbitmq.*` or `spring.artemis.*`).

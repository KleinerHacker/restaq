# Configuration

All RESTAQ-specific configuration lives under the `restqa` namespace in `application.yaml`. Broker connectivity uses standard Spring Boot properties.

For feature-specific behaviour and design rationale, see the [Features](features/sender.md) section.

---

## Queue Type

```yaml
restqa:
  type: amqp  # or: jms
```

| Value | Broker |
|-------|--------|
| `amqp` (default) | RabbitMQ or AMQP-compatible brokers |
| `jms` | ActiveMQ Artemis or JMS-compatible providers |

---

## Sender Configuration

Each sender exposes a REST POST endpoint that forwards requests to a queue.

```yaml
restqa:
  sender:
    orders:
      rest:
        path: /api/orders
      queue:
        name: orders.queue
```

### Properties

| Property | Required | Description |
|----------|----------|-------------|
| `rest.path` | Yes | HTTP path for the sender endpoint |
| `queue.name` | Yes | Queue or destination name |
| `queue.exchange` | No | AMQP exchange (ignored for JMS) |
| `queue.routingKey` | No | AMQP routing key (defaults to queue name) |
| `timeout` | No | Maximum wait time (default: `30s`) |

See [Sender](features/sender.md) for details.

---

## Receiver Configuration

Each receiver consumes messages from a queue and delivers them via HTTP POST to a target URL.

```yaml
restqa:
  receiver:
    notifications:
      rest:
        url: http://downstream:8080/notify
      queue:
        name: notifications.queue
```

### Properties

| Property | Required | Default | Description |
|----------|----------|---------|-------------|
| `rest.url` | Conditional | – | Target HTTP URL for callback delivery. Must be omitted for sync-only receivers. |
| `queue.name` | Yes | – | Queue or destination name |
| `retry.max-retries` | No | `3` | Maximum number of delivery attempts before DLQ |
| `retry.backoff-period` | No | `5s` | Delay between retry attempts |
| `time-to-live` | No | *(none)* | Maximum age of a message; expired messages are discarded |
| `timeout` | No | `30s` | Maximum processing/wait time |

See [Receiver](features/receiver.md) for details.

---

## Payload Size Limit

```yaml
restqa:
  max-payload-size: 10MB
```

Requests exceeding this size receive a `413 Content Too Large` response with Problem Details. If not set, no size limit is enforced.

See [Payload Limits](features/payload-limits.md) for details.

---

## Synchronous Mode

Synchronous mode allows a sender to wait for the downstream response from a receiver before replying to the original caller. This enables request-reply semantics over a queue. The sender and its referenced receiver must run in the same JVM instance.

### Sender Properties

| Property | Required | Default | Description |
|----------|----------|---------|-------------|
| `synchronous.receiver-ref` | Yes | – | Name of the receiver that handles the response |
| `timeout` | No | `30s` | Maximum time to wait for the downstream response |

### Receiver Constraint

A receiver used for synchronous mode must **not** have a `rest.url` configured — it acts as a sync-only channel.

See [Synchronous Mode](features/synchronous-mode.md) for the full guide.

---

## Broker Connection

RESTAQ uses standard Spring Boot properties for broker connectivity.

=== "AMQP (RabbitMQ)"

    ```yaml
    spring:
      rabbitmq:
        host: localhost
        port: 5672
        username: guest
        password: guest
    ```

=== "JMS (Artemis)"

    ```yaml
    spring:
      artemis:
        mode: native
        broker-url: tcp://localhost:61616
        user: artemis
        password: artemis
    ```

---

## Header Propagation

All HTTP headers are propagated as queue message properties, with exclusions for TLS/certificate, transport, proxy, and forwarding headers. The `Content-Type` header is always propagated.

See [Header Propagation](features/header-propagation.md) for the complete list of excluded headers.

---

## Complete Example

```yaml
spring:
  rabbitmq:
    host: broker.internal
    port: 5672
    username: restqa
    password: secret

restqa:
  type: amqp
  max-payload-size: 5MB
  sender:
    orders:
      rest:
        path: /api/orders
      queue:
        name: orders.queue
        exchange: orders.exchange
        routingKey: orders.created
      synchronous:
        receiver-ref: order-processor
      timeout: 30s
    events:
      rest:
        path: /api/events
      queue:
        name: events.queue
  receiver:
    order-processor:
      queue:
        name: orders.queue
      timeout: 30s
    notifications:
      rest:
        url: http://notification-service:8080/receive
      queue:
        name: notifications.queue
      retry:
        max-retries: 5
        backoff-period: 10s
      time-to-live: 30m
      timeout: 30s
```

# Configuration

All RESTAQ-specific configuration lives under the `restqa` namespace in `application.yaml`. Broker connectivity uses standard Spring Boot properties.

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
    <name>:
      rest:
        path: /api/orders        # REST endpoint path
      queue:
        name: orders.queue       # Queue/destination name
        exchange: orders.exchange # (AMQP only) Exchange name
        routingKey: orders.created # (AMQP only) Routing key, defaults to queue name
```

### Properties

| Property | Required | Description |
|----------|----------|-------------|
| `rest.path` | Yes | HTTP path for the sender endpoint |
| `queue.name` | Yes | Queue or destination name |
| `queue.exchange` | No | AMQP exchange (ignored for JMS) |
| `queue.routingKey` | No | AMQP routing key (defaults to queue name) |
| `timeout` | No | Maximum wait time (default: `30s`) |

---

## Receiver Configuration

Each receiver consumes messages from a queue and delivers them via HTTP POST to a target URL.

```yaml
restqa:
  receiver:
    <name>:
      rest:
        url: http://downstream:8080/notify  # Target URL
      queue:
        name: notifications.queue            # Queue/destination name
      retry:
        max-retries: 3                       # Max delivery attempts (default: 3)
        backoff-period: 5s                   # Delay between retries (default: 5s)
      time-to-live: 1h                       # Max message age (optional)
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

---

## Payload Size Limit

```yaml
restqa:
  max-payload-size: 10MB
```

Requests exceeding this size receive a `413 Content Too Large` response with Problem Details. If not set, no size limit is enforced.

---

## Synchronous Mode

By default, senders respond with **202 Accepted** immediately after placing a message on the queue. Synchronous mode allows a sender to wait for the downstream response from a receiver before replying to the original caller.

This enables request-reply semantics over a queue while preserving the full RESTAQ pipeline (header propagation, payload forwarding).

!!! warning "Same-JVM Constraint"
    Synchronous mode requires the sender and its referenced receiver to run in the **same JVM instance**. Correlation is handled via an in-memory registry — it does not work across separate deployments.

!!! info "No Retry for Synchronous Messages"
    Synchronous messages do not use retry logic. On delivery failure, the message is routed directly to the broker's dead-letter queue. The `X-Retry-Count` header is not injected for synchronous messages.

### Sender Configuration

```yaml
restqa:
  sender:
    orders:
      rest:
        path: /api/orders
      queue:
        name: orders.queue
      synchronous:
        receiver-ref: order-processor   # References a receiver by name
      timeout: 30s                      # Max wait time (default: 30s)
```

| Property | Required | Default | Description |
|----------|----------|---------|-------------|
| `synchronous.receiver-ref` | Yes | – | Name of the receiver that handles the response |
| `timeout` | No | `30s` | Maximum time to wait for the downstream response |

### Receiver Configuration

A receiver used for synchronous mode must **not** have a `rest.url` configured — it acts as a sync-only channel. Correlation is detected automatically when the `X-Restqa-Correlation-Id` header is present on the message.

```yaml
restqa:
  receiver:
    order-processor:
      queue:
        name: orders.queue
      timeout: 30s                      # Max processing time (default: 30s)
```

| Property | Required | Default | Description |
|----------|----------|---------|-------------|
| `queue.name` | Yes | – | Queue or destination name |
| `timeout` | No | `30s` | Maximum time for processing the synchronous response |

!!! note "Validation Rules"
    - A sender's `synchronous.receiver-ref` must point to an existing receiver
    - The referenced receiver must **not** have a `rest.url`
    - A receiver without `rest.url` must be referenced by at least one sender's `synchronous.receiver-ref`
    - A receiver with `rest.url` must **not** be used as a synchronous reference

### How It Works

1. The sender places the message on the queue with an injected `X-Restqa-Correlation-Id` header
2. The sender blocks (up to `timeout`) waiting for a response in the in-memory correlation registry
3. The receiver consumes the message, detects the correlation header, delivers it to the target, and feeds the response back to the registry
4. The sender returns the downstream response (status code, headers, body) to the original caller

**Timeout behaviour:** If the timeout expires before a response arrives, the sender returns **504 Gateway Timeout** with Problem Details.

**Non-2xx responses:** If the downstream target returns a non-2xx status, that response is transparently forwarded to the original caller.

**Failure handling:** On delivery failure, the message goes directly to the broker's DLQ — no retry attempts are made.

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

---

## Header Propagation

All HTTP headers are propagated as queue message properties, with the following exclusions:

**Excluded headers:**

- TLS/certificate: `X-Forwarded-Client-Cert`, `X-SSL-*`, `X-Client-Cert-*`
- Transport: `Host`, `Connection`, `Keep-Alive`, `Transfer-Encoding`, `TE`, `Trailer`, `Upgrade`
- Proxy: `Proxy-Authorization`, `Proxy-Authenticate`
- Forwarding: `X-Forwarded-Host`, `X-Forwarded-Port`, `X-Forwarded-Proto`, `X-Forwarded-For`, `X-Real-IP`, `Forwarded`, `Via`

The `Content-Type` header is always propagated.

On the receiver side, the `X-Retry-Count` header (zero-based) is injected into outgoing requests to indicate the delivery attempt number.

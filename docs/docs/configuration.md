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
| `rest.url` | Yes | – | Target HTTP URL for callback delivery |
| `queue.name` | Yes | – | Queue or destination name |
| `retry.max-retries` | No | `3` | Maximum number of delivery attempts before DLQ |
| `retry.backoff-period` | No | `5s` | Delay between retry attempts |
| `time-to-live` | No | *(none)* | Maximum age of a message; expired messages are discarded |

---

## Payload Size Limit

```yaml
restqa:
  max-payload-size: 10MB
```

Requests exceeding this size receive a `413 Content Too Large` response with Problem Details. If not set, no size limit is enforced.

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
    events:
      rest:
        path: /api/events
      queue:
        name: events.queue
  receiver:
    notifications:
      rest:
        url: http://notification-service:8080/receive
      queue:
        name: notifications.queue
      retry:
        max-retries: 5
        backoff-period: 10s
      time-to-live: 30m
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

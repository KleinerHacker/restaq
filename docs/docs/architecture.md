# Architecture

## Overview

RESTAQ acts as a protocol bridge between synchronous HTTP communication and asynchronous message-oriented middleware. It is designed to decouple systems across network segments, security zones, or trust boundaries.

## Components

The system consists of two functional areas:

### Sender

The sender exposes REST POST endpoints. Incoming HTTP requests are transformed into queue messages and forwarded to the configured messaging backend.

```
Client
  ──POST──▶ RESTAQ Sender Endpoint
    ──▶ Header Filtering
      ──▶ Payload Size Check
        ──▶ Queue Message
          ──▶ AMQP / JMS Broker
```

On success, the sender responds with **202 Accepted** (no body).

### Receiver

The receiver consumes messages from configured queues and proactively invokes HTTP POST endpoints on downstream systems.

```
AMQP / JMS Broker
  ──▶ RESTAQ Receiver Consumer
    ──▶ TTL Check
      ──▶ HTTP POST (with X-Retry-Count)
        ──▶ Target Application
```

On delivery failure, the receiver retries with configurable backoff. After exhausting retries, the message is routed to the broker's dead-letter queue.

---

## Package Structure

Root package: `org.pcsoft.micro.restqa`

Each functional area uses the following sub-package convention:

| Sub-package | Purpose |
|-------------|---------|
| `port` | REST controllers and queue connection points (adapters) |
| `configuration` | Queue and REST controller configuration beans |
| *(root)* | Services exposed for use by other domains |

### Sender Packages

```
org.pcsoft.micro.restqa.send
├── port/
│   ├── SenderEndpointController   (HTTP → Queue)
│   ├── MessageQueueClient         (interface)
│   ├── AmqpQueueClient            (RabbitMQ implementation)
│   └── JmsQueueClient             (JMS implementation)
└── configuration/
    └── SenderEndpointConfiguration (router setup)
```

### Receiver Packages

```
org.pcsoft.micro.restqa.receive
├── port/
│   ├── ReceiverEndpointController (Queue → HTTP)
│   ├── AmqpQueueConsumer          (RabbitMQ consumer)
│   └── JmsQueueConsumer           (JMS consumer)
└── configuration/
    └── WebClientConfiguration     (HTTP client)
```

### Shared Packages

```
org.pcsoft.micro.restqa
├── configuration/
│   ├── RestqaProperties           (binding model)
│   ├── AmqpQueueConfiguration     (AMQP declarations)
│   └── JmsQueueConfiguration      (JMS template)
└── internal/utils/
    ├── HeaderFilter               (header exclusion logic)
    └── LoggerUtils                (logger factory)
```

---

## Design Principles

- **POST-only:** Both sender and receiver use exclusively HTTP POST. If RESTAQ and the queue are removed, services can communicate directly via POST.
- **Transparent payload:** Any content type is forwarded without modification.
- **Header propagation:** HTTP headers become message properties and vice versa, excluding TLS/transport metadata.
- **Explicit error types:** `Either<ProblemDetail, T>` in the service layer; controllers fold into HTTP responses.
- **Broker-native DLQ:** RESTAQ does not implement custom dead-letter handling. It relies on RabbitMQ Dead Letter Exchanges or JMS broker DLQ configuration.

---

## Typical Deployment

```
External Network
    │
    │ HTTPS POST
    ▼
┌──────────┐
│  RESTAQ  │  (Sender)
└──────────┘
    │
    │ AMQP / JMS
    ▼
Message Broker
    │
    │ AMQP / JMS
    ▼
┌──────────┐
│  RESTAQ  │  (Receiver)
└──────────┘
    │
    │ HTTPS POST
    ▼
Internal Application
```

Or combined in a single instance handling both directions.

---

## Use Cases

| Scenario | Description |
|----------|-------------|
| DMZ Gateway | Expose a controlled REST interface externally, forward asynchronously into internal infrastructure |
| Async Integration | Convert synchronous REST calls into durable queue-based processing |
| Legacy Queue Integration | Provide modern REST interfaces for legacy JMS systems |
| Outage Buffering | Protect downstream systems by buffering traffic through queues |
| Cross-Network Bridging | Transfer messages securely between isolated network segments |

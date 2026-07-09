# RESTAQ

<p align="center">
  <img src="assets/images/logo.png" alt="RESTAQ Logo" width="384"/>
</p>

> **REST in. Queue on.**

RESTAQ is a lightweight, Spring Boot–based messaging gateway designed for crossing network boundaries using asynchronous messaging.

It exposes configurable REST endpoints that forward incoming requests into messaging infrastructures such as **AMQP** (RabbitMQ) and **JMS** (ActiveMQ Artemis), while also supporting the reverse direction through proactive HTTP callback delivery.

---

## Key Features

- **REST → Queue** gateway (sender)
- **Queue → REST** consumer delivery (receiver)
- Supports AMQP and JMS
- Fully configurable inbound and outbound endpoints
- RFC 9457 Problem Details error responses
- Configurable retry with backoff and dead-letter queue support
- Message time-to-live enforcement
- Transparent HTTP header propagation (with TLS/transport filtering)
- Configurable payload size limits
- Stateless and horizontally scalable
- DMZ / integration-zone friendly architecture

---

## Quick Start

### Prerequisites

- JDK 25
- Docker (for integration tests via Testcontainers)
- A message broker (RabbitMQ for AMQP, or ActiveMQ Artemis for JMS)

### Build & Run

```bash
./gradlew build
./gradlew bootRun
```

### Minimal Configuration

```yaml
restqa:
  type: amqp
  sender:
    orders:
      rest:
        path: /api/orders
      queue:
        name: orders.queue
  receiver:
    notifications:
      rest:
        url: http://downstream:8080/notify
      queue:
        name: notifications.queue
```

---

## How It Works

```
Client ──POST──▶ RESTAQ Sender ──▶ Queue ──▶ RESTAQ Receiver ──POST──▶ Target
```

1. An external client sends an HTTP POST to a RESTAQ sender endpoint.
2. The request body and headers are placed on the configured queue.
3. On the other side, the RESTAQ receiver consumes the message.
4. The receiver delivers the payload via HTTP POST to the configured target URL.

This decouples systems across network segments, security zones, or trust boundaries.

---

## Technology Stack

| Component | Technology |
|-----------|-----------|
| Language | Kotlin |
| Framework | Spring Boot 4 |
| Web Layer | Spring WebFlux |
| Messaging | Spring AMQP, Spring JMS |
| Error Handling | Arrow (Either), Spring ProblemDetail |
| Build | Gradle (Kotlin DSL) |
| Testing | JUnit 5, Mockito-Kotlin, Testcontainers, WireMock |

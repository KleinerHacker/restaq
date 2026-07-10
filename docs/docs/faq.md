# FAQ

## General

### What is RESTAQ?

RESTAQ is a lightweight messaging gateway — REST in, Queue on. It crosses network boundaries using asynchronous messaging, bridging REST endpoints and message queues (AMQP/JMS).

### What brokers are supported?

AMQP (RabbitMQ) and JMS (ActiveMQ Artemis).

### Why only POST?

If RESTAQ and the queue are removed from the architecture, services can talk directly to each other via POST without changes. This ensures design consistency and keeps the gateway transparent.

### Is RESTAQ stateless?

Yes. RESTAQ is stateless and horizontally scalable. The only exception is synchronous request-reply mode, which uses in-memory correlation within a single JVM.

## Configuration

### Can I run multiple senders/receivers?

Yes. Each sender and receiver is identified by a unique name under `restqa.sender.<name>` and `restqa.receiver.<name>`. You can define as many as needed.

### How do I switch between AMQP and JMS?

Set `restqa.type` to `amqp` or `jms` in your configuration.

### What happens if no max-payload-size is set?

No size limit is enforced. All payloads are accepted regardless of size.

### How do I configure the broker connection?

Use standard Spring Boot properties: `spring.rabbitmq.*` for RabbitMQ or `spring.artemis.*` for ActiveMQ Artemis.

## Synchronous Mode

### Can I use synchronous mode across multiple JVM instances?

No. Synchronous mode relies on in-memory correlation and requires the sender and receiver to run within the same JVM.

### What happens on timeout in sync mode?

RESTAQ returns a `504 Gateway Timeout` response with RFC 9457 Problem Details.

### Are sync messages retried?

No. On failure, synchronous messages go directly to the broker's dead-letter queue without retry.

## Error Handling

### What error format does RESTAQ use?

RFC 9457 Problem Details (`application/problem+json`). All error responses follow this standard.

### What happens when the broker is down?

RESTAQ returns `502 Bad Gateway` with Problem Details describing the connectivity failure.

### How does dead-letter handling work?

RESTAQ uses the broker's native dead-letter mechanism — RabbitMQ Dead Letter Exchanges (DLX) or Artemis Dead Letter Queues (DLQ). Messages are routed to the DLQ after retries are exhausted.

## Deployment

### What JDK version is required?

JDK 25.

### Can I run RESTAQ in Docker?

Yes. RESTAQ supports standard Spring Boot Docker packaging and deployment.

### Is RESTAQ suitable for DMZ deployment?

Yes. RESTAQ is designed for integration zones and network boundaries, making it well-suited for DMZ deployment.

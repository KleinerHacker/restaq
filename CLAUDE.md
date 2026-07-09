# Requirements

* Project Name: 'Restqa'

## Overview

This project is a service that wraps queue interactions behind a standard REST API.

### Technical Requirements

* Written in Kotlin
* Based on Spring Boot
* Built with Gradle
* Supports JMS and AMQP queues
* Uses Arrow (`io.arrow-kt:arrow-core`) for error handling
    * `Either<ProblemDetail, T>` as return type in service/queue layers
    * Controllers fold the Either into the appropriate HTTP response
    * No `Raise`-context or deeper functional patterns – keep it pragmatic
* Context
    * Microservices
    * Docker

## Architecture

The root package is `org.pcsoft.micro.restqa`. Each functional area lives in its own subdomain package.
Use the following sub-package structure within each domain:

* **port** – REST controllers and queue connection points
* **configuration** – queue and REST controller configuration
* **service** – domain-internal Spring Boot service classes (generally no components)
* *Root (no sub-package)* – Spring Boot services exposed for use by other domains

The service is split into two functional areas:

* **sender** – Publishes messages to a queue
    * Exposed as a REST POST endpoint on the service
        * The request body is forwarded as the queue message body
        * The request headers are forwarded as queue message properties
* **receiver** – Consumes messages from a queue
    * Delivers each message via a REST POST call to an external service
        * The queue message body becomes the POST request body
        * The queue message properties become POST request headers

Multiple senders and receivers can be defined through configuration.

### HTTP Method

Both sender and receiver exclusively use POST. This is by design: if Restaq and the queue are removed from the architecture, the communicating services must be able to talk to each other directly via POST.

### HTTP Response Behaviour

#### Sender

* Successful enqueue: **202 Accepted** (no body)
* Queue unreachable or broker error: **502 Bad Gateway** with a response body using the [RFC 9457 Problem Details](https://www.rfc-editor.org/rfc/rfc9457) JSON format

#### Receiver (callback delivery)

* Successful delivery: the message is acknowledged on the queue
* Failed delivery (target unreachable, non-2xx response): the message is marked as unprocessed and redelivered according to retry configuration (see below)

### Header Propagation

All HTTP headers are propagated as queue message properties, with the following exclusions:

* TLS certificate headers (e.g. `X-Forwarded-Client-Cert`)
* URL/path information (reconstructed by the receiving side)

The `Content-Type` header is propagated like any other header.

The receiver additionally injects the following headers into the outgoing HTTP request:

* `X-Retry-Count` – zero-based delivery attempt counter

### Payload Handling

* No restriction on content type – any payload is forwarded as-is
* Maximum payload size is configurable by the operator (see configuration below). If Spring Boot provides built-in mechanisms for request size limiting (e.g. `server.tomcat.max-http-form-post-size`, `spring.servlet.multipart.max-request-size`), those are used. Otherwise a custom size check is implemented.

### Schema Validation

Not in scope for the initial implementation. Planned as a future extension.

### Security

Not in scope for the initial implementation. Planned as a future extension.

## Configuration

* Uses `application.yaml` internally
* All custom configuration lives under the `restqa` namespace
* Anything not explicitly specified here follows Spring Boot defaults (e.g. broker connection via `spring.rabbitmq.*`, `spring.activemq.*`, `spring.artemis.*`)

### Queue Type

* `restqa.type` – defines the messaging protocol used globally: `jms` or `amqp`

### Sender Configuration

* Root: `restqa.sender`
* Each sender is identified by name: `restqa.sender.<sender_name>`
* Required properties:
    * `queue`
        * `name` – queue name
    * `rest`
        * `path` – REST endpoint path, e.g. `/my-sender`

### Receiver Configuration

* Root: `restqa.receiver`
* Each receiver is identified by name: `restqa.receiver.<receiver_name>`
* Required properties:
    * `queue`
        * `name` – queue name
    * `rest`
        * `url` – target URL, e.g. `http://localhost/my-receiver`
    * `retry` (optional)
        * `max-retries` – maximum number of redelivery attempts before sending to DLQ
        * `backoff-period` – delay between retries (duration format, e.g. `5s`, `30s`)
    * `time-to-live` (optional) – maximum age of a message before it is discarded to avoid stale delivery (duration format)
    * `dead-letter` – uses the broker's native dead-letter mechanism (RabbitMQ Dead Letter Exchange / JMS DLQ configuration). No custom DLQ implementation; rely on broker infrastructure.

### Payload Size

* `restqa.max-payload-size` (optional) – maximum allowed request body size (e.g. `10MB`). Applies to sender endpoints.

## Logging

Logging uses Logback (Spring Boot default).

### Log Levels

| Event | Level |
|-------|-------|
| Sender: request received | INFO |
| Sender: message placed on queue | INFO |
| Receiver: message consumed from queue | INFO |
| Receiver: callback delivered successfully | INFO |
| Receiver: retry attempt | WARN (no stacktrace) |
| Receiver: all retries exhausted / delivery failed permanently | ERROR (with stacktrace) |
| Sender: immediate error (e.g. broker unreachable) | ERROR (with stacktrace) |
| All other operational details | DEBUG |

## Testing

### Unit Testing

* Each class is tested in isolation
    * Mockito is used to mock all dependencies
    * Never run as a Spring Boot test
    * Target 100% code coverage for the class under test
    * Includes negative test cases
    * Assertions validate output data against expected results for given inputs

### Integration Testing

* Each domain's behaviour is verified with Spring Boot tests
    * Always run as Spring Boot tests
    * Includes negative test cases
    * WireMock is used to stub external HTTP services
    * Testcontainers is used to run queue brokers in Docker
    * JMS and AMQP configurations are tested separately
    * Mockito is not used
    * Assertions validate output data against expected results for given inputs, including external side effects
        * Verified in WireMock
        * Verified in the queue

### Coverage

* Target overall code coverage of 100%

<p align="center">
  <img src="docs/docs/assets/images/logo.png" alt="RESTAQ Logo" width="384"/>
</p>

# RESTAQ

> REST in. Queue on.

**Restaq** is a lightweight, Spring Boot based messaging gateway designed for crossing network boundaries using asynchronous messaging.

It exposes configurable REST endpoints that forward incoming requests into messaging infrastructures such as **AMQP** and **JMS**, while also supporting the reverse direction through proactive HTTP callback delivery.

Restaq acts as a protocol bridge between synchronous HTTP communication and asynchronous message-oriented middleware.

---

# Features

* REST-to-Queue gateway
* Queue-to-REST consumer delivery
* Supports:

  * AMQP
  * JMS
* Fully configurable inbound and outbound endpoints
* Optional XML schema validation (XSD)
* Optional JSON schema validation
* Supports arbitrary payload formats
* Transparent HTTP header propagation
* Spring Boot based
* Stateless and horizontally scalable
* Network boundary decoupling
* DMZ/integration-zone friendly architecture

---

# Architecture

Restaq is designed to decouple systems across network segments, security zones, or trust boundaries.

Typical use cases include:

* DMZ integration gateways
* Enterprise service boundaries
* Async API buffering
* Queue-backed integration layers
* Legacy system bridging
* Network isolation
* Burst traffic buffering
* Temporary outage decoupling

The system consists of two major components:

## Sender

The sender exposes REST endpoints that external systems can call.

Incoming HTTP requests are transformed into queue messages and forwarded to the configured messaging backend.

## Receiver

The receiver acts as a message consumer.

It listens to configured queues/topics and proactively invokes configured HTTP endpoints in downstream systems.

This enables asynchronous end-to-end communication while preserving HTTP semantics where useful.

---

# Messaging Support

Restaq currently supports:

| Protocol | Description                                             |
| -------- | ------------------------------------------------------- |
| AMQP     | RabbitMQ and compatible brokers                         |
| JMS      | ActiveMQ, Artemis, IBM MQ, and JMS-compatible providers |

The messaging layer is abstracted to allow flexible integration into existing enterprise environments.

---

# Payload Handling

Restaq intentionally does not enforce a strict payload model.

Any payload can be transported, including:

* JSON
* XML
* Plain text
* Binary data
* Proprietary formats

The HTTP request body is transferred as the message payload without modification unless validation or transformation is explicitly configured.

---

# Schema Validation

Optional schema validation can be enabled per endpoint.

## XML Validation

XML payloads can be validated using XSD schemas.

Supported features include:

* namespace-aware validation
* strict schema enforcement
* configurable validation failure handling

## JSON Validation

JSON payloads can be validated using JSON Schema definitions.

Validation can be configured independently for each endpoint.

---

# HTTP Header Propagation

Restaq preserves HTTP metadata by automatically transferring incoming HTTP headers into message properties.

## Mapping Rules

| HTTP Element | Message Representation |
| ------------ | ---------------------- |
| HTTP Headers | Message Properties     |
| HTTP Body    | Message Payload        |

All HTTP headers are propagated except:

* URL/path information
* transport-specific connection metadata

This allows downstream consumers to access:

* authentication metadata
* correlation IDs
* tracing headers
* custom integration headers
* content type information

---

# Endpoint Model

Endpoints are fully configurable.

Each endpoint can define:

* transport type
* destination queue/topic
* schema validation
* authentication
* timeout handling
* retry behavior
* header mapping rules

---

# Sender Flow

```text
Client
  -> RESTAQ REST Endpoint
    -> Validation (optional)
      -> Header Mapping
        -> Queue Message
          -> AMQP/JMS Broker
```

---

# Receiver Flow

```text
AMQP/JMS Broker
  -> RESTAQ Consumer
    -> Validation (optional)
      -> HTTP Request Creation
        -> Target REST Endpoint
```

---

# Typical Deployment

```text
External Network
    |
    | HTTPS
    v
+-----------+
|  RESTAQ   |
+-----------+
    |
    | AMQP / JMS
    v
Internal Messaging Infrastructure
```

Or in reverse-consumer mode:

```text
Queue
   |
   v
+-----------+
|  RESTAQ   |
+-----------+
   |
   | HTTPS Callback
   v
Target Application
```

---

# Technology Stack

* Java
* Spring Boot
* Spring Web
* Spring AMQP
* Spring JMS
* Jackson
* XML Validation APIs

---

# Design Goals

Restaq focuses on:

* simplicity
* transparency
* protocol decoupling
* operational robustness
* minimal payload assumptions
* infrastructure compatibility

The system intentionally avoids introducing proprietary payload contracts unless explicitly configured.

---

# Example Use Cases

## DMZ Gateway

Expose a controlled REST interface externally while forwarding requests asynchronously into internal infrastructure.

## Async Integration Layer

Convert synchronous REST calls into durable queue-based processing.

## Legacy Queue Integration

Provide modern REST interfaces for legacy JMS systems.

## Outage Buffering

Protect downstream systems by buffering incoming traffic through queues.

## Cross-Network Bridging

Transfer messages securely between isolated network segments.

---

# Future Ideas

Potential future extensions may include:

* Kafka support
* MQTT support
* payload transformation pipelines
* OpenAPI integration
* tracing/observability modules
* dead letter handling UI
* rate limiting
* OAuth2/JWT integration
* multi-tenant routing

---

# Philosophy

Restaq is intentionally infrastructure-focused.

It does not attempt to replace:

* API gateways
* ESBs
* workflow engines

Instead, it provides a lightweight and transparent bridge between HTTP and messaging systems.
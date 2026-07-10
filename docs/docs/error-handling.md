# Error Handling

RESTAQ uses [RFC 9457 Problem Details for HTTP APIs](https://www.rfc-editor.org/rfc/rfc9457) for all error responses. The response content type is `application/problem+json`.

---

## Sender Errors

### 202 Accepted

Returned on successful enqueue. No response body.

### 413 Content Too Large

Returned when the request body exceeds the configured `restqa.max-payload-size`.

```json
{
  "type": "urn:restqa:error:payload-too-large",
  "title": "Payload Too Large",
  "status": 413,
  "detail": "Request body of 15728640 bytes exceeds the configured limit of 10MB.",
  "instance": "/api/orders"
}
```

### 502 Bad Gateway

Returned when the message broker is unreachable or rejects the message.

```json
{
  "type": "urn:restqa:error:broker-unavailable",
  "title": "Bad Gateway",
  "status": 502,
  "detail": "Failed to deliver message to queue 'orders.queue': Connection refused",
  "instance": "/api/orders"
}
```

### 504 Gateway Timeout

Returned when a synchronous sender's timeout expires before the receiver delivers a downstream response.

```json
{
  "type": "urn:restqa:error:synchronous-timeout",
  "title": "Gateway Timeout",
  "status": 504,
  "detail": "Synchronous response not received within 30s for receiver 'order-processor'.",
  "instance": "/api/orders"
}
```

---

## Receiver Error Behaviour

The receiver does not expose HTTP endpoints itself — it consumes messages and delivers them via HTTP POST. Errors are handled internally through retry and dead-letter routing.

For the complete retry flow, backoff configuration, and DLQ behaviour, see [Retry & Dead-Letter Queue](features/retry-and-dlq.md).

For message expiry behaviour, see [Message Time-to-Live](features/time-to-live.md).

### Key Points

- Failed deliveries are retried with configurable backoff (see [configuration](features/retry-and-dlq.md#configuration))
- After exhausting retries, messages are rejected to the broker's native DLQ
- Synchronous messages skip retry and go directly to DLQ on failure
- The `X-Retry-Count` header (zero-based) is injected on every async delivery
- Messages exceeding `time-to-live` are silently discarded

---

## Internal Error Handling Pattern

RESTAQ uses [Arrow's `Either`](https://arrow-kt.io/docs/apidocs/arrow-core/arrow.core/-either/) for explicit error propagation in the service layer:

```kotlin
// Service layer returns Either
fun sendToQueue(payload: ByteArray, headers: Map<String, String>): Either<ProblemDetail, Unit>

// Controller folds into HTTP response
result.fold(
    ifLeft = { problem -> problemResponse(problem) },
    ifRight = { ServerResponse.accepted().build() },
)
```

This avoids exception-driven control flow and makes error paths visible in the type system.

---

## Logging Levels

| Event | Level | Stacktrace |
|-------|-------|------------|
| Sender: request received | INFO | No |
| Sender: message placed on queue | INFO | No |
| Sender: broker error | ERROR | Yes |
| Receiver: message consumed from queue | INFO | No |
| Receiver: callback delivered successfully | INFO | No |
| Receiver: retry attempt | WARN | No |
| Receiver: all retries exhausted | ERROR | Yes |
| All other operational details | DEBUG | — |

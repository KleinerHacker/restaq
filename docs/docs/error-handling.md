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

The receiver does not expose HTTP endpoints itself — it consumes messages and delivers them via HTTP POST. Errors are handled internally:

### Retry Flow (Asynchronous Messages)

1. Message consumed from queue
2. HTTP POST to target URL
3. On failure (non-2xx or connection error):
    - Increment retry count
    - Wait for `backoff-period`
    - Redeliver the message
4. After `max-retries` exhausted:
    - Log ERROR with stacktrace
    - Reject the message → broker routes to Dead Letter Queue

### Synchronous Message Failures

Synchronous messages (identified by the presence of `X-Restqa-Correlation-Id`) do **not** use retry logic. On delivery failure:

1. Message consumed from queue
2. HTTP POST to target
3. On failure (non-2xx or connection error):
    - Log ERROR with stacktrace
    - Reject the message → broker routes directly to Dead Letter Queue

No `X-Retry-Count` header is injected for synchronous messages.

### X-Retry-Count Header

Every outgoing HTTP request from the receiver for **asynchronous** messages includes the `X-Retry-Count` header (zero-based integer). This allows downstream services to detect redeliveries:

```
X-Retry-Count: 0   (first attempt)
X-Retry-Count: 1   (first retry)
X-Retry-Count: 2   (second retry)
```

This header is **not** present on synchronous message deliveries.

### Time-to-Live

If `time-to-live` is configured and a message's age exceeds it, the message is discarded without delivery. This prevents stale data from reaching downstream systems.

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

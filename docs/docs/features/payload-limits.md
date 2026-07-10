# Payload Limits

## Overview

RESTAQ supports configurable payload size limits to prevent oversized messages from reaching the messaging infrastructure. When a request exceeds the configured maximum, it is rejected immediately with a **413 Content Too Large** response.

This protects both the broker and downstream consumers from unexpectedly large payloads.

---

## How It Works

1. A client sends an HTTP POST to a sender endpoint.
2. Before publishing to the queue, the sender checks the request body size against the configured limit.
3. If the payload exceeds the limit, the request is rejected with a **413** response containing RFC 9457 Problem Details.
4. If within limits (or no limit configured), the message proceeds normally.

!!! note
    Payload limits apply to **sender endpoints only**. There is no size restriction on the receiver side — messages already on the queue are delivered regardless of size.

---

## Response on Violation

When a payload exceeds the configured limit, the sender returns:

- **Status:** `413 Content Too Large`
- **Body:** RFC 9457 Problem Details JSON

```json
{
  "type": "about:blank",
  "title": "Content Too Large",
  "status": 413,
  "detail": "Payload size exceeds the configured maximum of 10MB"
}
```

---

## Content Type Agnostic

The payload limit applies to **any content type**. RESTAQ does not restrict or inspect the content type of incoming requests — it only enforces the byte size of the request body.

---

## Configuration

Set the maximum payload size globally for all sender endpoints:

```yaml
restqa:
  max-payload-size: 10MB
```

### Properties

| Property | Description | Required |
|----------|-------------|----------|
| `restqa.max-payload-size` | Maximum request body size (e.g., `1MB`, `512KB`, `10MB`) | No |

!!! tip
    If `max-payload-size` is not configured, no size restriction is enforced and payloads of any size are accepted.

---

## Example

```yaml
restqa:
  type: amqp
  max-payload-size: 5MB
  sender:
    documents:
      rest:
        path: /api/documents
      queue:
        name: documents.queue
    events:
      rest:
        path: /api/events
      queue:
        name: events.queue
```

In this example, both `/api/documents` and `/api/events` enforce a 5MB payload limit. Requests with bodies larger than 5MB receive a 413 response.

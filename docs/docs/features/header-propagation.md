# Header Propagation

## Overview

RESTAQ transparently propagates HTTP headers between REST endpoints and queue messages. When a sender receives an HTTP request, the headers are mapped to queue message properties. When a receiver delivers a message, those properties are restored as HTTP headers on the outgoing POST request.

This ensures that metadata such as content type, trace identifiers, and custom application headers travel end-to-end through the queue without manual configuration.

---

## How It Works

```
Client Headers ──▶ Sender ──filter──▶ Message Properties ──▶ Receiver ──▶ Target Headers
```

1. **Sender (inbound):** HTTP request headers are filtered and stored as queue message properties.
2. **Receiver (outbound):** Queue message properties are mapped back to HTTP headers on the POST to the target.
3. **Injected headers:** The receiver adds `X-Retry-Count` to every delivery.

---

## Filtering

Not all HTTP headers are appropriate to propagate through a queue. Headers that are specific to the transport layer, TLS session, or proxy infrastructure are excluded.

### Excluded Header Categories

| Category | Reason |
|----------|--------|
| **TLS / Certificate** | Tied to the sender's TLS session, not meaningful for downstream |
| **Transport** | Hop-by-hop headers, not valid end-to-end |
| **Proxy** | Proxy-specific metadata, not relevant after the queue |
| **Forwarding** | Describes the original client path, not the queue-mediated path |

### Specific Excluded Headers

| Header | Category |
|--------|----------|
| `X-Forwarded-For` | Forwarding |
| `X-Forwarded-Proto` | Forwarding |
| `X-Forwarded-Host` | Forwarding |
| `X-Forwarded-Port` | Forwarding |
| `Forwarded` | Forwarding |
| `X-Real-IP` | Forwarding |
| `X-Client-Cert` | TLS |
| `X-Client-Cert-Chain` | TLS |
| `X-SSL-Client-Cert` | TLS |
| `X-SSL-Client-S-DN` | TLS |
| `X-SSL-Client-I-DN` | TLS |
| `X-SSL-Client-Verify` | TLS |
| `X-SSL-Protocol` | TLS |
| `X-SSL-Cipher` | TLS |
| `Connection` | Transport |
| `Keep-Alive` | Transport |
| `Transfer-Encoding` | Transport |
| `TE` | Transport |
| `Trailer` | Transport |
| `Upgrade` | Transport |
| `Host` | Transport |
| `Content-Length` | Transport |
| `Proxy-Authorization` | Proxy |
| `Proxy-Authenticate` | Proxy |
| `Proxy-Connection` | Proxy |

### Always Propagated

| Header | Reason |
|--------|--------|
| `Content-Type` | Essential for the receiver to correctly set the outgoing request content type |

---

## Injected Headers

The receiver injects additional headers on every delivery:

| Header | Description |
|--------|-------------|
| `X-Retry-Count` | Zero-based retry attempt counter |

---

## Why Filtering Matters

!!! warning "Security"
    TLS client certificate headers (e.g., `X-Client-Cert`) describe the authentication state of the *original* sender connection. Propagating them through a queue would allow downstream systems to incorrectly trust a certificate assertion that was not established with them.

!!! warning "Correctness"
    Transport headers like `Connection`, `Transfer-Encoding`, and `Content-Length` are hop-by-hop — they describe the HTTP connection between two adjacent nodes. Forwarding them through a queue and reattaching them to a new HTTP request produces invalid or misleading metadata.

!!! info "Forwarding Headers"
    Headers like `X-Forwarded-For` describe the original client's network path. After traversing a queue, this information is no longer accurate or useful, and could confuse downstream reverse proxy logic.

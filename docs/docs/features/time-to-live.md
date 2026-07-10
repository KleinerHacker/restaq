# Time-to-Live

## Overview

RESTAQ supports message time-to-live (TTL) enforcement on the receiver side. Messages that exceed their maximum age are discarded without delivery, preventing stale or outdated data from reaching downstream systems.

This is particularly useful for time-sensitive operations where delivering an expired message could cause incorrect behaviour or unnecessary processing.

---

## How It Works

```
Queue ──▶ Receiver ──▶ TTL Check ──▶ Expired? ──Yes──▶ Discard (acknowledge)
                                        │
                                        No
                                        │
                                        ▼
                                   Deliver to Target
```

1. The receiver picks up a message from the queue.
2. Before delivery, it calculates the message age using the **message timestamp**.
3. If the age exceeds the configured `time-to-live`, the message is **discarded** (acknowledged without delivery).
4. If within TTL, the message proceeds to normal delivery.

### Age Calculation

```
message age = current time − message timestamp
```

The message timestamp is set by the broker when the message is originally published.

!!! note
    TTL is checked **before** delivery. An expired message never reaches the target URL — it is silently acknowledged and removed from the queue.

---

## Use Cases

| Scenario | Example |
|----------|---------|
| **Price updates** | A stock price older than 5 seconds is no longer relevant |
| **Session tokens** | A password reset link message older than 15 minutes should not be delivered |
| **Real-time notifications** | A push notification older than 1 minute loses its urgency |
| **Queue backlog recovery** | After a downstream outage, skip messages that accumulated beyond a useful age |

---

## Configuration

TTL is configured per receiver:

```yaml
restqa:
  type: amqp
  receiver:
    price-updates:
      rest:
        url: http://trading-service:8080/prices
      queue:
        name: price-updates.queue
      time-to-live: 5s
      timeout: 10s
    notifications:
      rest:
        url: http://notification-service:8080/push
      queue:
        name: notifications.queue
      time-to-live: 1m
      retry:
        max-retries: 3
        backoff-period: 5s
      timeout: 15s
```

### Properties

| Property | Description | Required |
|----------|-------------|----------|
| `time-to-live` | Maximum message age (e.g., `5s`, `1m`, `1h`, `24h`) | No |

!!! tip
    If `time-to-live` is not configured, messages are delivered regardless of age.

---

## Example

```yaml
restqa:
  type: amqp
  receiver:
    commands:
      rest:
        url: http://device-controller:8080/execute
      queue:
        name: device-commands.queue
      time-to-live: 30s
      retry:
        max-retries: 2
        backoff-period: 5s
      timeout: 10s
```

In this example, device commands older than 30 seconds are discarded. This prevents stale commands from being executed on a device after a queue backlog clears.

!!! warning
    TTL discard is silent — the message is acknowledged without generating an error or routing to the DLQ. If you need visibility into discarded messages, implement monitoring on the broker side.

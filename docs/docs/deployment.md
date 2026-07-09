# Deployment

## Building

RESTAQ is a Kotlin / Spring Boot application built with Gradle (Kotlin DSL). A Gradle wrapper is included.

### Prerequisites

- **JDK 25** (the project uses a Java toolchain targeting Java 25)
- **Docker** (optional, for integration tests via Testcontainers)

### Build Commands

=== "Linux / macOS"

    ```bash
    ./gradlew build
    ```

=== "Windows"

    ```powershell
    .\gradlew.bat build
    ```

### Executable JAR

```bash
./gradlew bootJar
```

Produces: `build/libs/restqa-0.0.1-SNAPSHOT.jar`

---

## Running

### Gradle

```bash
./gradlew bootRun
```

### JAR

```bash
java -jar build/libs/restqa-0.0.1-SNAPSHOT.jar
```

### Docker

```dockerfile
FROM eclipse-temurin:25-jre-alpine
COPY build/libs/restqa-0.0.1-SNAPSHOT.jar /app/restqa.jar
ENTRYPOINT ["java", "-jar", "/app/restqa.jar"]
```

```bash
docker build -t restqa .
docker run -p 8080:8080 \
  -e SPRING_RABBITMQ_HOST=broker \
  -e RESTQA_TYPE=amqp \
  restqa
```

---

## Environment Variables

All configuration properties can be set via environment variables using Spring Boot's relaxed binding:

| Property | Environment Variable |
|----------|---------------------|
| `restqa.type` | `RESTQA_TYPE` |
| `restqa.max-payload-size` | `RESTQA_MAX_PAYLOAD_SIZE` |
| `restqa.sender.orders.rest.path` | `RESTQA_SENDER_ORDERS_REST_PATH` |
| `restqa.receiver.notify.rest.url` | `RESTQA_RECEIVER_NOTIFY_REST_URL` |
| `spring.rabbitmq.host` | `SPRING_RABBITMQ_HOST` |

---

## Typical Deployment Topology

### DMZ Gateway

```
Internet
    │
    │ HTTPS (TLS termination at load balancer)
    ▼
┌──────────────┐
│ RESTAQ       │  sender mode
│ (DMZ)        │
└──────────────┘
    │
    │ AMQP (internal network)
    ▼
┌──────────────┐
│ RabbitMQ     │
└──────────────┘
    │
    │ AMQP
    ▼
┌──────────────┐
│ RESTAQ       │  receiver mode
│ (Internal)   │
└──────────────┘
    │
    │ HTTP POST
    ▼
┌──────────────┐
│ Application  │
└──────────────┘
```

### Combined Instance

A single RESTAQ instance can serve both sender and receiver flows simultaneously.

---

## Health & Monitoring

RESTAQ uses Spring Boot Actuator (if enabled) for health checks:

```yaml
management:
  endpoints:
    web:
      exposure:
        include: health, info
```

---

## Scaling

RESTAQ is stateless. Scale horizontally by running multiple instances behind a load balancer. The message broker handles distribution:

- **Sender:** Multiple instances accept requests; all publish to the same queue.
- **Receiver:** Multiple instances consume from the same queue; the broker distributes messages (competing consumers pattern).

---

## Testing

### Run Unit Tests

```bash
./gradlew test
```

### Run Integration Tests

Integration tests require Docker (Testcontainers starts broker containers automatically):

```bash
./gradlew test
```

Test infrastructure:

- **Testcontainers** – RabbitMQ and ActiveMQ Artemis containers
- **WireMock** – HTTP callback endpoint stubs
- **Awaitility** – Async assertion timeouts

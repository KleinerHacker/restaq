# CLAUDE.md

## Project: RESTAQ

RESTAQ ("REST in. Queue on.") is a lightweight Spring Boot messaging gateway that
bridges synchronous HTTP and asynchronous message-oriented middleware (AMQP, JMS).
It forwards incoming REST requests into queues (sender side) and consumes queue
messages to invoke downstream HTTP endpoints (receiver side).

## Tech Stack

- **Language:** Kotlin (JVM)
- **Framework:** Spring Boot 4.1.0
- **Build:** Gradle (Kotlin DSL) via the wrapper (`./gradlew` / `gradlew.bat`)
- **JDK:** Java 25 (toolchain)
- **Group / base package:** `org.pcsoft.micro.restqa`

## Build & Run

```bash
./gradlew build      # compile + test + package
./gradlew test       # tests only
./gradlew bootRun    # run the app
./gradlew bootJar    # executable Spring Boot JAR -> build/libs/
```

## Package Structure (Domain Package Approach)

The codebase is organized by **domain**. There are two domains under the base
package `org.pcsoft.micro.restqa`:

- **`send`** — REST-to-Queue sender side
- **`receive`** — Queue-to-REST receiver side

Within each domain, classes are placed by their role:

| Sub-package      | Contents                                                                 |
| ---------------- | ------------------------------------------------------------------------ |
| `port`           | REST controllers                                                         |
| `configuration`  | Spring configuration classes                                             |
| `service`        | Services used **only inside** the domain (internal)                      |
| *(domain root)*  | Services used **outside** the domain (public API of the domain)          |

Example layout:

```
org.pcsoft.micro.restqa
├── send
│   ├── port            (REST controllers)
│   ├── configuration   (config)
│   ├── service         (domain-internal services)
│   └── SendService.kt  (service used outside the domain -> domain root)
└── receive
    ├── port
    ├── configuration
    ├── service
    └── ReceiveService.kt
```

### Placement rule of thumb

- REST controller → `port`
- Config class → `configuration`
- Service only used within its own domain → `service`
- Service used by another domain (cross-domain) → directly in the domain root

**If unsure where a class belongs, ask before placing it.**

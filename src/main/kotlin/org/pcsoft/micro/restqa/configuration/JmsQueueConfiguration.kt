package org.pcsoft.micro.restqa.configuration

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Configuration

/**
 * Activates the JMS/Artemis message-queue client when `restqa.queue.type` is `jms`.
 * Broker connectivity is configured through the Spring Boot standard
 * `spring.artemis.*` properties.
 */
@Configuration
@ConditionalOnProperty(prefix = "restqa.queue", name = ["type"], havingValue = "jms")
class JmsQueueConfiguration

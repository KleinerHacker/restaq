package org.pcsoft.micro.restqa.configuration

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Configuration

/**
 * Activates the AMQP/RabbitMQ message-queue client when `restqa.queue.type` is `amqp`
 * (the default when the property is absent). Broker connectivity itself is configured
 * through the Spring Boot standard `spring.rabbitmq.*` properties.
 */
@Configuration
@ConditionalOnProperty(prefix = "restqa.queue", name = ["type"], havingValue = "amqp", matchIfMissing = true)
class AmqpQueueConfiguration

package org.pcsoft.micro.restqa.configuration

import jakarta.annotation.PostConstruct
import jakarta.jms.ConnectionFactory
import org.pcsoft.micro.restqa.internal.utils.logger
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.jms.core.JmsTemplate

/**
 * Activates the JMS/Artemis message-queue client when `restqa.type` is `jms`.
 * Broker connectivity is configured through the Spring Boot standard
 * `spring.artemis.*` properties.
 *
 * Artemis auto-creates the queues on first use, so there is no declarative broker-admin
 * step like for AMQP. This configuration provides a queue-domain [JmsTemplate] and
 * validates/logs the configured destination names at startup.
 */
@Configuration
@ConditionalOnProperty(prefix = "restqa", name = ["type"], havingValue = "jms")
class JmsQueueConfiguration(
    private val properties: RestqaProperties,
) {

    companion object {
        private val log = logger()
    }

    @PostConstruct
    private fun validateDestinations() {
        val destinations = (properties.sender.values.map { it.queue.name } +
            properties.receiver.values.map { it.queue.name }).distinct()

        destinations.forEach { name ->
            require(name.isNotBlank()) { "Configured JMS destination name must not be blank" }
            log.info("Configured JMS destination '{}' (auto-created by the broker on first use)", name)
        }
    }

    /**
     * Provides a [JmsTemplate] configured for point-to-point (queue) messaging.
     * The template's `pubSubDomain` is set to `false` so that all destinations are
     * treated as queues rather than topics.
     */
    @Bean
    fun jmsTemplate(connectionFactory: ConnectionFactory): JmsTemplate =
        JmsTemplate(connectionFactory).apply {
            // RESTAQ bridges to queues, never topics.
            isPubSubDomain = false
        }
}

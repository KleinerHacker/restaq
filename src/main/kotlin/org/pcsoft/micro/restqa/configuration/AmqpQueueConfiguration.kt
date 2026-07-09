package org.pcsoft.micro.restqa.configuration

import org.pcsoft.micro.restqa.internal.utils.logger
import org.springframework.amqp.core.Binding
import org.springframework.amqp.core.BindingBuilder
import org.springframework.amqp.core.Declarable
import org.springframework.amqp.core.Declarables
import org.springframework.amqp.core.DirectExchange
import org.springframework.amqp.core.Queue
import org.springframework.amqp.core.QueueBuilder
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

/**
 * Activates the AMQP/RabbitMQ message-queue client when `restqa.type` is `amqp`
 * (the default when the property is absent). Broker connectivity itself is configured
 * through the Spring Boot standard `spring.rabbitmq.*` properties.
 *
 * Declares the queues (and, where configured, exchanges + bindings) for every sender
 * and receiver flow. All queues are durable **quorum** queues. The auto-configured
 * `RabbitAdmin` picks up the [Declarables] bean and declares everything on the broker
 * once a connection is established.
 */
@Configuration
@ConditionalOnProperty(prefix = "restqa", name = ["type"], havingValue = "amqp", matchIfMissing = true)
class AmqpQueueConfiguration(
    private val properties: RestqaProperties,
) {

    companion object {
        private val log = logger()
    }

    @Bean
    fun restqaAmqpDeclarables(): Declarables {
        val endpoints = (properties.sender.values.map { it.queue } + properties.receiver.values.map { it.queue })

        val queues = HashMap<String, Queue>()
        val exchanges = HashMap<String, DirectExchange>()
        val bindings = ArrayList<Binding>()

        for (endpoint in endpoints) {
            val queue = queues.getOrPut(endpoint.name) {
                log.info("Declaring AMQP quorum queue '{}'", endpoint.name)
                QueueBuilder.durable(endpoint.name).quorum().build()
            }

            val exchangeName = endpoint.exchange ?: continue
            val exchange = exchanges.getOrPut(exchangeName) {
                log.info("Declaring AMQP direct exchange '{}'", exchangeName)
                DirectExchange(exchangeName, true, false)
            }
            val routingKey = endpoint.routingKey ?: endpoint.name
            log.info("Binding queue '{}' to exchange '{}' with routing key '{}'", endpoint.name, exchangeName, routingKey)
            bindings += BindingBuilder.bind(queue).to(exchange).with(routingKey)
        }

        val declarables = ArrayList<Declarable>()
        declarables += queues.values
        declarables += exchanges.values
        declarables += bindings
        return Declarables(declarables)
    }
}

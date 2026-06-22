package org.pcsoft.micro.restqa.receive.controller

import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import org.pcsoft.micro.restqa.configuration.ReceiverProperties
import org.pcsoft.micro.restqa.configuration.RestqaProperties
import org.pcsoft.micro.restqa.internal.utils.logger
import org.pcsoft.micro.restqa.receive.port.ReceiverEndpointController
import org.slf4j.MDC
import org.springframework.amqp.core.Message
import org.springframework.amqp.core.MessageListener
import org.springframework.amqp.rabbit.connection.ConnectionFactory
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClient

/**
 * Consumes messages from the AMQP queue of every configured receiver and forwards them
 * to the receiver's HTTP endpoint via [ReceiverEndpointController].
 *
 * Active when `restqa.queue.type` is `amqp` (the default). One
 * [SimpleMessageListenerContainer] is registered per `restqa.receiver.<key>` entry; the
 * receiver key is exposed to logging through the MDC (field `receiver`).
 */
@Component
@ConditionalOnProperty(prefix = "restqa.queue", name = ["type"], havingValue = "amqp", matchIfMissing = true)
class AmqpQueueConsumer(
    private val connectionFactory: ConnectionFactory,
    private val properties: RestqaProperties,
    private val webClientBuilder: WebClient.Builder,
) {

    companion object {
        private val log = logger()

        /** MDC field carrying the configured receiver key during message handling. */
        const val MDC_RECEIVER = "receiver"
    }

    private val containers = mutableListOf<SimpleMessageListenerContainer>()

    /**
     * Builds (but does not start) one listener container per configured receiver.
     */
    private fun buildContainers(): List<SimpleMessageListenerContainer> =
        properties.receiver.map { (receiverKey, receiverProperties) ->
            val controller = ReceiverEndpointController(receiverProperties, webClientBuilder.clone().build())
            SimpleMessageListenerContainer(connectionFactory).apply {
                setQueueNames(receiverProperties.queue.name)
                setMessageListener(listener(receiverKey, receiverProperties, controller))
            }
        }

    private fun listener(
        receiverKey: String,
        receiverProperties: ReceiverProperties,
        controller: ReceiverEndpointController,
    ) = MessageListener { message: Message ->
        MDC.putCloseable(MDC_RECEIVER, receiverKey).use {
            log.debug("Received message from queue '{}'", receiverProperties.queue.name)
            controller.forward(message.body, headersOf(message)).block()
        }
    }

    private fun headersOf(message: Message): Map<String, String> {
        val props = message.messageProperties
        val headers = props.headers.entries
            .filter { it.value != null }
            .associate { it.key to it.value.toString() }
            .toMutableMap()
        props.contentType.let { headers["Content-Type"] = it }
        return headers
    }

    @PostConstruct
    internal fun start() {
        containers += buildContainers()
        containers.forEach {
            it.afterPropertiesSet()
            it.start()
        }
    }

    @PreDestroy
    internal fun stop() {
        containers.forEach { it.stop() }
        containers.clear()
    }
}

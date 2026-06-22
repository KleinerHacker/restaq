package org.pcsoft.micro.restqa.receive.controller

import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import jakarta.jms.BytesMessage
import jakarta.jms.ConnectionFactory
import jakarta.jms.Message
import jakarta.jms.MessageListener
import jakarta.jms.TextMessage
import org.pcsoft.micro.restqa.configuration.ReceiverProperties
import org.pcsoft.micro.restqa.configuration.RestqaProperties
import org.pcsoft.micro.restqa.internal.utils.logger
import org.pcsoft.micro.restqa.receive.port.ReceiverEndpointController
import org.slf4j.MDC
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.jms.listener.DefaultMessageListenerContainer
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClient

/**
 * Consumes messages from the JMS destination of every configured receiver and forwards
 * them to the receiver's HTTP endpoint via [ReceiverEndpointController].
 *
 * Active when `restqa.queue.type` is `jms`. One [DefaultMessageListenerContainer] is
 * registered per `restqa.receiver.<key>` entry; the receiver key is exposed to logging
 * through the MDC (field `receiver`).
 */
@Component
@ConditionalOnProperty(prefix = "restqa.queue", name = ["type"], havingValue = "jms")
class JmsQueueConsumer(
    private val connectionFactory: ConnectionFactory,
    private val properties: RestqaProperties,
    private val webClientBuilder: WebClient.Builder,
) {

    companion object {
        private val log = logger()

        /** MDC field carrying the configured receiver key during message handling. */
        const val MDC_RECEIVER = "receiver"
    }

    private val containers = mutableListOf<DefaultMessageListenerContainer>()

    /**
     * Builds (but does not start) one listener container per configured receiver.
     */
    private fun buildContainers(): List<DefaultMessageListenerContainer> =
        properties.receiver.map { (receiverKey, receiverProperties) ->
            val controller = ReceiverEndpointController(receiverProperties, webClientBuilder.clone().build())
            DefaultMessageListenerContainer().apply {
                setConnectionFactory(this@JmsQueueConsumer.connectionFactory)
                destinationName = receiverProperties.queue.name
                isPubSubDomain = false
                messageListener = listener(receiverKey, receiverProperties, controller)
            }
        }

    private fun listener(
        receiverKey: String,
        receiverProperties: ReceiverProperties,
        controller: ReceiverEndpointController,
    ) = MessageListener { message: Message ->
        MDC.putCloseable(MDC_RECEIVER, receiverKey).use {
            log.debug("Received message from destination '{}'", receiverProperties.queue.name)
            controller.forward(payloadOf(message), headersOf(message)).block()
        }
    }

    private fun payloadOf(message: Message): ByteArray = when (message) {
        is BytesMessage -> ByteArray(message.bodyLength.toInt()).also { message.readBytes(it) }
        is TextMessage -> (message.text ?: "").toByteArray()
        else -> ByteArray(0)
    }

    private fun headersOf(message: Message): Map<String, String> = buildMap {
        val names = message.propertyNames
        while (names.hasMoreElements()) {
            val name = names.nextElement() as String
            message.getObjectProperty(name)?.let { put(name, it.toString()) }
        }
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

package org.pcsoft.micro.restqa.receive.port

import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import jakarta.jms.BytesMessage
import jakarta.jms.ConnectionFactory
import jakarta.jms.Message
import jakarta.jms.MessageListener
import jakarta.jms.TextMessage
import org.pcsoft.micro.restqa.configuration.ReceiverProperties
import org.pcsoft.micro.restqa.configuration.RestqaProperties
import org.pcsoft.micro.restqa.internal.SynchronousResponseRegistry
import org.pcsoft.micro.restqa.internal.utils.logger
import org.slf4j.MDC
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.jms.listener.DefaultMessageListenerContainer
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClient
import java.time.Duration
import java.time.Instant

/**
 * Consumes messages from the JMS destination of every configured receiver and forwards
 * them to the receiver's HTTP endpoint via [ReceiverEndpointController].
 *
 * Active when `restqa.type` is `jms`. One [DefaultMessageListenerContainer] is
 * registered per `restqa.receiver.<key>` entry; the receiver key is exposed to logging
 * through the MDC (field `receiver`).
 *
 * Retry behaviour:
 * - On delivery failure, the message listener throws an exception, causing the
 *   transacted JMS session to roll back and redeliver the message.
 * - The retry count is tracked via the `JMSXDeliveryCount` standard property (broker-managed,
 *   1-based on first delivery) combined with a custom property fallback.
 * - After [ReceiverProperties.retry.maxRetries] attempts, the exception propagates without
 *   backoff, letting the broker route the message to its configured DLQ.
 * - If [ReceiverProperties.timeToLive] is set, messages older than this threshold are
 *   acknowledged without delivery to avoid stale forwarding.
 */
@Component
@ConditionalOnProperty(prefix = "restqa", name = ["type"], havingValue = "jms")
class JmsQueueConsumer(
    private val connectionFactory: ConnectionFactory,
    private val properties: RestqaProperties,
    private val webClientBuilder: WebClient.Builder,
    private val synchronousRegistry: SynchronousResponseRegistry? = null,
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
    internal fun buildContainers(): List<DefaultMessageListenerContainer> =
        properties.receiver.map { (receiverKey, receiverProperties) ->
            val controller = ReceiverEndpointController(receiverProperties, webClientBuilder.clone().build(), synchronousRegistry)
            DefaultMessageListenerContainer().apply {
                setConnectionFactory(this@JmsQueueConsumer.connectionFactory)
                destinationName = receiverProperties.queue.name
                isPubSubDomain = false
                isSessionTransacted = true
                // Use a short recovery interval so that shutdown is not blocked by long sleeps.
                setRecoveryInterval(1000)
                messageListener = listener(receiverKey, receiverProperties, controller)
            }
        }

    private fun listener(
        receiverKey: String,
        receiverProperties: ReceiverProperties,
        controller: ReceiverEndpointController,
    ) = MessageListener { message: Message ->
        MDC.putCloseable(MDC_RECEIVER, receiverKey).use {
            val headers = headersOf(message)
            val isSynchronous = headers.containsKey(SynchronousResponseRegistry.HEADER_CORRELATION_ID)
            val retryCount = if (isSynchronous) 0 else getRetryCount(message)

            // TTL check: acknowledge stale messages without forwarding.
            if (isExpired(message, receiverProperties.timeToLive)) {
                log.warn("Message expired (TTL exceeded), acknowledging without delivery")
                message.acknowledge()
                return@use
            }

            log.info("Message consumed from destination '{}' (attempt {})", receiverProperties.queue.name, retryCount)

            val result = controller.forward(payloadOf(message), headers, retryCount)

            result.fold(
                ifLeft = { ex ->
                    if (isSynchronous) {
                        // Synchronous: no retry, throw immediately to let broker route to DLQ.
                        log.error("Synchronous message delivery failed for destination '{}', routing to DLQ", receiverProperties.queue.name, ex)
                        throw RuntimeException("Synchronous delivery failed", ex)
                    } else {
                        handleFailure(retryCount, receiverProperties, ex)
                    }
                },
                ifRight = {
                    // Success – transacted session will commit automatically.
                },
            )
        }
    }

    private fun handleFailure(
        retryCount: Int,
        receiverProperties: ReceiverProperties,
        ex: Exception,
    ) {
        val maxRetries = receiverProperties.retry.maxRetries
        if (retryCount >= maxRetries) {
            log.error(
                "All {} retries exhausted for destination '{}', message will be moved to DLQ by broker",
                maxRetries, receiverProperties.queue.name, ex,
            )
        } else {
            log.warn(
                "Delivery attempt {} failed for destination '{}': {}. Retrying after {}.",
                retryCount, receiverProperties.queue.name, ex.message, receiverProperties.retry.backoffPeriod,
            )
            // Backoff before rollback/redeliver.
            Thread.sleep(receiverProperties.retry.backoffPeriod.toMillis())
        }
        // Throw to trigger session rollback → broker redelivers or routes to DLQ.
        throw RuntimeException("Delivery failed after attempt $retryCount", ex)
    }

    /**
     * Returns the zero-based retry count. JMS brokers expose `JMSXDeliveryCount` which is
     * 1-based on first delivery. We convert to 0-based.
     */
    private fun getRetryCount(message: Message): Int {
        val deliveryCount = try {
            message.getIntProperty("JMSXDeliveryCount")
        } catch (_: Exception) {
            1
        }
        return (deliveryCount - 1).coerceAtLeast(0)
    }

    private fun isExpired(message: Message, ttl: Duration?): Boolean {
        if (ttl == null) return false
        val timestamp = message.jmsTimestamp
        if (timestamp == 0L) return false
        return Instant.now().isAfter(Instant.ofEpochMilli(timestamp).plus(ttl))
    }

    internal fun payloadOf(message: Message): ByteArray = when (message) {
        is BytesMessage -> ByteArray(message.bodyLength.toInt()).also { message.readBytes(it) }
        is TextMessage -> (message.text ?: "").toByteArray()
        else -> ByteArray(0)
    }

    internal fun headersOf(message: Message): Map<String, String> = buildMap {
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
        containers.forEach {
            it.stop()
            it.shutdown()
        }
        containers.clear()
    }
}

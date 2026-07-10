package org.pcsoft.micro.restqa.receive.port

import com.rabbitmq.client.Channel
import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import org.pcsoft.micro.restqa.configuration.ReceiverProperties
import org.pcsoft.micro.restqa.configuration.RestqaProperties
import org.pcsoft.micro.restqa.internal.SynchronousResponseRegistry
import org.pcsoft.micro.restqa.internal.utils.logger
import org.slf4j.MDC
import org.springframework.amqp.core.AcknowledgeMode
import org.springframework.amqp.core.Message
import org.springframework.amqp.rabbit.connection.ConnectionFactory
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClient
import java.time.Duration
import java.time.Instant

/**
 * Consumes messages from the AMQP queue of every configured receiver and forwards them
 * to the receiver's HTTP endpoint via [ReceiverEndpointController].
 *
 * Active when `restqa.type` is `amqp` (the default). One
 * [SimpleMessageListenerContainer] is registered per `restqa.receiver.<key>` entry; the
 * receiver key is exposed to logging through the MDC (field `receiver`).
 *
 * Retry behaviour:
 * - On delivery failure, the message is nacked with a backoff delay, then redelivered.
 * - The retry count is tracked via a custom AMQP header (`x-restqa-retry-count`).
 * - After [ReceiverProperties.retry.maxRetries] attempts, the message is rejected
 *   (requeue=false), routing it to the broker's Dead Letter Exchange if configured.
 * - If [ReceiverProperties.timeToLive] is set, messages older than this threshold are
 *   discarded (rejected without requeue) to avoid stale delivery.
 */
@Component
@ConditionalOnProperty(prefix = "restqa", name = ["type"], havingValue = "amqp", matchIfMissing = true)
class AmqpQueueConsumer(
    private val connectionFactory: ConnectionFactory,
    private val properties: RestqaProperties,
    private val webClientBuilder: WebClient.Builder,
    private val synchronousRegistry: SynchronousResponseRegistry? = null,
) {

    companion object {
        private val log = logger()

        /** MDC field carrying the configured receiver key during message handling. */
        const val MDC_RECEIVER = "receiver"

        /** Custom AMQP header to track the retry count across redeliveries. */
        const val HEADER_RETRY_COUNT = "x-restqa-retry-count"
    }

    private val containers = mutableListOf<SimpleMessageListenerContainer>()

    /**
     * Builds (but does not start) one listener container per configured receiver.
     */
    internal fun buildContainers(): List<SimpleMessageListenerContainer> =
        properties.receiver.map { (receiverKey, receiverProperties) ->
            val controller = ReceiverEndpointController(receiverProperties, webClientBuilder.clone().build(), synchronousRegistry)
            SimpleMessageListenerContainer(connectionFactory).apply {
                setQueueNames(receiverProperties.queue.name)
                acknowledgeMode = AcknowledgeMode.MANUAL
                setMissingQueuesFatal(false)
                setMessageListener(listener(receiverKey, receiverProperties, controller))
            }
        }

    private fun listener(
        receiverKey: String,
        receiverProperties: ReceiverProperties,
        controller: ReceiverEndpointController,
    ) = ChannelAwareMessageListener { message: Message, channel: Channel? ->
        MDC.putCloseable(MDC_RECEIVER, receiverKey).use {
            val deliveryTag = message.messageProperties.deliveryTag
            val headers = headersOf(message)
            val isSynchronous = headers.containsKey(SynchronousResponseRegistry.HEADER_CORRELATION_ID)
            val retryCount = if (isSynchronous) 0 else getRetryCount(message)

            // TTL check: discard stale messages.
            if (isExpired(message, receiverProperties.timeToLive)) {
                log.warn("Message expired (TTL exceeded), discarding to DLQ")
                channel?.basicReject(deliveryTag, false)
                return@use
            }

            log.info("Message consumed from queue '{}' (attempt {})", receiverProperties.queue.name, retryCount)

            val result = controller.forward(message.body, headers, retryCount)

            result.fold(
                ifLeft = { ex ->
                    if (isSynchronous) {
                        // Synchronous: no retry, reject directly to DLQ.
                        log.error("Synchronous message delivery failed for queue '{}', rejecting to DLQ", receiverProperties.queue.name, ex)
                        channel?.basicReject(deliveryTag, false)
                    } else {
                        handleFailure(channel, message, deliveryTag, retryCount, receiverProperties, ex)
                    }
                },
                ifRight = {
                    channel?.basicAck(deliveryTag, false)
                },
            )
        }
    }

    private fun handleFailure(
        channel: Channel?,
        message: Message,
        deliveryTag: Long,
        retryCount: Int,
        receiverProperties: ReceiverProperties,
        ex: Exception,
    ) {
        val maxRetries = receiverProperties.retry.maxRetries
        if (retryCount >= maxRetries) {
            log.error(
                "All {} retries exhausted for queue '{}', rejecting to DLQ",
                maxRetries, receiverProperties.queue.name, ex,
            )
            channel?.basicReject(deliveryTag, false)
        } else {
            log.warn(
                "Delivery attempt {} failed for queue '{}': {}. Retrying after {}.",
                retryCount, receiverProperties.queue.name, ex.message, receiverProperties.retry.backoffPeriod,
            )
            // Backoff before requeue.
            Thread.sleep(receiverProperties.retry.backoffPeriod.toMillis())
            // Increment retry count in message header and nack with requeue.
            message.messageProperties.setHeader(HEADER_RETRY_COUNT, retryCount + 1)
            channel?.basicNack(deliveryTag, false, true)
        }
    }

    private fun getRetryCount(message: Message): Int =
        (message.messageProperties.headers[HEADER_RETRY_COUNT] as? Number)?.toInt() ?: 0

    private fun isExpired(message: Message, ttl: Duration?): Boolean {
        if (ttl == null) return false
        val timestamp = message.messageProperties.timestamp?.toInstant() ?: return false
        return Instant.now().isAfter(timestamp.plus(ttl))
    }

    internal fun headersOf(message: Message): Map<String, String> {
        val props = message.messageProperties
        val headers = props.headers.entries
            .filter { it.value != null }
            .filter { it.key != HEADER_RETRY_COUNT } // Internal header, not propagated.
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
        containers.forEach {
            it.stop()
            it.destroy()
        }
        containers.clear()
    }
}

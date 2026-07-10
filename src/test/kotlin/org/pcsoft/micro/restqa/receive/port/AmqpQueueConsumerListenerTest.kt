package org.pcsoft.micro.restqa.receive.port

import com.rabbitmq.client.Channel
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.pcsoft.micro.restqa.configuration.*
import org.springframework.amqp.core.AcknowledgeMode
import org.springframework.amqp.core.Message
import org.springframework.amqp.core.MessageProperties
import org.springframework.amqp.rabbit.connection.ConnectionFactory
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener
import org.springframework.web.reactive.function.client.WebClient
import java.time.Duration
import java.util.*
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Unit tests for the internal listener logic of [AmqpQueueConsumer]:
 * - Retry success scenario (forward succeeds → basicAck)
 * - Retry failure with backoff → basicNack with requeue
 * - Retry exhausted → basicReject (DLQ routing)
 * - X-Retry-Count header propagation
 * - TTL-based message expiry
 * - headersOf extraction
 * - getRetryCount parsing
 */
class AmqpQueueConsumerListenerTest {

    private fun receiver(
        maxRetries: Int = 3,
        backoffPeriod: Duration = Duration.ofMillis(10),
        ttl: Duration? = null,
    ) = ReceiverProperties(
        rest = ReceiverRestProperties(url = "http://localhost:8080/callback"),
        queue = QueueEndpointProperties(name = "test.queue"),
        retry = RetryProperties(maxRetries = maxRetries, backoffPeriod = backoffPeriod),
        timeToLive = ttl,
    )

    private fun buildConsumerAndGetListener(
        receiverProperties: ReceiverProperties,
    ): Pair<AmqpQueueConsumer, ChannelAwareMessageListener> {
        val props = RestqaProperties(receiver = mapOf("test" to receiverProperties))
        val consumer = AmqpQueueConsumer(mock<ConnectionFactory>(), props, WebClient.builder())
        val containers = consumer.buildContainers()
        assertEquals(1, containers.size)
        assertEquals(AcknowledgeMode.MANUAL, containers[0].acknowledgeMode)
        // Extract the ChannelAwareMessageListener from the container.
        val listener = containers[0].messageListener as ChannelAwareMessageListener
        return consumer to listener
    }

    private fun amqpMessage(
        body: ByteArray = "hello".toByteArray(),
        headers: Map<String, Any> = emptyMap(),
        contentType: String? = null,
        deliveryTag: Long = 42L,
        timestamp: Date? = null,
    ): Message {
        val props = MessageProperties()
        props.setDeliveryTag(deliveryTag)
        headers.forEach { (k, v) -> props.setHeader(k, v) }
        contentType?.let { props.setContentType(it) }
        timestamp?.let { props.setTimestamp(it) }
        return Message(body, props)
    }

    // ─── Retry Success ────────────────────────────────────────────────────────────

    @Test
    fun `listener acks message on successful forward (first attempt)`() {
        val receiverProps = receiver()
        val (_, _) = buildConsumerAndGetListener(receiverProps)
        val channel = mock<Channel>()
        val message = amqpMessage(body = "payload".toByteArray())

        // The listener calls ReceiverEndpointController.forward() which uses a real WebClient.
        // We need to intercept this at the HTTP level. Since we can't easily mock the internal
        // WebClient here, we use a WireMock-like approach with a mock ExchangeFunction.
        // However, the listener instantiates ReceiverEndpointController internally. To test
        // the full flow, we use a real WebClient with a stubbed exchange function.
        // Unfortunately, the consumer constructs its own ReceiverEndpointController internally,
        // so we need to use a different approach: test with a WebClient.Builder that returns
        // a pre-configured WebClient with a mock exchange.

        // Let's use a custom WebClient.Builder that produces a WebClient with a stub response.
        val props = RestqaProperties(receiver = mapOf("test" to receiverProps))
        val exchangeFunction = org.springframework.web.reactive.function.client.ExchangeFunction {
            reactor.core.publisher.Mono.just(
                org.springframework.web.reactive.function.client.ClientResponse.create(org.springframework.http.HttpStatus.OK).build()
            )
        }
        val webClientBuilder = WebClient.builder().exchangeFunction(exchangeFunction)
        val consumer = AmqpQueueConsumer(mock<ConnectionFactory>(), props, webClientBuilder)
        val containers = consumer.buildContainers()
        val realListener = containers[0].messageListener as ChannelAwareMessageListener

        realListener.onMessage(message, channel)

        verify(channel).basicAck(42L, false)
        verify(channel, never()).basicNack(any(), any(), any())
        verify(channel, never()).basicReject(any(), any())
    }

    @Test
    fun `listener acks message on successful forward after retries (retryCount=2)`() {
        val receiverProps = receiver()
        val props = RestqaProperties(receiver = mapOf("test" to receiverProps))
        val exchangeFunction = org.springframework.web.reactive.function.client.ExchangeFunction {
            reactor.core.publisher.Mono.just(
                org.springframework.web.reactive.function.client.ClientResponse.create(org.springframework.http.HttpStatus.OK).build()
            )
        }
        val webClientBuilder = WebClient.builder().exchangeFunction(exchangeFunction)
        val consumer = AmqpQueueConsumer(mock<ConnectionFactory>(), props, webClientBuilder)
        val containers = consumer.buildContainers()
        val listener = containers[0].messageListener as ChannelAwareMessageListener
        val channel = mock<Channel>()

        // Message has been retried twice already.
        val message = amqpMessage(
            body = "retry-payload".toByteArray(),
            headers = mapOf(AmqpQueueConsumer.HEADER_RETRY_COUNT to 2),
        )

        listener.onMessage(message, channel)

        // Should ack successfully because forward succeeded.
        verify(channel).basicAck(42L, false)
        verify(channel, never()).basicReject(any(), any())
    }

    // ─── Retry Failure → Nack (requeue) ──────────────────────────────────────────

    @Test
    fun `listener nacks with requeue on forward failure when retries remain`() {
        val receiverProps = receiver(maxRetries = 3, backoffPeriod = Duration.ofMillis(1))
        val props = RestqaProperties(receiver = mapOf("test" to receiverProps))
        val exchangeFunction = org.springframework.web.reactive.function.client.ExchangeFunction {
            reactor.core.publisher.Mono.just(
                org.springframework.web.reactive.function.client.ClientResponse
                    .create(org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR).build()
            )
        }
        val webClientBuilder = WebClient.builder().exchangeFunction(exchangeFunction)
        val consumer = AmqpQueueConsumer(mock<ConnectionFactory>(), props, webClientBuilder)
        val containers = consumer.buildContainers()
        val listener = containers[0].messageListener as ChannelAwareMessageListener
        val channel = mock<Channel>()

        // First attempt (retryCount = 0), forward fails → should nack with requeue.
        val message = amqpMessage(body = "fail-payload".toByteArray())

        listener.onMessage(message, channel)

        verify(channel).basicNack(42L, false, true)
        verify(channel, never()).basicAck(any(), any())
        verify(channel, never()).basicReject(any(), any())
        // Verify retry count was incremented in message headers.
        assertEquals(1, message.messageProperties.headers[AmqpQueueConsumer.HEADER_RETRY_COUNT])
    }

    @Test
    fun `listener increments retry count header on each failed attempt`() {
        val receiverProps = receiver(maxRetries = 5, backoffPeriod = Duration.ofMillis(1))
        val props = RestqaProperties(receiver = mapOf("test" to receiverProps))
        val exchangeFunction = org.springframework.web.reactive.function.client.ExchangeFunction {
            reactor.core.publisher.Mono.just(
                org.springframework.web.reactive.function.client.ClientResponse
                    .create(org.springframework.http.HttpStatus.SERVICE_UNAVAILABLE).build()
            )
        }
        val webClientBuilder = WebClient.builder().exchangeFunction(exchangeFunction)
        val consumer = AmqpQueueConsumer(mock<ConnectionFactory>(), props, webClientBuilder)
        val containers = consumer.buildContainers()
        val listener = containers[0].messageListener as ChannelAwareMessageListener
        val channel = mock<Channel>()

        // Simulate message already at retryCount=2.
        val message = amqpMessage(
            body = "data".toByteArray(),
            headers = mapOf(AmqpQueueConsumer.HEADER_RETRY_COUNT to 2),
        )

        listener.onMessage(message, channel)

        // Should increment to 3.
        assertEquals(3, message.messageProperties.headers[AmqpQueueConsumer.HEADER_RETRY_COUNT])
        verify(channel).basicNack(42L, false, true)
    }

    // ─── Retry Exhausted → DLQ (basicReject) ─────────────────────────────────────

    @Test
    fun `listener rejects to DLQ when max retries exhausted`() {
        val receiverProps = receiver(maxRetries = 3, backoffPeriod = Duration.ofMillis(1))
        val props = RestqaProperties(receiver = mapOf("test" to receiverProps))
        val exchangeFunction = org.springframework.web.reactive.function.client.ExchangeFunction {
            reactor.core.publisher.Mono.just(
                org.springframework.web.reactive.function.client.ClientResponse
                    .create(org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR).build()
            )
        }
        val webClientBuilder = WebClient.builder().exchangeFunction(exchangeFunction)
        val consumer = AmqpQueueConsumer(mock<ConnectionFactory>(), props, webClientBuilder)
        val containers = consumer.buildContainers()
        val listener = containers[0].messageListener as ChannelAwareMessageListener
        val channel = mock<Channel>()

        // retryCount = maxRetries (3) → exhausted.
        val message = amqpMessage(
            body = "doomed".toByteArray(),
            headers = mapOf(AmqpQueueConsumer.HEADER_RETRY_COUNT to 3),
        )

        listener.onMessage(message, channel)

        // Should reject without requeue → routes to DLQ.
        verify(channel).basicReject(42L, false)
        verify(channel, never()).basicAck(any(), any())
        verify(channel, never()).basicNack(any(), any(), any())
    }

    @Test
    fun `listener rejects to DLQ when retry count exceeds max retries`() {
        val receiverProps = receiver(maxRetries = 2, backoffPeriod = Duration.ofMillis(1))
        val props = RestqaProperties(receiver = mapOf("test" to receiverProps))
        val exchangeFunction = org.springframework.web.reactive.function.client.ExchangeFunction {
            reactor.core.publisher.Mono.just(
                org.springframework.web.reactive.function.client.ClientResponse
                    .create(org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR).build()
            )
        }
        val webClientBuilder = WebClient.builder().exchangeFunction(exchangeFunction)
        val consumer = AmqpQueueConsumer(mock<ConnectionFactory>(), props, webClientBuilder)
        val containers = consumer.buildContainers()
        val listener = containers[0].messageListener as ChannelAwareMessageListener
        val channel = mock<Channel>()

        // retryCount (5) > maxRetries (2) → exhausted.
        val message = amqpMessage(
            body = "data".toByteArray(),
            headers = mapOf(AmqpQueueConsumer.HEADER_RETRY_COUNT to 5),
        )

        listener.onMessage(message, channel)

        verify(channel).basicReject(42L, false)
    }

    // ─── X-Retry-Count Header ─────────────────────────────────────────────────────

    @Test
    fun `forward receives correct retryCount from AMQP header`() {
        val receiverProps = receiver()
        val props = RestqaProperties(receiver = mapOf("test" to receiverProps))

        // Capture the X-Retry-Count sent to downstream.
        var capturedRetryCount: String? = null
        val exchangeFunction = org.springframework.web.reactive.function.client.ExchangeFunction { request ->
            capturedRetryCount = request.headers().getFirst("X-Retry-Count")
            reactor.core.publisher.Mono.just(
                org.springframework.web.reactive.function.client.ClientResponse
                    .create(org.springframework.http.HttpStatus.OK).build()
            )
        }
        val webClientBuilder = WebClient.builder().exchangeFunction(exchangeFunction)
        val consumer = AmqpQueueConsumer(mock<ConnectionFactory>(), props, webClientBuilder)
        val containers = consumer.buildContainers()
        val listener = containers[0].messageListener as ChannelAwareMessageListener
        val channel = mock<Channel>()

        val message = amqpMessage(
            body = "data".toByteArray(),
            headers = mapOf(AmqpQueueConsumer.HEADER_RETRY_COUNT to 4),
        )

        listener.onMessage(message, channel)

        assertEquals("4", capturedRetryCount)
    }

    @Test
    fun `forward receives retryCount 0 when no retry header present`() {
        val receiverProps = receiver()
        val props = RestqaProperties(receiver = mapOf("test" to receiverProps))

        var capturedRetryCount: String? = null
        val exchangeFunction = org.springframework.web.reactive.function.client.ExchangeFunction { request ->
            capturedRetryCount = request.headers().getFirst("X-Retry-Count")
            reactor.core.publisher.Mono.just(
                org.springframework.web.reactive.function.client.ClientResponse
                    .create(org.springframework.http.HttpStatus.OK).build()
            )
        }
        val webClientBuilder = WebClient.builder().exchangeFunction(exchangeFunction)
        val consumer = AmqpQueueConsumer(mock<ConnectionFactory>(), props, webClientBuilder)
        val containers = consumer.buildContainers()
        val listener = containers[0].messageListener as ChannelAwareMessageListener
        val channel = mock<Channel>()

        val message = amqpMessage(body = "data".toByteArray())

        listener.onMessage(message, channel)

        assertEquals("0", capturedRetryCount)
    }

    // ─── TTL / isExpired ──────────────────────────────────────────────────────────

    @Test
    fun `listener rejects expired message without forwarding`() {
        val receiverProps = receiver(ttl = Duration.ofSeconds(5))
        val props = RestqaProperties(receiver = mapOf("test" to receiverProps))

        var forwardCalled = false
        val exchangeFunction = org.springframework.web.reactive.function.client.ExchangeFunction {
            forwardCalled = true
            reactor.core.publisher.Mono.just(
                org.springframework.web.reactive.function.client.ClientResponse
                    .create(org.springframework.http.HttpStatus.OK).build()
            )
        }
        val webClientBuilder = WebClient.builder().exchangeFunction(exchangeFunction)
        val consumer = AmqpQueueConsumer(mock<ConnectionFactory>(), props, webClientBuilder)
        val containers = consumer.buildContainers()
        val listener = containers[0].messageListener as ChannelAwareMessageListener
        val channel = mock<Channel>()

        // Message timestamp 1 minute ago, TTL is 5 seconds → expired.
        val oldTimestamp = Date(System.currentTimeMillis() - 60_000)
        val message = amqpMessage(body = "expired".toByteArray(), timestamp = oldTimestamp)

        listener.onMessage(message, channel)

        // Should reject (DLQ) without forwarding.
        verify(channel).basicReject(42L, false)
        verify(channel, never()).basicAck(any(), any())
        assertFalse(forwardCalled, "Forward should not be called for expired messages")
    }

    @Test
    fun `listener processes non-expired message normally`() {
        val receiverProps = receiver(ttl = Duration.ofMinutes(5))
        val props = RestqaProperties(receiver = mapOf("test" to receiverProps))
        val exchangeFunction = org.springframework.web.reactive.function.client.ExchangeFunction {
            reactor.core.publisher.Mono.just(
                org.springframework.web.reactive.function.client.ClientResponse
                    .create(org.springframework.http.HttpStatus.OK).build()
            )
        }
        val webClientBuilder = WebClient.builder().exchangeFunction(exchangeFunction)
        val consumer = AmqpQueueConsumer(mock<ConnectionFactory>(), props, webClientBuilder)
        val containers = consumer.buildContainers()
        val listener = containers[0].messageListener as ChannelAwareMessageListener
        val channel = mock<Channel>()

        // Message timestamp 1 second ago, TTL is 5 minutes → not expired.
        val recentTimestamp = Date(System.currentTimeMillis() - 1_000)
        val message = amqpMessage(body = "fresh".toByteArray(), timestamp = recentTimestamp)

        listener.onMessage(message, channel)

        verify(channel).basicAck(42L, false)
    }

    @Test
    fun `listener processes message without timestamp when TTL is configured`() {
        val receiverProps = receiver(ttl = Duration.ofSeconds(5))
        val props = RestqaProperties(receiver = mapOf("test" to receiverProps))
        val exchangeFunction = org.springframework.web.reactive.function.client.ExchangeFunction {
            reactor.core.publisher.Mono.just(
                org.springframework.web.reactive.function.client.ClientResponse
                    .create(org.springframework.http.HttpStatus.OK).build()
            )
        }
        val webClientBuilder = WebClient.builder().exchangeFunction(exchangeFunction)
        val consumer = AmqpQueueConsumer(mock<ConnectionFactory>(), props, webClientBuilder)
        val containers = consumer.buildContainers()
        val listener = containers[0].messageListener as ChannelAwareMessageListener
        val channel = mock<Channel>()

        // No timestamp → cannot determine if expired → process normally.
        val message = amqpMessage(body = "no-ts".toByteArray())

        listener.onMessage(message, channel)

        verify(channel).basicAck(42L, false)
    }

    // ─── headersOf ────────────────────────────────────────────────────────────────

    @Test
    fun `headersOf extracts message headers and content type`() {
        val receiverProps = receiver()
        val props = RestqaProperties(receiver = mapOf("test" to receiverProps))
        val consumer = AmqpQueueConsumer(mock<ConnectionFactory>(), props, WebClient.builder())

        val message = amqpMessage(
            headers = mapOf("X-Custom" to "value", AmqpQueueConsumer.HEADER_RETRY_COUNT to 2),
            contentType = "application/json",
        )

        val headers = consumer.headersOf(message)

        assertEquals("value", headers["X-Custom"])
        assertEquals("application/json", headers["Content-Type"])
        // Internal retry header should NOT be propagated.
        assertFalse(headers.containsKey(AmqpQueueConsumer.HEADER_RETRY_COUNT))
    }

    @Test
    fun `headersOf excludes null values`() {
        val receiverProps = receiver()
        val props = RestqaProperties(receiver = mapOf("test" to receiverProps))
        val consumer = AmqpQueueConsumer(mock<ConnectionFactory>(), props, WebClient.builder())

        val msgProps = MessageProperties().apply {
            setHeader("present", "yes")
            // Simulate a null-valued header (can happen when broker injects headers).
            headers["absent"] = null
        }
        val message = Message("data".toByteArray(), msgProps)

        val headers = consumer.headersOf(message)

        assertTrue(headers.containsKey("present"))
        assertFalse(headers.containsKey("absent"))
    }

    // ─── Connection failure ───────────────────────────────────────────────────────

    @Test
    fun `listener nacks with requeue on connection error to downstream`() {
        val receiverProps = receiver(maxRetries = 3, backoffPeriod = Duration.ofMillis(1))
        val props = RestqaProperties(receiver = mapOf("test" to receiverProps))
        val exchangeFunction = org.springframework.web.reactive.function.client.ExchangeFunction {
            reactor.core.publisher.Mono.error(RuntimeException("Connection refused"))
        }
        val webClientBuilder = WebClient.builder().exchangeFunction(exchangeFunction)
        val consumer = AmqpQueueConsumer(mock<ConnectionFactory>(), props, webClientBuilder)
        val containers = consumer.buildContainers()
        val listener = containers[0].messageListener as ChannelAwareMessageListener
        val channel = mock<Channel>()

        val message = amqpMessage(body = "data".toByteArray())

        listener.onMessage(message, channel)

        verify(channel).basicNack(42L, false, true)
    }
}

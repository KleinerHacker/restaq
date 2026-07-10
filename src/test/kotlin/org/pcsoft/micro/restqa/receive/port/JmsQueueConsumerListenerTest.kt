package org.pcsoft.micro.restqa.receive.port

import jakarta.jms.BytesMessage
import jakarta.jms.ConnectionFactory
import jakarta.jms.MessageListener
import jakarta.jms.TextMessage
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import org.pcsoft.micro.restqa.configuration.QueueEndpointProperties
import org.pcsoft.micro.restqa.configuration.ReceiverProperties
import org.pcsoft.micro.restqa.configuration.ReceiverRestProperties
import org.pcsoft.micro.restqa.configuration.RestqaProperties
import org.pcsoft.micro.restqa.configuration.RetryProperties
import org.springframework.web.reactive.function.client.ClientResponse
import org.springframework.web.reactive.function.client.ExchangeFunction
import org.springframework.web.reactive.function.client.WebClient
import reactor.core.publisher.Mono
import java.time.Duration
import java.util.Collections
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Unit tests for the internal listener logic of [JmsQueueConsumer]:
 * - Retry success scenario (forward succeeds → commit)
 * - Retry failure with backoff → exception → rollback → redelivery
 * - Retry exhausted → exception → broker routes to DLQ
 * - X-Retry-Count header propagation via JMSXDeliveryCount
 * - TTL-based message expiry
 * - headersOf extraction
 * - payloadOf extraction (BytesMessage, TextMessage, other)
 * - getRetryCount parsing
 */
class JmsQueueConsumerListenerTest {

    private fun receiver(
        maxRetries: Int = 3,
        backoffPeriod: Duration = Duration.ofMillis(1),
        ttl: Duration? = null,
    ) = ReceiverProperties(
        rest = ReceiverRestProperties(url = "http://localhost:8080/callback"),
        queue = QueueEndpointProperties(name = "test.queue"),
        retry = RetryProperties(maxRetries = maxRetries, backoffPeriod = backoffPeriod),
        timeToLive = ttl,
    )

    private fun buildListener(
        receiverProperties: ReceiverProperties,
        httpStatus: org.springframework.http.HttpStatus = org.springframework.http.HttpStatus.OK,
        exchangeFn: ExchangeFunction? = null,
    ): Pair<JmsQueueConsumer, MessageListener> {
        val props = RestqaProperties(receiver = mapOf("test" to receiverProperties))
        val exchange = exchangeFn ?: ExchangeFunction {
            Mono.just(ClientResponse.create(httpStatus).build())
        }
        val webClientBuilder = WebClient.builder().exchangeFunction(exchange)
        val consumer = JmsQueueConsumer(mock<ConnectionFactory>(), props, webClientBuilder)
        val containers = consumer.buildContainers()
        assertEquals(1, containers.size)
        assertTrue(containers[0].isSessionTransacted)
        val listener = containers[0].messageListener as MessageListener
        return consumer to listener
    }

    private fun mockBytesMessage(
        body: ByteArray = "hello".toByteArray(),
        deliveryCount: Int = 1,
        properties: Map<String, String> = emptyMap(),
        timestamp: Long = System.currentTimeMillis(),
    ): BytesMessage {
        val message = mock<BytesMessage>()
        whenever(message.bodyLength).thenReturn(body.size.toLong())
        whenever(message.readBytes(org.mockito.kotlin.any<ByteArray>())).thenAnswer { invocation ->
            val buf = invocation.getArgument<ByteArray>(0)
            body.copyInto(buf)
            body.size
        }
        whenever(message.getIntProperty("JMSXDeliveryCount")).thenReturn(deliveryCount)
        whenever(message.jmsTimestamp).thenReturn(timestamp)

        val propertyNames = Collections.enumeration(properties.keys.toList())
        whenever(message.propertyNames).thenReturn(propertyNames)
        properties.forEach { (k, v) ->
            whenever(message.getObjectProperty(k)).thenReturn(v)
        }
        return message
    }

    private fun mockTextMessage(
        text: String = "hello",
        deliveryCount: Int = 1,
        properties: Map<String, String> = emptyMap(),
        timestamp: Long = System.currentTimeMillis(),
    ): TextMessage {
        val message = mock<TextMessage>()
        whenever(message.text).thenReturn(text)
        whenever(message.getIntProperty("JMSXDeliveryCount")).thenReturn(deliveryCount)
        whenever(message.jmsTimestamp).thenReturn(timestamp)

        val propertyNames = Collections.enumeration(properties.keys.toList())
        whenever(message.propertyNames).thenReturn(propertyNames)
        properties.forEach { (k, v) ->
            whenever(message.getObjectProperty(k)).thenReturn(v)
        }
        return message
    }

    // ─── Retry Success ────────────────────────────────────────────────────────────

    @Test
    fun `listener commits on successful forward (first delivery)`() {
        val (_, listener) = buildListener(receiver())
        val message = mockBytesMessage(body = "payload".toByteArray(), deliveryCount = 1)

        // Should not throw → transacted session commits.
        listener.onMessage(message)
    }

    @Test
    fun `listener commits on successful forward after retries (deliveryCount=3)`() {
        val (_, listener) = buildListener(receiver())
        val message = mockBytesMessage(
            body = "retry-payload".toByteArray(),
            deliveryCount = 3, // 0-based retryCount = 2
        )

        // Should not throw → success.
        listener.onMessage(message)
    }

    // ─── Retry Failure → Exception → Rollback ────────────────────────────────────

    @Test
    fun `listener throws on forward failure when retries remain causing rollback`() {
        val (_, listener) = buildListener(
            receiver(maxRetries = 3, backoffPeriod = Duration.ofMillis(1)),
            httpStatus = org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR,
        )
        // First delivery (deliveryCount=1, retryCount=0) → retries remain.
        val message = mockBytesMessage(body = "fail".toByteArray(), deliveryCount = 1)

        // Should throw to trigger session rollback → broker redelivers.
        val ex = assertThrows<RuntimeException> { listener.onMessage(message) }
        assertTrue(ex.message!!.contains("Delivery failed after attempt 0"))
    }

    @Test
    fun `listener throws on forward failure at intermediate retry`() {
        val (_, listener) = buildListener(
            receiver(maxRetries = 5, backoffPeriod = Duration.ofMillis(1)),
            httpStatus = org.springframework.http.HttpStatus.SERVICE_UNAVAILABLE,
        )
        // deliveryCount=3 → retryCount=2, maxRetries=5 → retries remain.
        val message = mockBytesMessage(body = "data".toByteArray(), deliveryCount = 3)

        val ex = assertThrows<RuntimeException> { listener.onMessage(message) }
        assertTrue(ex.message!!.contains("Delivery failed after attempt 2"))
    }

    // ─── Retry Exhausted → DLQ ───────────────────────────────────────────────────

    @Test
    fun `listener throws on exhausted retries to let broker route to DLQ`() {
        val (_, listener) = buildListener(
            receiver(maxRetries = 3, backoffPeriod = Duration.ofMillis(1)),
            httpStatus = org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR,
        )
        // deliveryCount=4 → retryCount=3 == maxRetries → exhausted.
        val message = mockBytesMessage(body = "doomed".toByteArray(), deliveryCount = 4)

        val ex = assertThrows<RuntimeException> { listener.onMessage(message) }
        assertTrue(ex.message!!.contains("Delivery failed after attempt 3"))
    }

    @Test
    fun `listener throws when retry count exceeds max retries`() {
        val (_, listener) = buildListener(
            receiver(maxRetries = 2, backoffPeriod = Duration.ofMillis(1)),
            httpStatus = org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR,
        )
        // deliveryCount=10 → retryCount=9 >> maxRetries=2 → exhausted.
        val message = mockBytesMessage(body = "data".toByteArray(), deliveryCount = 10)

        assertThrows<RuntimeException> { listener.onMessage(message) }
    }

    // ─── X-Retry-Count Header ─────────────────────────────────────────────────────

    @Test
    fun `forward receives correct retryCount from JMSXDeliveryCount`() {
        var capturedRetryCount: String? = null
        val exchangeFn = ExchangeFunction { request ->
            capturedRetryCount = request.headers().getFirst("X-Retry-Count")
            Mono.just(ClientResponse.create(org.springframework.http.HttpStatus.OK).build())
        }
        val (_, listener) = buildListener(receiver(), exchangeFn = exchangeFn)

        // deliveryCount=4 → retryCount=3.
        val message = mockBytesMessage(body = "data".toByteArray(), deliveryCount = 4)
        listener.onMessage(message)

        assertEquals("3", capturedRetryCount)
    }

    @Test
    fun `forward receives retryCount 0 on first delivery`() {
        var capturedRetryCount: String? = null
        val exchangeFn = ExchangeFunction { request ->
            capturedRetryCount = request.headers().getFirst("X-Retry-Count")
            Mono.just(ClientResponse.create(org.springframework.http.HttpStatus.OK).build())
        }
        val (_, listener) = buildListener(receiver(), exchangeFn = exchangeFn)

        val message = mockBytesMessage(body = "data".toByteArray(), deliveryCount = 1)
        listener.onMessage(message)

        assertEquals("0", capturedRetryCount)
    }

    @Test
    fun `getRetryCount handles missing JMSXDeliveryCount gracefully`() {
        var capturedRetryCount: String? = null
        val exchangeFn = ExchangeFunction { request ->
            capturedRetryCount = request.headers().getFirst("X-Retry-Count")
            Mono.just(ClientResponse.create(org.springframework.http.HttpStatus.OK).build())
        }
        val (_, listener) = buildListener(receiver(), exchangeFn = exchangeFn)

        // Mock a message where getIntProperty throws.
        val message = mock<BytesMessage>()
        whenever(message.getIntProperty("JMSXDeliveryCount")).thenThrow(NumberFormatException("not set"))
        whenever(message.bodyLength).thenReturn(4)
        whenever(message.readBytes(org.mockito.kotlin.any<ByteArray>())).thenAnswer { invocation ->
            "data".toByteArray().copyInto(invocation.getArgument<ByteArray>(0))
            4
        }
        whenever(message.jmsTimestamp).thenReturn(System.currentTimeMillis())
        whenever(message.propertyNames).thenReturn(Collections.enumeration(emptyList<String>()))

        listener.onMessage(message)

        // Fallback: deliveryCount defaults to 1 → retryCount = 0.
        assertEquals("0", capturedRetryCount)
    }

    // ─── TTL / isExpired ──────────────────────────────────────────────────────────

    @Test
    fun `listener acknowledges expired message without forwarding`() {
        val receiverProps = receiver(ttl = Duration.ofSeconds(5))
        var forwardCalled = false
        val exchangeFn = ExchangeFunction {
            forwardCalled = true
            Mono.just(ClientResponse.create(org.springframework.http.HttpStatus.OK).build())
        }
        val (_, listener) = buildListener(receiverProps, exchangeFn = exchangeFn)

        // Timestamp 1 minute ago, TTL 5 seconds → expired.
        val message = mockBytesMessage(
            body = "expired".toByteArray(),
            deliveryCount = 1,
            timestamp = System.currentTimeMillis() - 60_000,
        )
        whenever(message.acknowledge()).then { /* no-op */ }

        listener.onMessage(message)

        assertFalse(forwardCalled, "Forward should not be called for expired messages")
    }

    @Test
    fun `listener processes non-expired message normally`() {
        val receiverProps = receiver(ttl = Duration.ofMinutes(5))
        val (_, listener) = buildListener(receiverProps)

        // Timestamp 1 second ago, TTL 5 minutes → not expired.
        val message = mockBytesMessage(
            body = "fresh".toByteArray(),
            deliveryCount = 1,
            timestamp = System.currentTimeMillis() - 1_000,
        )

        // Should not throw.
        listener.onMessage(message)
    }

    @Test
    fun `listener processes message with timestamp 0 when TTL is configured`() {
        val receiverProps = receiver(ttl = Duration.ofSeconds(5))
        val (_, listener) = buildListener(receiverProps)

        // timestamp=0 → cannot determine if expired → process normally.
        val message = mockBytesMessage(body = "no-ts".toByteArray(), deliveryCount = 1, timestamp = 0)

        listener.onMessage(message)
    }

    // ─── payloadOf ────────────────────────────────────────────────────────────────

    @Test
    fun `payloadOf extracts bytes from BytesMessage`() {
        val props = RestqaProperties(receiver = mapOf("test" to receiver()))
        val consumer = JmsQueueConsumer(mock<ConnectionFactory>(), props, WebClient.builder())

        val message = mockBytesMessage(body = "binary-data".toByteArray())
        val payload = consumer.payloadOf(message)

        assertEquals("binary-data", String(payload))
    }

    @Test
    fun `payloadOf extracts text from TextMessage`() {
        val props = RestqaProperties(receiver = mapOf("test" to receiver()))
        val consumer = JmsQueueConsumer(mock<ConnectionFactory>(), props, WebClient.builder())

        val message = mockTextMessage(text = "text-data")
        val payload = consumer.payloadOf(message)

        assertEquals("text-data", String(payload))
    }

    @Test
    fun `payloadOf returns empty for null text in TextMessage`() {
        val props = RestqaProperties(receiver = mapOf("test" to receiver()))
        val consumer = JmsQueueConsumer(mock<ConnectionFactory>(), props, WebClient.builder())

        val message = mock<TextMessage>()
        whenever(message.text).thenReturn(null)

        val payload = consumer.payloadOf(message)

        assertEquals(0, payload.size)
    }

    @Test
    fun `payloadOf returns empty for unknown message type`() {
        val props = RestqaProperties(receiver = mapOf("test" to receiver()))
        val consumer = JmsQueueConsumer(mock<ConnectionFactory>(), props, WebClient.builder())

        val message = mock<jakarta.jms.Message>()
        val payload = consumer.payloadOf(message)

        assertEquals(0, payload.size)
    }

    // ─── headersOf ────────────────────────────────────────────────────────────────

    @Test
    fun `headersOf extracts JMS properties as headers`() {
        val props = RestqaProperties(receiver = mapOf("test" to receiver()))
        val consumer = JmsQueueConsumer(mock<ConnectionFactory>(), props, WebClient.builder())

        val message = mock<jakarta.jms.Message>()
        val propertyNames = Collections.enumeration(listOf("X-Custom", "Content_Type"))
        whenever(message.propertyNames).thenReturn(propertyNames)
        whenever(message.getObjectProperty("X-Custom")).thenReturn("value")
        whenever(message.getObjectProperty("Content_Type")).thenReturn("application/json")

        val headers = consumer.headersOf(message)

        assertEquals("value", headers["X-Custom"])
        assertEquals("application/json", headers["Content_Type"])
    }

    @Test
    fun `headersOf skips null property values`() {
        val props = RestqaProperties(receiver = mapOf("test" to receiver()))
        val consumer = JmsQueueConsumer(mock<ConnectionFactory>(), props, WebClient.builder())

        val message = mock<jakarta.jms.Message>()
        val propertyNames = Collections.enumeration(listOf("present", "absent"))
        whenever(message.propertyNames).thenReturn(propertyNames)
        whenever(message.getObjectProperty("present")).thenReturn("yes")
        whenever(message.getObjectProperty("absent")).thenReturn(null)

        val headers = consumer.headersOf(message)

        assertTrue(headers.containsKey("present"))
        assertFalse(headers.containsKey("absent"))
    }

    // ─── Connection error ─────────────────────────────────────────────────────────

    @Test
    fun `listener throws on connection error to trigger rollback`() {
        val exchangeFn = ExchangeFunction {
            Mono.error(RuntimeException("Connection refused"))
        }
        val (_, listener) = buildListener(
            receiver(maxRetries = 3, backoffPeriod = Duration.ofMillis(1)),
            exchangeFn = exchangeFn,
        )
        val message = mockBytesMessage(body = "data".toByteArray(), deliveryCount = 1)

        assertThrows<RuntimeException> { listener.onMessage(message) }
    }
}

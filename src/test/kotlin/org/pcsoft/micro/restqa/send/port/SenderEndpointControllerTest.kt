package org.pcsoft.micro.restqa.send.port

import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.pcsoft.micro.restqa.configuration.QueueEndpointProperties
import org.pcsoft.micro.restqa.configuration.SenderProperties
import org.pcsoft.micro.restqa.configuration.SenderRestProperties
import org.springframework.http.HttpStatus
import org.springframework.mock.http.server.reactive.MockServerHttpRequest
import org.springframework.mock.web.server.MockServerWebExchange
import org.springframework.util.unit.DataSize
import org.springframework.web.reactive.function.server.HandlerStrategies
import org.springframework.web.reactive.function.server.ServerRequest
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

/**
 * Unit tests for [SenderEndpointController] covering the core sender behavior:
 * successful message forwarding, empty payloads, header propagation, payload size
 * enforcement, error handling when the queue client fails, and transport header filtering.
 */
class SenderEndpointControllerTest {

    private val properties = SenderProperties(
        rest = SenderRestProperties(path = "/api/orders"),
        queue = QueueEndpointProperties(name = "orders.queue"),
    )

    private fun requestWithBody(body: String?, headers: Map<String, String> = emptyMap()): ServerRequest {
        var builder = MockServerHttpRequest.post("/api/orders")
        headers.forEach { (name, value) -> builder = builder.header(name, value) }
        val request = if (body == null) builder.build() else builder.body(body)
        val exchange = MockServerWebExchange.from(request)
        return ServerRequest.create(exchange, HandlerStrategies.withDefaults().messageReaders())
    }

    /**
     * Verifies the happy-path: the handler extracts the request body, forwards it to
     * the configured queue via the queue client, and returns HTTP 202 Accepted to the caller.
     */
    @Test
    fun `handle forwards the request body to the queue and replies 202 Accepted`() {
        val queueClient = mock<MessageQueueClient>()
        val handler = SenderEndpointController(properties, queueClient)

        val response = handler.handle(requestWithBody("hello")).block()

        assertEquals(HttpStatus.ACCEPTED, response?.statusCode())
        verify(queueClient).send(eq(properties.queue), eq("hello".toByteArray()), any())
    }

    /**
     * Verifies that when the HTTP request has no body, the handler forwards an empty
     * byte array to the queue and still returns HTTP 202 Accepted, ensuring bodyless
     * requests (e.g., trigger-style calls) are handled gracefully.
     */
    @Test
    fun `handle forwards an empty payload when there is no body`() {
        val queueClient = mock<MessageQueueClient>()
        val handler = SenderEndpointController(properties, queueClient)

        val response = handler.handle(requestWithBody(null)).block()

        assertEquals(HttpStatus.ACCEPTED, response?.statusCode())
        verify(queueClient).send(eq(properties.queue), eq(ByteArray(0)), any())
    }

    /**
     * Verifies that HTTP headers from the incoming request are propagated to the queue
     * client as a header map, enabling downstream consumers to access metadata like
     * trace IDs that were sent by the original caller.
     */
    @Test
    fun `handle forwards the HTTP headers to the queue`() {
        val queueClient = mock<MessageQueueClient>()
        val handler = SenderEndpointController(properties, queueClient)

        handler.handle(requestWithBody("hello", mapOf("X-Trace" to "abc"))).block()

        val captor = argumentCaptor<Map<String, String>>()
        verify(queueClient).send(eq(properties.queue), eq("hello".toByteArray()), captor.capture())
        assertEquals("abc", captor.firstValue["X-Trace"])
    }

    /**
     * Verifies that the handler returns HTTP 413 Content Too Large when the request body
     * exceeds the configured `maxPayloadSize` limit, protecting the messaging system
     * from oversized messages.
     */
    @Test
    fun `handle returns 413 when payload exceeds max size`() {
        val queueClient = mock<MessageQueueClient>()
        val handler = SenderEndpointController(properties, queueClient, maxPayloadSize = DataSize.ofBytes(5))

        val response = handler.handle(requestWithBody("this is way too long")).block()

        assertNotNull(response)
        assertEquals(HttpStatus.CONTENT_TOO_LARGE, response.statusCode())
    }

    /**
     * Verifies that a payload within the configured size limit is accepted normally,
     * resulting in a successful 202 Accepted response — the size check only rejects
     * payloads that exceed the threshold.
     */
    @Test
    fun `handle accepts payload within size limit`() {
        val queueClient = mock<MessageQueueClient>()
        val handler = SenderEndpointController(properties, queueClient, maxPayloadSize = DataSize.ofKilobytes(1))

        val response = handler.handle(requestWithBody("small")).block()

        assertEquals(HttpStatus.ACCEPTED, response?.statusCode())
    }

    /**
     * Verifies that when the queue client throws an exception during message send,
     * the handler returns HTTP 502 Bad Gateway, signaling to the caller that the
     * downstream messaging infrastructure is unavailable.
     */
    @Test
    fun `handle returns 502 when queue client throws exception`() {
        val queueClient = mock<MessageQueueClient>()
        whenever(queueClient.send(any(), any(), any())).thenThrow(RuntimeException("Broker unreachable"))
        val handler = SenderEndpointController(properties, queueClient)

        val response = handler.handle(requestWithBody("hello")).block()

        assertNotNull(response)
        assertEquals(HttpStatus.BAD_GATEWAY, response.statusCode())
    }

    /**
     * Verifies that transport-level HTTP headers (e.g., `Host`) are filtered out before
     * forwarding to the queue, while application-level headers (e.g., `X-Trace`) are
     * preserved. This prevents leaking network topology details into the messaging layer.
     */
    @Test
    fun `handle excludes transport headers before forwarding`() {
        val queueClient = mock<MessageQueueClient>()
        val handler = SenderEndpointController(properties, queueClient)

        handler.handle(requestWithBody("hello", mapOf("Host" to "example.com", "X-Trace" to "abc"))).block()

        val captor = argumentCaptor<Map<String, String>>()
        verify(queueClient).send(eq(properties.queue), eq("hello".toByteArray()), captor.capture())
        assertEquals("abc", captor.firstValue["X-Trace"])
        assertEquals(null, captor.firstValue["Host"])
    }
}

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

    @Test
    fun `handle forwards the request body to the queue and replies 202 Accepted`() {
        val queueClient = mock<MessageQueueClient>()
        val handler = SenderEndpointController(properties, queueClient)

        val response = handler.handle(requestWithBody("hello")).block()

        assertEquals(HttpStatus.ACCEPTED, response?.statusCode())
        verify(queueClient).send(eq(properties.queue), eq("hello".toByteArray()), any())
    }

    @Test
    fun `handle forwards an empty payload when there is no body`() {
        val queueClient = mock<MessageQueueClient>()
        val handler = SenderEndpointController(properties, queueClient)

        val response = handler.handle(requestWithBody(null)).block()

        assertEquals(HttpStatus.ACCEPTED, response?.statusCode())
        verify(queueClient).send(eq(properties.queue), eq(ByteArray(0)), any())
    }

    @Test
    fun `handle forwards the HTTP headers to the queue`() {
        val queueClient = mock<MessageQueueClient>()
        val handler = SenderEndpointController(properties, queueClient)

        handler.handle(requestWithBody("hello", mapOf("X-Trace" to "abc"))).block()

        val captor = argumentCaptor<Map<String, String>>()
        verify(queueClient).send(eq(properties.queue), eq("hello".toByteArray()), captor.capture())
        assertEquals("abc", captor.firstValue["X-Trace"])
    }

    @Test
    fun `handle returns 413 when payload exceeds max size`() {
        val queueClient = mock<MessageQueueClient>()
        val handler = SenderEndpointController(properties, queueClient, maxPayloadSize = DataSize.ofBytes(5))

        val response = handler.handle(requestWithBody("this is way too long")).block()

        assertNotNull(response)
        assertEquals(HttpStatus.CONTENT_TOO_LARGE, response.statusCode())
    }

    @Test
    fun `handle accepts payload within size limit`() {
        val queueClient = mock<MessageQueueClient>()
        val handler = SenderEndpointController(properties, queueClient, maxPayloadSize = DataSize.ofKilobytes(1))

        val response = handler.handle(requestWithBody("small")).block()

        assertEquals(HttpStatus.ACCEPTED, response?.statusCode())
    }

    @Test
    fun `handle returns 502 when queue client throws exception`() {
        val queueClient = mock<MessageQueueClient>()
        whenever(queueClient.send(any(), any(), any())).thenThrow(RuntimeException("Broker unreachable"))
        val handler = SenderEndpointController(properties, queueClient)

        val response = handler.handle(requestWithBody("hello")).block()

        assertNotNull(response)
        assertEquals(HttpStatus.BAD_GATEWAY, response.statusCode())
    }

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

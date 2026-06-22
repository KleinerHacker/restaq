package org.pcsoft.micro.restqa.send.port

import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.pcsoft.micro.restqa.configuration.QueueEndpointProperties
import org.pcsoft.micro.restqa.configuration.SenderProperties
import org.pcsoft.micro.restqa.send.controller.MessageQueueClient
import org.springframework.http.HttpStatus
import org.springframework.mock.http.server.reactive.MockServerHttpRequest
import org.springframework.mock.web.server.MockServerWebExchange
import org.springframework.web.reactive.function.server.HandlerStrategies
import org.springframework.web.reactive.function.server.ServerRequest
import kotlin.test.assertEquals

class SenderEndpointControllerTest {

    private val properties = SenderProperties(
        endpoint = "/api/orders",
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
    fun `handle forwards the request body to the queue and replies 200 OK`() {
        val queueClient = mock<MessageQueueClient>()
        val handler = SenderEndpointController(properties, queueClient)

        val response = handler.handle(requestWithBody("hello")).block()

        assertEquals(HttpStatus.OK, response?.statusCode())
        verify(queueClient).send(eq(properties.queue), eq("hello".toByteArray()), any())
    }

    @Test
    fun `handle forwards an empty payload when there is no body`() {
        val queueClient = mock<MessageQueueClient>()
        val handler = SenderEndpointController(properties, queueClient)

        val response = handler.handle(requestWithBody(null)).block()

        assertEquals(HttpStatus.OK, response?.statusCode())
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
}

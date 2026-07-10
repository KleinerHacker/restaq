package org.pcsoft.micro.restqa.send.port

import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.pcsoft.micro.restqa.configuration.QueueEndpointProperties
import org.pcsoft.micro.restqa.configuration.SenderProperties
import org.pcsoft.micro.restqa.configuration.SenderRestProperties
import org.pcsoft.micro.restqa.configuration.SenderSynchronousProperties
import org.pcsoft.micro.restqa.internal.SynchronousResponse
import org.pcsoft.micro.restqa.internal.SynchronousResponseRegistry
import org.springframework.http.HttpStatus
import org.springframework.mock.http.server.reactive.MockServerHttpRequest
import org.springframework.mock.web.server.MockServerWebExchange
import org.springframework.web.reactive.function.server.HandlerStrategies
import org.springframework.web.reactive.function.server.ServerRequest
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * Tests for the synchronous sender mode in [SenderEndpointController].
 */
class SenderSynchronousTest {

    private val syncProperties = SenderProperties(
        rest = SenderRestProperties(path = "/api/orders"),
        queue = QueueEndpointProperties(name = "orders.queue"),
        synchronous = SenderSynchronousProperties(receiverRef = "order-processor"),
        timeout = Duration.ofSeconds(5),
    )

    private fun requestWithBody(body: String): ServerRequest {
        val request = MockServerHttpRequest.post("/api/orders").body(body)
        val exchange = MockServerWebExchange.from(request)
        return ServerRequest.create(exchange, HandlerStrategies.withDefaults().messageReaders())
    }

    @Test
    fun `synchronous mode injects correlation ID header and returns response`() {
        val queueClient = mock<MessageQueueClient>()
        val registry = SynchronousResponseRegistry()
        val handler = SenderEndpointController(syncProperties, queueClient, null, registry)

        whenever(queueClient.send(any(), any(), any())).thenAnswer { invocation ->
            val headers = invocation.getArgument<Map<String, String>>(2)
            val correlationId = headers[SynchronousResponseRegistry.HEADER_CORRELATION_ID]
            assertNotNull(correlationId, "Correlation ID header must be present")
            registry.complete(correlationId, SynchronousResponse(200, "response".toByteArray()))
        }

        val response = handler.handle(requestWithBody("hello")).block()

        assertNotNull(response)
        assertEquals(HttpStatus.OK, response.statusCode())
        verify(queueClient).send(any(), any(), any())
    }

    @Test
    fun `synchronous mode returns downstream response status and body`() {
        val queueClient = mock<MessageQueueClient>()
        val registry = SynchronousResponseRegistry()
        val handler = SenderEndpointController(syncProperties, queueClient, null, registry)

        whenever(queueClient.send(any(), any(), any())).thenAnswer { invocation ->
            val headers = invocation.getArgument<Map<String, String>>(2)
            val correlationId = headers[SynchronousResponseRegistry.HEADER_CORRELATION_ID]!!
            registry.complete(
                correlationId,
                SynchronousResponse(
                    statusCode = 201,
                    body = """{"id": "123"}""".toByteArray(),
                    headers = mapOf("Content-Type" to "application/json"),
                ),
            )
        }

        val response = handler.handle(requestWithBody("create")).block()

        assertNotNull(response)
        assertEquals(HttpStatus.CREATED, response.statusCode())
    }

    @Test
    fun `synchronous mode returns 504 on timeout`() {
        val queueClient = mock<MessageQueueClient>()
        val registry = SynchronousResponseRegistry()
        val shortTimeoutProperties = syncProperties.copy(timeout = Duration.ofMillis(50))
        val handler = SenderEndpointController(shortTimeoutProperties, queueClient, null, registry)

        // Don't complete the response → timeout.
        val response = handler.handle(requestWithBody("hello")).block()

        assertNotNull(response)
        assertEquals(HttpStatus.GATEWAY_TIMEOUT, response.statusCode())
    }

    @Test
    fun `synchronous mode returns 502 when queue send fails`() {
        val queueClient = mock<MessageQueueClient>()
        val registry = SynchronousResponseRegistry()
        val handler = SenderEndpointController(syncProperties, queueClient, null, registry)

        whenever(queueClient.send(any(), any(), any())).thenThrow(RuntimeException("Broker down"))

        val response = handler.handle(requestWithBody("hello")).block()

        assertNotNull(response)
        assertEquals(HttpStatus.BAD_GATEWAY, response.statusCode())
        assertEquals(0, registry.pendingCount())
    }

    @Test
    fun `asynchronous mode returns 202 regardless of timeout`() {
        val asyncProperties = SenderProperties(
            rest = SenderRestProperties(path = "/api/orders"),
            queue = QueueEndpointProperties(name = "orders.queue"),
            timeout = Duration.ofSeconds(30),
            // No synchronous config.
        )
        val queueClient = mock<MessageQueueClient>()
        val registry = SynchronousResponseRegistry()
        val handler = SenderEndpointController(asyncProperties, queueClient, null, registry)

        val response = handler.handle(requestWithBody("hello")).block()

        assertNotNull(response)
        assertEquals(HttpStatus.ACCEPTED, response.statusCode())
    }

    @Test
    fun `synchronous mode propagates non-2xx downstream response`() {
        val queueClient = mock<MessageQueueClient>()
        val registry = SynchronousResponseRegistry()
        val handler = SenderEndpointController(syncProperties, queueClient, null, registry)

        whenever(queueClient.send(any(), any(), any())).thenAnswer { invocation ->
            val headers = invocation.getArgument<Map<String, String>>(2)
            val correlationId = headers[SynchronousResponseRegistry.HEADER_CORRELATION_ID]!!
            registry.complete(
                correlationId,
                SynchronousResponse(
                    statusCode = 422,
                    body = """{"error": "validation failed"}""".toByteArray(),
                    headers = mapOf("Content-Type" to "application/json"),
                ),
            )
        }

        val response = handler.handle(requestWithBody("invalid")).block()

        assertNotNull(response)
        assertEquals(422, response.statusCode().value())
    }

    @Test
    fun `correlation ID header is included in forwarded headers`() {
        val queueClient = mock<MessageQueueClient>()
        val registry = SynchronousResponseRegistry()
        val handler = SenderEndpointController(syncProperties, queueClient, null, registry)

        whenever(queueClient.send(any(), any(), any())).thenAnswer { invocation ->
            val headers = invocation.getArgument<Map<String, String>>(2)
            assertTrue(headers.containsKey(SynchronousResponseRegistry.HEADER_CORRELATION_ID))
            val correlationId = headers[SynchronousResponseRegistry.HEADER_CORRELATION_ID]!!
            registry.complete(correlationId, SynchronousResponse(200, ByteArray(0)))
        }

        handler.handle(requestWithBody("data")).block()

        val captor = argumentCaptor<Map<String, String>>()
        verify(queueClient).send(any(), any(), captor.capture())
        assertTrue(captor.firstValue.containsKey(SynchronousResponseRegistry.HEADER_CORRELATION_ID))
    }
}

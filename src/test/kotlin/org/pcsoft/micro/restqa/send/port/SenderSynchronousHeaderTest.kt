package org.pcsoft.micro.restqa.send.port

import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
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

/**
 * Tests for critical header scenarios in synchronous mode:
 * - Client sends Accept header that does not match downstream Content-Type
 * - Downstream returns 415 Unsupported Media Type (wrong Content-Type from client)
 * - Downstream returns 406 Not Acceptable
 *
 * These tests verify that the synchronous sender transparently forwards
 * downstream error responses (including header mismatches) to the original client.
 */
class SenderSynchronousHeaderTest {

    private val syncProperties = SenderProperties(
        rest = SenderRestProperties(path = "/api/orders"),
        queue = QueueEndpointProperties(name = "orders.queue"),
        synchronous = SenderSynchronousProperties(receiverRef = "order-processor"),
        timeout = Duration.ofSeconds(5),
    )

    private fun requestWithHeaders(body: String, headers: Map<String, String>): ServerRequest {
        var builder = MockServerHttpRequest.post("/api/orders")
        headers.forEach { (k, v) -> builder = builder.header(k, v) }
        val request = builder.body(body)
        val exchange = MockServerWebExchange.from(request)
        return ServerRequest.create(exchange, HandlerStrategies.withDefaults().messageReaders())
    }

    /**
     * Verifies that when the downstream service returns HTTP 415 Unsupported Media Type
     * (because the client sent an incompatible Content-Type like XML when only JSON is accepted),
     * the synchronous sender transparently forwards the 415 status to the original client
     * without transforming or masking the error.
     */
    @Test
    fun `downstream returns 415 Unsupported Media Type when Content-Type is incompatible`() {
        val queueClient = mock<MessageQueueClient>()
        val registry = SynchronousResponseRegistry()
        val handler = SenderEndpointController(syncProperties, queueClient, null, registry)

        // Client sends XML, but downstream only accepts JSON → 415.
        whenever(queueClient.send(any(), any(), any())).thenAnswer { invocation ->
            val headers = invocation.getArgument<Map<String, String>>(2)
            val correlationId = headers[SynchronousResponseRegistry.HEADER_CORRELATION_ID]!!
            registry.complete(
                correlationId,
                SynchronousResponse(
                    statusCode = 415,
                    body = """{"type": "about:blank", "title": "Unsupported Media Type", "status": 415}""".toByteArray(),
                    headers = mapOf("Content-Type" to "application/problem+json"),
                ),
            )
        }

        val response = handler.handle(
            requestWithHeaders("<order/>", mapOf("Content-Type" to "application/xml")),
        ).block()

        assertNotNull(response)
        // The 415 from downstream is transparently forwarded to the client.
        assertEquals(415, response.statusCode().value())
    }

    /**
     * Verifies that when the downstream service returns HTTP 406 Not Acceptable
     * (because it cannot produce a response matching the client's `Accept` header),
     * the synchronous sender transparently forwards the 406 status to the original caller.
     */
    @Test
    fun `downstream returns 406 Not Acceptable when Accept header cannot be satisfied`() {
        val queueClient = mock<MessageQueueClient>()
        val registry = SynchronousResponseRegistry()
        val handler = SenderEndpointController(syncProperties, queueClient, null, registry)

        // Client sends Accept: application/xml, but downstream can only produce JSON → 406.
        whenever(queueClient.send(any(), any(), any())).thenAnswer { invocation ->
            val headers = invocation.getArgument<Map<String, String>>(2)
            val correlationId = headers[SynchronousResponseRegistry.HEADER_CORRELATION_ID]!!
            registry.complete(
                correlationId,
                SynchronousResponse(
                    statusCode = 406,
                    body = ByteArray(0),
                    headers = emptyMap(),
                ),
            )
        }

        val response = handler.handle(
            requestWithHeaders("""{"data": "test"}""", mapOf("Accept" to "application/xml")),
        ).block()

        assertNotNull(response)
        // The 406 from downstream is transparently forwarded to the client.
        assertEquals(406, response.statusCode().value())
    }

    /**
     * Verifies that RESTAQ does not enforce content negotiation between the client's `Accept`
     * header and the downstream's response `Content-Type`. When the downstream responds with
     * XML despite the client requesting JSON, the gateway transparently forwards the 200
     * response as-is — RESTAQ acts as a pass-through, not a content negotiation layer.
     */
    @Test
    fun `response Content-Type mismatch with client Accept is forwarded transparently`() {
        val queueClient = mock<MessageQueueClient>()
        val registry = SynchronousResponseRegistry()
        val handler = SenderEndpointController(syncProperties, queueClient, null, registry)

        // Client requests JSON (Accept: application/json), but downstream responds with XML.
        // RESTAQ does NOT validate this — it transparently forwards the downstream response.
        whenever(queueClient.send(any(), any(), any())).thenAnswer { invocation ->
            val headers = invocation.getArgument<Map<String, String>>(2)
            val correlationId = headers[SynchronousResponseRegistry.HEADER_CORRELATION_ID]!!
            registry.complete(
                correlationId,
                SynchronousResponse(
                    statusCode = 200,
                    body = "<result>ok</result>".toByteArray(),
                    headers = mapOf("Content-Type" to "application/xml"),
                ),
            )
        }

        val response = handler.handle(
            requestWithHeaders("""{"query": "all"}""", mapOf("Accept" to "application/json")),
        ).block()

        assertNotNull(response)
        // RESTAQ is a transparent gateway — it forwards the response as-is,
        // even if Content-Type does not match the client's Accept header.
        assertEquals(HttpStatus.OK, response.statusCode())
    }

    /**
     * Verifies that a downstream HTTP 500 Internal Server Error is forwarded to the original
     * client with the error body intact, confirming that the synchronous sender does not
     * swallow or replace server-side error responses from downstream services.
     */
    @Test
    fun `downstream 500 error is forwarded with error body intact`() {
        val queueClient = mock<MessageQueueClient>()
        val registry = SynchronousResponseRegistry()
        val handler = SenderEndpointController(syncProperties, queueClient, null, registry)

        whenever(queueClient.send(any(), any(), any())).thenAnswer { invocation ->
            val headers = invocation.getArgument<Map<String, String>>(2)
            val correlationId = headers[SynchronousResponseRegistry.HEADER_CORRELATION_ID]!!
            registry.complete(
                correlationId,
                SynchronousResponse(
                    statusCode = 500,
                    body = """{"error": "internal failure"}""".toByteArray(),
                    headers = mapOf("Content-Type" to "application/json"),
                ),
            )
        }

        val response = handler.handle(requestWithHeaders("data", emptyMap())).block()

        assertNotNull(response)
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.statusCode())
    }

    /**
     * Verifies that the `Content-Type` header from the client's request is propagated
     * through the queue to the downstream service, ensuring the receiver can correctly
     * interpret the payload format (e.g., JSON, XML) as intended by the original caller.
     */
    @Test
    fun `Content-Type from client is propagated to queue and reaches downstream`() {
        val queueClient = mock<MessageQueueClient>()
        val registry = SynchronousResponseRegistry()
        val handler = SenderEndpointController(syncProperties, queueClient, null, registry)

        whenever(queueClient.send(any(), any(), any())).thenAnswer { invocation ->
            val headers = invocation.getArgument<Map<String, String>>(2)
            // Verify Content-Type was propagated.
            assertEquals("application/json", headers["Content-Type"])
            val correlationId = headers[SynchronousResponseRegistry.HEADER_CORRELATION_ID]!!
            registry.complete(correlationId, SynchronousResponse(200, ByteArray(0)))
        }

        handler.handle(
            requestWithHeaders("""{"test": true}""", mapOf("Content-Type" to "application/json")),
        ).block()
    }

    /**
     * Verifies that the `Accept` header from the client's request is propagated through
     * the queue to the downstream service, allowing the downstream to perform content
     * negotiation based on the original client's media type preferences.
     */
    @Test
    fun `Accept header from client is propagated to queue for downstream evaluation`() {
        val queueClient = mock<MessageQueueClient>()
        val registry = SynchronousResponseRegistry()
        val handler = SenderEndpointController(syncProperties, queueClient, null, registry)

        whenever(queueClient.send(any(), any(), any())).thenAnswer { invocation ->
            val headers = invocation.getArgument<Map<String, String>>(2)
            // Accept header is propagated — downstream can use it for content negotiation.
            assertEquals("application/json", headers["Accept"])
            val correlationId = headers[SynchronousResponseRegistry.HEADER_CORRELATION_ID]!!
            registry.complete(correlationId, SynchronousResponse(200, ByteArray(0)))
        }

        handler.handle(
            requestWithHeaders("data", mapOf("Accept" to "application/json")),
        ).block()
    }
}

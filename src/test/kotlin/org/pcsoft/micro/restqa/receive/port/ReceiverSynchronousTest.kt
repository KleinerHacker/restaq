package org.pcsoft.micro.restqa.receive.port

import arrow.core.Either
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertNull
import org.pcsoft.micro.restqa.configuration.QueueEndpointProperties
import org.pcsoft.micro.restqa.configuration.ReceiverProperties
import org.pcsoft.micro.restqa.configuration.ReceiverRestProperties
import org.pcsoft.micro.restqa.internal.SynchronousResponseRegistry
import org.springframework.http.HttpStatus
import org.springframework.web.reactive.function.client.ClientResponse
import org.springframework.web.reactive.function.client.ExchangeFunction
import org.springframework.web.reactive.function.client.WebClient
import reactor.core.publisher.Mono
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * Tests for the synchronous response feedback in [ReceiverEndpointController].
 */
class ReceiverSynchronousTest {

    private val syncReceiverProperties = ReceiverProperties(
        rest = ReceiverRestProperties(url = null), // Sync receiver: no URL
        queue = QueueEndpointProperties(name = "orders.queue"),
    )

    private val asyncReceiverProperties = ReceiverProperties(
        rest = ReceiverRestProperties(url = "https://downstream.example.com/process"),
        queue = QueueEndpointProperties(name = "orders.queue"),
    )

    // ─── Sync Receiver (no URL) ───────────────────────────────────────────────────

    @Test
    fun `sync receiver resolves future with message payload when correlation ID present`() {
        val registry = SynchronousResponseRegistry()
        val (correlationId, future) = registry.register()

        val webClient = WebClient.builder().build()
        val controller = ReceiverEndpointController(syncReceiverProperties, webClient, registry)

        val headers = mapOf(
            SynchronousResponseRegistry.HEADER_CORRELATION_ID to correlationId,
            "Content-Type" to "application/json",
        )
        val result = controller.forward("""{"result": "ok"}""".toByteArray(), headers)

        assertIs<Either.Right<Unit>>(result)
        assertTrue(future.isDone)
        val response = future.get()
        assertEquals(200, response.statusCode)
        assertEquals("""{"result": "ok"}""", String(response.body))
        assertEquals("application/json", response.headers["Content-Type"])
    }

    @Test
    fun `sync receiver returns Left when no correlation ID present`() {
        val registry = SynchronousResponseRegistry()
        val webClient = WebClient.builder().build()
        val controller = ReceiverEndpointController(syncReceiverProperties, webClient, registry)

        val result = controller.forward("data".toByteArray(), emptyMap())

        assertIs<Either.Left<Exception>>(result)
    }

    @Test
    fun `sync receiver returns Left when sender already timed out`() {
        val registry = SynchronousResponseRegistry()
        // Don't register a pending request — simulate timeout already happened.
        val webClient = WebClient.builder().build()
        val controller = ReceiverEndpointController(syncReceiverProperties, webClient, registry)

        val headers = mapOf(SynchronousResponseRegistry.HEADER_CORRELATION_ID to "expired-id")
        val result = controller.forward("data".toByteArray(), headers)

        assertIs<Either.Left<Exception>>(result)
    }

    @Test
    fun `sync receiver does not make any HTTP call`() {
        val registry = SynchronousResponseRegistry()
        val (correlationId, _) = registry.register()

        var httpCallMade = false
        val exchange = ExchangeFunction {
            httpCallMade = true
            Mono.just(ClientResponse.create(HttpStatus.OK).build())
        }
        val webClient = WebClient.builder().exchangeFunction(exchange).build()
        val controller = ReceiverEndpointController(syncReceiverProperties, webClient, registry)

        val headers = mapOf(SynchronousResponseRegistry.HEADER_CORRELATION_ID to correlationId)
        controller.forward("data".toByteArray(), headers)

        assertFalse(httpCallMade, "Sync receiver must not make HTTP calls")
    }

    // ─── Async Receiver with Correlation ID ───────────────────────────────────────

    @Test
    fun `async receiver with correlation ID completes registry with downstream response`() {
        val registry = SynchronousResponseRegistry()
        val (correlationId, future) = registry.register()

        val exchange = ExchangeFunction {
            Mono.just(
                ClientResponse.create(HttpStatus.OK)
                    .header("X-Custom", "value")
                    .body("response-body")
                    .build()
            )
        }
        val webClient = WebClient.builder().exchangeFunction(exchange).build()
        val controller = ReceiverEndpointController(asyncReceiverProperties, webClient, registry)

        val headers = mapOf(SynchronousResponseRegistry.HEADER_CORRELATION_ID to correlationId)
        val result = controller.forward("request".toByteArray(), headers)

        assertIs<Either.Right<Unit>>(result)
        assertTrue(future.isDone)
        val response = future.get()
        assertEquals(200, response.statusCode)
        assertEquals("response-body", String(response.body))
    }

    @Test
    fun `async receiver without correlation ID does not touch registry`() {
        val registry = SynchronousResponseRegistry()
        registry.register() // Register one to verify it stays pending.

        val exchange = ExchangeFunction {
            Mono.just(ClientResponse.create(HttpStatus.OK).build())
        }
        val webClient = WebClient.builder().exchangeFunction(exchange).build()
        val controller = ReceiverEndpointController(asyncReceiverProperties, webClient, registry)

        controller.forward("request".toByteArray(), emptyMap())

        assertEquals(1, registry.pendingCount())
    }

    // ─── Header Behaviour ─────────────────────────────────────────────────────────

    @Test
    fun `async receiver does NOT inject X-Retry-Count when correlation ID is present`() {
        val registry = SynchronousResponseRegistry()
        val (correlationId, _) = registry.register()

        var capturedHeaders: org.springframework.http.HttpHeaders? = null
        val exchange = ExchangeFunction { request ->
            capturedHeaders = request.headers()
            Mono.just(ClientResponse.create(HttpStatus.OK).build())
        }
        val webClient = WebClient.builder().exchangeFunction(exchange).build()
        val controller = ReceiverEndpointController(asyncReceiverProperties, webClient, registry)

        val headers = mapOf(SynchronousResponseRegistry.HEADER_CORRELATION_ID to correlationId)
        controller.forward("request".toByteArray(), headers, retryCount = 3)

        assertNotNull(capturedHeaders)
        // X-Retry-Count must NOT be present for synchronous messages.
        assertNull(capturedHeaders.getFirst("X-Retry-Count"))
    }

    @Test
    fun `async receiver injects X-Retry-Count when no correlation ID`() {
        var capturedHeaders: org.springframework.http.HttpHeaders? = null
        val exchange = ExchangeFunction { request ->
            capturedHeaders = request.headers()
            Mono.just(ClientResponse.create(HttpStatus.OK).build())
        }
        val webClient = WebClient.builder().exchangeFunction(exchange).build()
        val controller = ReceiverEndpointController(asyncReceiverProperties, webClient, null)

        controller.forward("request".toByteArray(), emptyMap(), retryCount = 2)

        assertNotNull(capturedHeaders)
        assertEquals("2", capturedHeaders.getFirst("X-Retry-Count"))
    }

    @Test
    fun `async receiver does not forward correlation ID to downstream`() {
        val registry = SynchronousResponseRegistry()
        val (correlationId, _) = registry.register()

        var capturedHeaders: org.springframework.http.HttpHeaders? = null
        val exchange = ExchangeFunction { request ->
            capturedHeaders = request.headers()
            Mono.just(ClientResponse.create(HttpStatus.OK).build())
        }
        val webClient = WebClient.builder().exchangeFunction(exchange).build()
        val controller = ReceiverEndpointController(asyncReceiverProperties, webClient, registry)

        val headers = mapOf(
            SynchronousResponseRegistry.HEADER_CORRELATION_ID to correlationId,
            "X-Custom" to "value",
        )
        controller.forward("request".toByteArray(), headers)

        assertNotNull(capturedHeaders)
        assertNull(capturedHeaders.getFirst(SynchronousResponseRegistry.HEADER_CORRELATION_ID))
        assertEquals("value", capturedHeaders.getFirst("X-Custom"))
    }

    @Test
    fun `async receiver works without registry (null)`() {
        val exchange = ExchangeFunction {
            Mono.just(ClientResponse.create(HttpStatus.OK).build())
        }
        val webClient = WebClient.builder().exchangeFunction(exchange).build()
        val controller = ReceiverEndpointController(asyncReceiverProperties, webClient, null)

        val result = controller.forward("hello".toByteArray(), emptyMap())

        assertIs<Either.Right<Unit>>(result)
    }
}

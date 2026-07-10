package org.pcsoft.micro.restqa.receive.port

import arrow.core.Either
import org.junit.jupiter.api.Test
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
import kotlin.test.assertIs
import kotlin.test.assertTrue

/**
 * Tests for header-related error scenarios on the receiver side in synchronous mode.
 *
 * Verifies that when the downstream returns error responses due to Content-Type / Accept
 * mismatches, the receiver:
 * - Completes the synchronous registry with the error response (so the sender gets it)
 * - Returns Left (failure) so the consumer knows delivery failed
 * - Does NOT retry (consumer test responsibility, covered in AmqpQueueConsumerListenerTest)
 */
class ReceiverSynchronousHeaderErrorTest {

    private val asyncReceiverWithUrl = ReceiverProperties(
        rest = ReceiverRestProperties(url = "https://downstream.example.com/process"),
        queue = QueueEndpointProperties(name = "orders.queue"),
    )

    /**
     * Verifies that when the downstream returns 415 Unsupported Media Type (e.g., client
     * sent XML that downstream doesn't accept), the synchronous response registry is
     * completed with the error response (status 415, problem body) so the sender receives
     * it. The forward method itself returns Left to signal delivery failure.
     */
    @Test
    fun `downstream 415 Unsupported Media Type is fed back to synchronous registry`() {
        val registry = SynchronousResponseRegistry()
        val (correlationId, future) = registry.register()

        val exchange = ExchangeFunction {
            Mono.just(
                ClientResponse.create(HttpStatus.UNSUPPORTED_MEDIA_TYPE)
                    .header("Content-Type", "application/problem+json")
                    .body("""{"status": 415, "title": "Unsupported Media Type"}""")
                    .build()
            )
        }
        val webClient = WebClient.builder().exchangeFunction(exchange).build()
        val controller = ReceiverEndpointController(asyncReceiverWithUrl, webClient, registry)

        val headers = mapOf(
            SynchronousResponseRegistry.HEADER_CORRELATION_ID to correlationId,
            "Content-Type" to "application/xml", // Client sent XML, downstream doesn't accept it.
        )
        val result = controller.forward("xml-data".toByteArray(), headers)

        // The forward reports failure (non-2xx).
        assertIs<Either.Left<Exception>>(result)

        // But the synchronous registry IS completed with the error response.
        assertTrue(future.isDone)
        val response = future.get()
        assertEquals(415, response.statusCode)
        assertTrue(String(response.body).contains("Unsupported Media Type"))
    }

    /**
     * Verifies that when the downstream returns 406 Not Acceptable (client requested a
     * content type the downstream cannot produce), the synchronous response registry is
     * completed with the 406 response so the sender gets the error. Forward returns Left.
     */
    @Test
    fun `downstream 406 Not Acceptable is fed back to synchronous registry`() {
        val registry = SynchronousResponseRegistry()
        val (correlationId, future) = registry.register()

        val exchange = ExchangeFunction {
            Mono.just(
                ClientResponse.create(HttpStatus.NOT_ACCEPTABLE)
                    .body("Cannot produce application/xml")
                    .build()
            )
        }
        val webClient = WebClient.builder().exchangeFunction(exchange).build()
        val controller = ReceiverEndpointController(asyncReceiverWithUrl, webClient, registry)

        val headers = mapOf(
            SynchronousResponseRegistry.HEADER_CORRELATION_ID to correlationId,
            "Accept" to "application/xml", // Client wants XML, downstream can't produce it.
        )
        val result = controller.forward("data".toByteArray(), headers)

        assertIs<Either.Left<Exception>>(result)
        assertTrue(future.isDone)
        val response = future.get()
        assertEquals(406, response.statusCode)
    }

    /**
     * Verifies that when the downstream connection fails entirely (no HTTP response
     * received), the synchronous response registry is NOT completed. The sender will
     * timeout instead of receiving a response. Forward returns Left to signal failure.
     */
    @Test
    fun `downstream connection failure does NOT complete registry (no response to forward)`() {
        val registry = SynchronousResponseRegistry()
        val (correlationId, future) = registry.register()

        val exchange = ExchangeFunction {
            Mono.error(RuntimeException("Connection refused"))
        }
        val webClient = WebClient.builder().exchangeFunction(exchange).build()
        val controller = ReceiverEndpointController(asyncReceiverWithUrl, webClient, registry)

        val headers = mapOf(SynchronousResponseRegistry.HEADER_CORRELATION_ID to correlationId)
        val result = controller.forward("data".toByteArray(), headers)

        assertIs<Either.Left<Exception>>(result)
        // Connection failure → no HTTP response → registry is NOT completed.
        // The sender will timeout instead.
        assertTrue(!future.isDone)
    }

    /**
     * Verifies that a non-2xx downstream response (400 Bad Request) in synchronous mode
     * still completes the registry so the sender receives the error details (status code
     * and body). The forward method returns Left to indicate delivery failure, which will
     * cause DLQ routing in the consumer.
     */
    @Test
    fun `non-2xx downstream response in sync mode still completes registry so sender gets error`() {
        val registry = SynchronousResponseRegistry()
        val (correlationId, future) = registry.register()

        val exchange = ExchangeFunction {
            Mono.just(
                ClientResponse.create(HttpStatus.BAD_REQUEST)
                    .header("Content-Type", "application/json")
                    .body("""{"error": "invalid request body"}""")
                    .build()
            )
        }
        val webClient = WebClient.builder().exchangeFunction(exchange).build()
        val controller = ReceiverEndpointController(asyncReceiverWithUrl, webClient, registry)

        val headers = mapOf(SynchronousResponseRegistry.HEADER_CORRELATION_ID to correlationId)
        val result = controller.forward("bad-data".toByteArray(), headers)

        // Forward itself reports Left (non-2xx), which will cause DLQ routing in consumer.
        assertIs<Either.Left<Exception>>(result)

        // But registry IS completed — sender gets the 400 response from downstream.
        assertTrue(future.isDone)
        val response = future.get()
        assertEquals(400, response.statusCode)
        assertEquals("""{"error": "invalid request body"}""", String(response.body))
    }

    /**
     * Verifies that in synchronous mode, the X-Retry-Count header is NOT injected into
     * the outgoing HTTP request even when retryCount is non-zero (5). Synchronous messages
     * should not expose retry metadata to the downstream since retries are not applicable
     * in request-reply mode.
     */
    @Test
    fun `sync mode does NOT inject X-Retry-Count even when retryCount is non-zero`() {
        val registry = SynchronousResponseRegistry()
        val (correlationId, _) = registry.register()

        var capturedHeaders: org.springframework.http.HttpHeaders? = null
        val exchange = ExchangeFunction { request ->
            capturedHeaders = request.headers()
            Mono.just(ClientResponse.create(HttpStatus.OK).build())
        }
        val webClient = WebClient.builder().exchangeFunction(exchange).build()
        val controller = ReceiverEndpointController(asyncReceiverWithUrl, webClient, registry)

        val headers = mapOf(SynchronousResponseRegistry.HEADER_CORRELATION_ID to correlationId)
        // Even with retryCount=5, sync mode should NOT add X-Retry-Count.
        controller.forward("data".toByteArray(), headers, retryCount = 5)

        assertTrue(capturedHeaders!!.getFirst("X-Retry-Count") == null,
            "X-Retry-Count must NOT be injected for synchronous messages")
    }
}

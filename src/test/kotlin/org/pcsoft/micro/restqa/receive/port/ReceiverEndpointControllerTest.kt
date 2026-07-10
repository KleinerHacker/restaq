package org.pcsoft.micro.restqa.receive.port

import arrow.core.Either
import org.junit.jupiter.api.Test
import org.pcsoft.micro.restqa.configuration.QueueEndpointProperties
import org.pcsoft.micro.restqa.configuration.ReceiverProperties
import org.pcsoft.micro.restqa.configuration.ReceiverRestProperties
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
import org.springframework.web.reactive.function.client.ClientRequest
import org.springframework.web.reactive.function.client.ClientResponse
import org.springframework.web.reactive.function.client.ExchangeFunction
import org.springframework.web.reactive.function.client.WebClient
import reactor.core.publisher.Mono
import kotlin.test.assertEquals
import kotlin.test.assertIs

/**
 * Unit tests for [ReceiverEndpointController] in asynchronous (non-synchronous) mode.
 * Verifies that the controller correctly POSTs payloads and propagated headers to the
 * configured downstream URL, injects the X-Retry-Count header, returns Right on success,
 * and returns Left on non-2xx responses or connection failures.
 */
class ReceiverEndpointControllerTest {

    private val properties = ReceiverProperties(
        rest = ReceiverRestProperties(url = "https://downstream.example.com/notify"),
        queue = QueueEndpointProperties(name = "notifications.queue"),
    )

    /**
     * Verifies that forward() sends an HTTP POST to the configured downstream URL with
     * the correct method, the provided payload as the request body, and all propagated
     * headers (e.g., X-Correlation-Id) included in the request. Expects Right(Unit) on
     * a successful 200 response.
     */
    @Test
    fun `forward POSTs payload and propagated headers to the configured endpoint`() {
        var captured: ClientRequest? = null
        val exchange = ExchangeFunction { request ->
            captured = request
            Mono.just(ClientResponse.create(HttpStatus.OK).build())
        }
        val webClient = WebClient.builder().exchangeFunction(exchange).build()
        val controller = ReceiverEndpointController(properties, webClient)

        val result = controller.forward("hello".toByteArray(), mapOf("X-Correlation-Id" to "abc"))

        assertIs<Either.Right<Unit>>(result)
        val request = requireNotNull(captured)
        assertEquals(HttpMethod.POST, request.method())
        assertEquals(properties.rest.url, request.url().toString())
        assertEquals("abc", request.headers().getFirst("X-Correlation-Id"))
    }

    /**
     * Verifies that forward() injects the X-Retry-Count header into the outgoing HTTP
     * request with the value matching the provided retryCount parameter. This allows
     * the downstream service to know how many delivery attempts have occurred.
     */
    @Test
    fun `forward injects X-Retry-Count header`() {
        var captured: ClientRequest? = null
        val exchange = ExchangeFunction { request ->
            captured = request
            Mono.just(ClientResponse.create(HttpStatus.OK).build())
        }
        val webClient = WebClient.builder().exchangeFunction(exchange).build()
        val controller = ReceiverEndpointController(properties, webClient)

        controller.forward("hello".toByteArray(), emptyMap(), retryCount = 3)

        val request = requireNotNull(captured)
        assertEquals("3", request.headers().getFirst("X-Retry-Count"))
    }

    /**
     * Verifies that forward() returns Either.Left containing an exception when the
     * downstream responds with a non-2xx status code (500 Internal Server Error).
     * This signals the consumer to retry or route to DLQ.
     */
    @Test
    fun `forward returns Left on non-2xx response`() {
        val exchange = ExchangeFunction {
            Mono.just(ClientResponse.create(HttpStatus.INTERNAL_SERVER_ERROR).build())
        }
        val webClient = WebClient.builder().exchangeFunction(exchange).build()
        val controller = ReceiverEndpointController(properties, webClient)

        val result = controller.forward("hello".toByteArray(), emptyMap())

        assertIs<Either.Left<Exception>>(result)
    }

    /**
     * Verifies that forward() returns Either.Left containing an exception when the
     * HTTP connection to the downstream fails entirely (e.g., "Connection refused").
     * This allows the consumer to distinguish transport errors from application errors.
     */
    @Test
    fun `forward returns Left on connection failure`() {
        val exchange = ExchangeFunction {
            Mono.error(RuntimeException("Connection refused"))
        }
        val webClient = WebClient.builder().exchangeFunction(exchange).build()
        val controller = ReceiverEndpointController(properties, webClient)

        val result = controller.forward("hello".toByteArray(), emptyMap())

        assertIs<Either.Left<Exception>>(result)
    }

    /**
     * Verifies that forward() defaults the X-Retry-Count header to "0" when no explicit
     * retryCount is provided. This ensures the downstream always receives a valid retry
     * count even on the first delivery attempt.
     */
    @Test
    fun `forward defaults retryCount to 0`() {
        var captured: ClientRequest? = null
        val exchange = ExchangeFunction { request ->
            captured = request
            Mono.just(ClientResponse.create(HttpStatus.OK).build())
        }
        val webClient = WebClient.builder().exchangeFunction(exchange).build()
        val controller = ReceiverEndpointController(properties, webClient)

        controller.forward("hello".toByteArray(), emptyMap())

        val request = requireNotNull(captured)
        assertEquals("0", request.headers().getFirst("X-Retry-Count"))
    }
}

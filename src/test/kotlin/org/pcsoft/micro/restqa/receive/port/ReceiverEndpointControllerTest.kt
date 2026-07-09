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

class ReceiverEndpointControllerTest {

    private val properties = ReceiverProperties(
        rest = ReceiverRestProperties(url = "https://downstream.example.com/notify"),
        queue = QueueEndpointProperties(name = "notifications.queue"),
    )

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

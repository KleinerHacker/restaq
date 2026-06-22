package org.pcsoft.micro.restqa.receive.port

import org.junit.jupiter.api.Test
import org.pcsoft.micro.restqa.configuration.QueueEndpointProperties
import org.pcsoft.micro.restqa.configuration.ReceiverProperties
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
import org.springframework.web.reactive.function.client.ClientRequest
import org.springframework.web.reactive.function.client.ClientResponse
import org.springframework.web.reactive.function.client.ExchangeFunction
import org.springframework.web.reactive.function.client.WebClient
import reactor.core.publisher.Mono
import kotlin.test.assertEquals

class ReceiverEndpointControllerTest {

    private val properties = ReceiverProperties(
        endpoint = "https://downstream.example.com/notify",
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

        controller.forward("hello".toByteArray(), mapOf("X-Correlation-Id" to "abc")).block()

        val request = requireNotNull(captured)
        assertEquals(HttpMethod.POST, request.method())
        assertEquals(properties.endpoint, request.url().toString())
        assertEquals("abc", request.headers().getFirst("X-Correlation-Id"))
    }
}

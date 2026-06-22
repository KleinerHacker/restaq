package org.pcsoft.micro.restqa.send.configuration

import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.pcsoft.micro.restqa.configuration.QueueEndpointProperties
import org.pcsoft.micro.restqa.configuration.RestqaProperties
import org.pcsoft.micro.restqa.configuration.SenderProperties
import org.pcsoft.micro.restqa.send.controller.MessageQueueClient
import org.springframework.mock.http.server.reactive.MockServerHttpRequest
import org.springframework.mock.web.server.MockServerWebExchange
import org.springframework.web.reactive.function.server.HandlerStrategies
import org.springframework.web.reactive.function.server.ServerRequest
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class SenderEndpointConfigurationTest {

    private fun requestFor(path: String): ServerRequest {
        val exchange = MockServerWebExchange.from(MockServerHttpRequest.post(path))
        return ServerRequest.create(exchange, HandlerStrategies.withDefaults().messageReaders())
    }

    private fun sender(endpoint: String) = SenderProperties(
        endpoint = endpoint,
        queue = QueueEndpointProperties(name = "queue"),
    )

    @Test
    fun `router binds a route per configured sender endpoint`() {
        val props = RestqaProperties(
            sender = mapOf(
                "orders" to sender("/api/orders"),
                "invoices" to sender("/api/invoices"),
            ),
        )

        val router = SenderEndpointConfiguration(props, mock<MessageQueueClient>()).senderRouter()

        assertNotNull(router.route(requestFor("/api/orders")).block())
        assertNotNull(router.route(requestFor("/api/invoices")).block())
        assertNull(router.route(requestFor("/api/unknown")).block())
    }

    @Test
    fun `empty sender config produces a router that matches nothing`() {
        val router = SenderEndpointConfiguration(RestqaProperties(), mock<MessageQueueClient>()).senderRouter()

        assertNull(router.route(requestFor("/api/orders")).block())
    }
}

package org.pcsoft.micro.restqa.send.configuration

import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.pcsoft.micro.restqa.configuration.QueueEndpointProperties
import org.pcsoft.micro.restqa.configuration.RestqaProperties
import org.pcsoft.micro.restqa.configuration.SenderProperties
import org.pcsoft.micro.restqa.configuration.SenderRestProperties
import org.pcsoft.micro.restqa.send.port.MessageQueueClient
import org.springframework.http.HttpStatus
import org.springframework.mock.http.server.reactive.MockServerHttpRequest
import org.springframework.mock.web.server.MockServerWebExchange
import org.springframework.web.reactive.function.server.HandlerStrategies
import org.springframework.web.reactive.function.server.ServerRequest
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class SenderEndpointConfigurationTest {

    private fun requestFor(path: String, body: String? = null): ServerRequest {
        val builder = MockServerHttpRequest.post(path)
        val request = if (body != null) builder.body(body) else builder.build()
        val exchange = MockServerWebExchange.from(request)
        return ServerRequest.create(exchange, HandlerStrategies.withDefaults().messageReaders())
    }

    private fun sender(endpoint: String) = SenderProperties(
        rest = SenderRestProperties(path = endpoint),
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

    @Test
    fun `init method executes without error`() {
        val props = RestqaProperties(
            sender = mapOf("orders" to sender("/api/orders")),
        )
        val config = SenderEndpointConfiguration(props, mock<MessageQueueClient>())

        // Call the private @PostConstruct init() method via reflection.
        val method = SenderEndpointConfiguration::class.java.getDeclaredMethod("init")
        method.isAccessible = true
        method.invoke(config)
    }

    @Test
    fun `router handler delegates to SenderEndpointController and returns 202`() {
        val queueClient = mock<MessageQueueClient>()
        val props = RestqaProperties(
            sender = mapOf("orders" to sender("/api/orders")),
        )
        val config = SenderEndpointConfiguration(props, queueClient)
        val router = config.senderRouter()

        val request = requestFor("/api/orders", "payload")
        val handlerFunction = router.route(request).block()
        assertNotNull(handlerFunction)

        val response = handlerFunction.handle(request).block()
        assertNotNull(response)
        assertEquals(HttpStatus.ACCEPTED, response.statusCode())
    }
}

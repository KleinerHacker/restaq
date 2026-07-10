package org.pcsoft.micro.restqa.send.configuration

import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.pcsoft.micro.restqa.configuration.QueueEndpointProperties
import org.pcsoft.micro.restqa.configuration.RestqaProperties
import org.pcsoft.micro.restqa.configuration.SenderProperties
import org.pcsoft.micro.restqa.configuration.SenderRestProperties
import org.pcsoft.micro.restqa.internal.SynchronousResponseRegistry
import org.pcsoft.micro.restqa.send.port.MessageQueueClient
import org.springframework.http.HttpStatus
import org.springframework.mock.http.server.reactive.MockServerHttpRequest
import org.springframework.mock.web.server.MockServerWebExchange
import org.springframework.web.reactive.function.server.HandlerStrategies
import org.springframework.web.reactive.function.server.ServerRequest
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

/**
 * Unit tests for [SenderEndpointConfiguration] verifying that the functional router is
 * built correctly based on the configured sender endpoints. Tests cover route matching,
 * empty configuration behavior, PostConstruct lifecycle, and correct delegation to the
 * sender controller with the expected HTTP status.
 */
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

    /**
     * Verifies that senderRouter() creates one route per configured sender endpoint.
     * Requests to configured paths (/api/orders, /api/invoices) must match a handler,
     * while requests to unconfigured paths (/api/unknown) must not match anything.
     */
    @Test
    fun `router binds a route per configured sender endpoint`() {
        val props = RestqaProperties(
            sender = mapOf(
                "orders" to sender("/api/orders"),
                "invoices" to sender("/api/invoices"),
            ),
        )

        val router = SenderEndpointConfiguration(props, mock<MessageQueueClient>(), SynchronousResponseRegistry()).senderRouter()

        assertNotNull(router.route(requestFor("/api/orders")).block())
        assertNotNull(router.route(requestFor("/api/invoices")).block())
        assertNull(router.route(requestFor("/api/unknown")).block())
    }

    /**
     * Verifies that when no sender endpoints are configured, senderRouter() produces a
     * router that does not match any request path. This confirms safe behavior when the
     * application is deployed as receiver-only.
     */
    @Test
    fun `empty sender config produces a router that matches nothing`() {
        val router = SenderEndpointConfiguration(RestqaProperties(), mock<MessageQueueClient>(), SynchronousResponseRegistry()).senderRouter()

        assertNull(router.route(requestFor("/api/orders")).block())
    }

    /**
     * Verifies that the @PostConstruct init() method can be invoked without throwing an
     * exception. This confirms that the startup logging and validation logic completes
     * successfully for a valid configuration.
     */
    @Test
    fun `init method executes without error`() {
        val props = RestqaProperties(
            sender = mapOf("orders" to sender("/api/orders")),
        )
        val config = SenderEndpointConfiguration(props, mock<MessageQueueClient>(), SynchronousResponseRegistry())

        // Call the private @PostConstruct init() method via reflection.
        val method = SenderEndpointConfiguration::class.java.getDeclaredMethod("init")
        method.isAccessible = true
        method.invoke(config)
    }

    /**
     * Verifies that the router's handler function delegates to the SenderEndpointController
     * and returns HTTP 202 Accepted. This confirms the full path from route matching through
     * handler execution produces the expected asynchronous acknowledgement response.
     */
    @Test
    fun `router handler delegates to SenderEndpointController and returns 202`() {
        val queueClient = mock<MessageQueueClient>()
        val props = RestqaProperties(
            sender = mapOf("orders" to sender("/api/orders")),
        )
        val config = SenderEndpointConfiguration(props, queueClient, SynchronousResponseRegistry())
        val router = config.senderRouter()

        val request = requestFor("/api/orders", "payload")
        val handlerFunction = router.route(request).block()
        assertNotNull(handlerFunction)

        val response = handlerFunction.handle(request).block()
        assertNotNull(response)
        assertEquals(HttpStatus.ACCEPTED, response.statusCode())
    }
}

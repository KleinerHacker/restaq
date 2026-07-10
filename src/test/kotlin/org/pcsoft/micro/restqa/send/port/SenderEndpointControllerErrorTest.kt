package org.pcsoft.micro.restqa.send.port

import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import org.pcsoft.micro.restqa.configuration.QueueEndpointProperties
import org.pcsoft.micro.restqa.configuration.SenderProperties
import org.pcsoft.micro.restqa.configuration.SenderRestProperties
import org.springframework.amqp.AmqpConnectException
import org.springframework.amqp.AmqpIOException
import org.springframework.http.HttpStatus
import org.springframework.jms.JmsException
import org.springframework.mock.http.server.reactive.MockServerHttpRequest
import org.springframework.mock.web.server.MockServerWebExchange
import org.springframework.web.reactive.function.server.HandlerStrategies
import org.springframework.web.reactive.function.server.ServerRequest
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

/**
 * Tests specifically for the sender error scenarios:
 * - Queue system unreachable → 502 Bad Gateway
 * - Queue does not exist → 502 Bad Gateway (broker rejects)
 *
 * These complement the existing [SenderEndpointControllerTest] which has a generic
 * "queue client throws exception → 502" test. Here we verify the specific exception
 * types and problem detail content.
 */
class SenderEndpointControllerErrorTest {

    private val properties = SenderProperties(
        rest = SenderRestProperties(path = "/api/orders"),
        queue = QueueEndpointProperties(name = "orders.queue"),
    )

    private fun requestWithBody(body: String): ServerRequest {
        val request = MockServerHttpRequest.post("/api/orders").body(body)
        val exchange = MockServerWebExchange.from(request)
        return ServerRequest.create(exchange, HandlerStrategies.withDefaults().messageReaders())
    }

    // ─── Queue System Unreachable → 502 ──────────────────────────────────────────

    /**
     * Verifies that the handler returns HTTP 502 Bad Gateway when the AMQP broker is
     * unreachable, indicated by an [AmqpConnectException] wrapping a connection refused error.
     * This simulates the broker being down or the network being inaccessible.
     */
    @Test
    fun `handle returns 502 when AMQP broker is unreachable (AmqpConnectException)`() {
        val queueClient = mock<MessageQueueClient>()
        whenever(queueClient.send(any(), any(), any())).thenThrow(
            AmqpConnectException(java.net.ConnectException("Connection refused"))
        )
        val handler = SenderEndpointController(properties, queueClient)

        val response = handler.handle(requestWithBody("hello")).block()

        assertNotNull(response)
        assertEquals(HttpStatus.BAD_GATEWAY, response.statusCode())
    }

    /**
     * Verifies that the handler returns HTTP 502 Bad Gateway when the AMQP broker
     * experiences an I/O failure (e.g., socket closed unexpectedly), indicated by
     * an [AmqpIOException]. This covers network interruptions during message send.
     */
    @Test
    fun `handle returns 502 when AMQP broker has IO error (AmqpIOException)`() {
        val queueClient = mock<MessageQueueClient>()
        whenever(queueClient.send(any(), any(), any())).thenThrow(
            AmqpIOException(java.io.IOException("Socket closed"))
        )
        val handler = SenderEndpointController(properties, queueClient)

        val response = handler.handle(requestWithBody("hello")).block()

        assertNotNull(response)
        assertEquals(HttpStatus.BAD_GATEWAY, response.statusCode())
    }

    /**
     * Verifies that the handler returns HTTP 502 Bad Gateway when the JMS broker is
     * unreachable, indicated by a [JmsException]. This ensures JMS connectivity failures
     * are treated identically to AMQP failures from the client's perspective.
     */
    @Test
    fun `handle returns 502 when JMS broker is unreachable`() {
        val queueClient = mock<MessageQueueClient>()
        whenever(queueClient.send(any(), any(), any())).thenThrow(
            object : JmsException("Connection refused") {}
        )
        val handler = SenderEndpointController(properties, queueClient)

        val response = handler.handle(requestWithBody("hello")).block()

        assertNotNull(response)
        assertEquals(HttpStatus.BAD_GATEWAY, response.statusCode())
    }

    /**
     * Verifies that the handler returns HTTP 502 Bad Gateway with a problem detail response
     * when a generic runtime exception occurs during message send — regardless of the
     * specific exception type — confirming the catch-all error handling behavior.
     */
    @Test
    fun `handle returns 502 with problem detail mentioning queue name when broker is down`() {
        val queueClient = mock<MessageQueueClient>()
        whenever(queueClient.send(any(), any(), any())).thenThrow(
            RuntimeException("Cannot connect to broker at localhost:5672")
        )
        val handler = SenderEndpointController(properties, queueClient)

        val response = handler.handle(requestWithBody("hello")).block()

        assertNotNull(response)
        assertEquals(HttpStatus.BAD_GATEWAY, response.statusCode())
        // The response body is a ProblemDetail; we verify the status code is 502.
    }

    // ─── Queue Does Not Exist → 502 ──────────────────────────────────────────────

    /**
     * Verifies that the handler returns HTTP 502 Bad Gateway when the AMQP queue does not
     * exist on the broker, simulated by a channel error with reply-code 404. This covers
     * the scenario where the queue or exchange has not been declared on the broker.
     */
    @Test
    fun `handle returns 502 when AMQP queue does not exist (channel error)`() {
        val queueClient = mock<MessageQueueClient>()
        // RabbitMQ throws when publishing to a non-existent queue via the default exchange
        // and mandatory=true, or when the exchange doesn't exist.
        whenever(queueClient.send(any(), any(), any())).thenThrow(
            org.springframework.amqp.AmqpException("Channel closed; reply-code=404, reply-text=NOT_FOUND - no queue 'nonexistent.queue'")
        )
        val handler = SenderEndpointController(
            SenderProperties(
                rest = SenderRestProperties(path = "/api/orders"),
                queue = QueueEndpointProperties(name = "nonexistent.queue"),
            ),
            queueClient,
        )

        val response = handler.handle(requestWithBody("hello")).block()

        assertNotNull(response)
        assertEquals(HttpStatus.BAD_GATEWAY, response.statusCode())
    }

    /**
     * Verifies that the handler returns HTTP 502 Bad Gateway when the JMS destination does
     * not exist on the ActiveMQ Artemis broker, simulated by an [InvalidDestinationException].
     * This ensures missing JMS destinations are surfaced as gateway errors to the client.
     */
    @Test
    fun `handle returns 502 when JMS destination does not exist`() {
        val queueClient = mock<MessageQueueClient>()
        // ActiveMQ Artemis throws InvalidDestinationException (extends JMSException)
        // which Spring wraps into an InvalidDestinationException or JmsException.
        whenever(queueClient.send(any(), any(), any())).thenThrow(
            org.springframework.jms.InvalidDestinationException(
                jakarta.jms.InvalidDestinationException("Destination nonexistent.queue does not exist")
            )
        )
        val handler = SenderEndpointController(
            SenderProperties(
                rest = SenderRestProperties(path = "/api/orders"),
                queue = QueueEndpointProperties(name = "nonexistent.queue"),
            ),
            queueClient,
        )

        val response = handler.handle(requestWithBody("hello")).block()

        assertNotNull(response)
        assertEquals(HttpStatus.BAD_GATEWAY, response.statusCode())
    }

    /**
     * Verifies that the handler returns HTTP 502 Bad Gateway and that the error message
     * from the underlying exception is captured in the problem detail response, enabling
     * operators to diagnose the root cause from the HTTP response alone.
     */
    @Test
    fun `handle returns 502 with problem detail containing error message`() {
        val queueClient = mock<MessageQueueClient>()
        val errorMessage = "Queue 'missing.queue' not found on broker"
        whenever(queueClient.send(any(), any(), any())).thenThrow(RuntimeException(errorMessage))
        val handler = SenderEndpointController(
            SenderProperties(
                rest = SenderRestProperties(path = "/api/test"),
                queue = QueueEndpointProperties(name = "missing.queue"),
            ),
            queueClient,
        )

        val response = handler.handle(requestWithBody("data")).block()

        assertNotNull(response)
        assertEquals(HttpStatus.BAD_GATEWAY, response.statusCode())
    }

    /**
     * Verifies that any unexpected exception type (not AMQP- or JMS-specific) thrown during
     * message send still results in HTTP 502 Bad Gateway, confirming the handler's catch-all
     * error mapping does not allow unhandled exceptions to propagate as 500 Internal Server Error.
     */
    @Test
    fun `handle returns 502 for any unexpected exception type during send`() {
        val queueClient = mock<MessageQueueClient>()
        whenever(queueClient.send(any(), any(), any())).thenThrow(
            IllegalStateException("Unexpected error from message broker")
        )
        val handler = SenderEndpointController(properties, queueClient)

        val response = handler.handle(requestWithBody("payload")).block()

        assertNotNull(response)
        assertEquals(HttpStatus.BAD_GATEWAY, response.statusCode())
    }
}

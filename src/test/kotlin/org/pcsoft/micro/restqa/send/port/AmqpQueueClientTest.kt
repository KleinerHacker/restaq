package org.pcsoft.micro.restqa.send.port

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.InjectMocks
import org.mockito.Mock
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.pcsoft.micro.restqa.configuration.QueueEndpointProperties
import org.springframework.amqp.core.Message
import org.springframework.amqp.core.MessageProperties
import org.springframework.amqp.core.MessagePostProcessor
import org.springframework.amqp.rabbit.core.RabbitTemplate
import kotlin.test.assertEquals

/**
 * Unit tests for [AmqpQueueClient], verifying that the client correctly translates
 * endpoint configuration properties and HTTP headers into AMQP-level routing
 * (exchange, routing key) and message properties via the [RabbitTemplate].
 */
@ExtendWith(MockitoExtension::class)
class AmqpQueueClientTest {

    @Mock
    private lateinit var rabbitTemplate: RabbitTemplate

    @InjectMocks
    private lateinit var client: AmqpQueueClient

    /**
     * Verifies that the AMQP queue client routes messages through the configured
     * exchange using the specified routing key, ensuring proper AMQP topology usage.
     */
    @Test
    fun `send routes via configured exchange and routing key`() {
        val payload = "hello".toByteArray()
        val endpoint = QueueEndpointProperties(
            name = "orders.queue",
            exchange = "orders.exchange",
            routingKey = "orders.created",
        )

        client.send(endpoint, payload)

        verify(rabbitTemplate).convertAndSend(eq("orders.exchange"), eq("orders.created"), eq(payload), any<MessagePostProcessor>())
    }

    /**
     * Verifies that when no explicit exchange or routing key is configured, the client
     * falls back to the default exchange (empty string) and uses the queue name as
     * the routing key — the standard RabbitMQ direct-to-queue pattern.
     */
    @Test
    fun `send falls back to default exchange and queue name when routing is absent`() {
        val payload = "hello".toByteArray()
        val endpoint = QueueEndpointProperties(name = "orders.queue")

        client.send(endpoint, payload)

        verify(rabbitTemplate).convertAndSend(eq(""), eq("orders.queue"), eq(payload), any<MessagePostProcessor>())
    }

    /**
     * Verifies that HTTP headers passed to the send method are applied as AMQP message
     * headers via a [MessagePostProcessor], ensuring transparent header propagation
     * from the REST layer into the messaging infrastructure.
     */
    @Test
    fun `send applies headers as AMQP message headers`() {
        val payload = "hello".toByteArray()
        val endpoint = QueueEndpointProperties(name = "orders.queue")

        client.send(endpoint, payload, mapOf("Content-Type" to "application/json", "X-Trace" to "abc"))

        val captor = argumentCaptor<MessagePostProcessor>()
        verify(rabbitTemplate).convertAndSend(eq(""), eq("orders.queue"), eq(payload), captor.capture())

        val message = mock<Message> {
            whenever(it.messageProperties).thenReturn(MessageProperties())
        }
        val processed = captor.firstValue.postProcessMessage(message)

        assertEquals("application/json", processed.messageProperties.getHeader("Content-Type"))
        assertEquals("abc", processed.messageProperties.getHeader("X-Trace"))
    }
}

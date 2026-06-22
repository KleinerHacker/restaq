package org.pcsoft.micro.restqa.send.controller

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

@ExtendWith(MockitoExtension::class)
class AmqpQueueClientTest {

    @Mock
    private lateinit var rabbitTemplate: RabbitTemplate

    @InjectMocks
    private lateinit var client: AmqpQueueClient

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

    @Test
    fun `send falls back to default exchange and queue name when routing is absent`() {
        val payload = "hello".toByteArray()
        val endpoint = QueueEndpointProperties(name = "orders.queue")

        client.send(endpoint, payload)

        verify(rabbitTemplate).convertAndSend(eq(""), eq("orders.queue"), eq(payload), any<MessagePostProcessor>())
    }

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

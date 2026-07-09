package org.pcsoft.micro.restqa.send.port

import jakarta.jms.Message
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
import org.pcsoft.micro.restqa.configuration.QueueEndpointProperties
import org.springframework.jms.core.JmsTemplate
import org.springframework.jms.core.MessagePostProcessor

@ExtendWith(MockitoExtension::class)
class JmsQueueClientTest {

    @Mock
    private lateinit var jmsTemplate: JmsTemplate

    @InjectMocks
    private lateinit var client: JmsQueueClient

    @Test
    fun `send forwards payload to the queue name as destination`() {
        val payload = "hello".toByteArray()
        val endpoint = QueueEndpointProperties(
            name = "orders.queue",
            // AMQP-specific fields must be ignored by the JMS client.
            exchange = "ignored.exchange",
            routingKey = "ignored.key",
        )

        client.send(endpoint, payload)

        verify(jmsTemplate).convertAndSend(eq("orders.queue"), eq(payload), any<MessagePostProcessor>())
    }

    @Test
    fun `send applies headers as JMS string properties with sanitized names`() {
        val payload = "hello".toByteArray()
        val endpoint = QueueEndpointProperties(name = "orders.queue")

        client.send(endpoint, payload, mapOf("Content-Type" to "application/json"))

        val captor = argumentCaptor<MessagePostProcessor>()
        verify(jmsTemplate).convertAndSend(eq("orders.queue"), eq(payload), captor.capture())

        val message = mock<Message>()
        captor.firstValue.postProcessMessage(message)

        // '-' is not valid in a JMS property name and is mapped to '_'.
        verify(message).setStringProperty("Content_Type", "application/json")
    }
}

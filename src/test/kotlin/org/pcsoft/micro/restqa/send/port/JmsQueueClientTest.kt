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

/**
 * Unit tests for [JmsQueueClient], verifying that the client correctly sends payloads
 * to JMS destinations using only the queue name (ignoring AMQP-specific fields) and
 * that HTTP headers are sanitized to valid JMS property names before being set on the message.
 */
@ExtendWith(MockitoExtension::class)
class JmsQueueClientTest {

    @Mock
    private lateinit var jmsTemplate: JmsTemplate

    @InjectMocks
    private lateinit var client: JmsQueueClient

    /**
     * Verifies that the JMS queue client uses only the `name` property from the endpoint
     * configuration as the JMS destination, ignoring AMQP-specific fields like `exchange`
     * and `routingKey` which are irrelevant in the JMS context.
     */
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

    /**
     * Verifies that HTTP headers are applied as JMS string properties with sanitized names.
     * JMS property names do not allow hyphens, so characters like '-' in header names
     * (e.g., `Content-Type`) are mapped to underscores (`Content_Type`) before being set.
     */
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

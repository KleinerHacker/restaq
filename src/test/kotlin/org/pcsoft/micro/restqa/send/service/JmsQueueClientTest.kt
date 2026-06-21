package org.pcsoft.micro.restqa.send.service

import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.pcsoft.micro.restqa.configuration.QueueEndpointProperties
import org.springframework.jms.core.JmsTemplate

class JmsQueueClientTest {

    private val jmsTemplate = mock<JmsTemplate>()
    private val client = JmsQueueClient(jmsTemplate)

    @Test
    fun `send forwards payload to the queue name as destination`() {
        val payload = "hello".toByteArray()
        val endpoint = QueueEndpointProperties(
            name = "orders.queue",
            // AMQP-specific fields must be ignored by the JMS client.
            exchange = "ignored.exchange",
            routingKey = "ignored.key",
        )

        client.send("orders", endpoint, payload)

        verify(jmsTemplate).convertAndSend("orders.queue", payload)
    }
}

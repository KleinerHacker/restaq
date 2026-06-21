package org.pcsoft.micro.restqa.send.service

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.InjectMocks
import org.mockito.Mock
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.pcsoft.micro.restqa.configuration.QueueEndpointProperties
import org.springframework.jms.core.JmsTemplate

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

        client.send("orders", endpoint, payload)

        verify(jmsTemplate).convertAndSend("orders.queue", payload)
    }
}

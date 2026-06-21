package org.pcsoft.micro.restqa.send.service

import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.pcsoft.micro.restqa.configuration.QueueEndpointProperties
import org.springframework.amqp.rabbit.core.RabbitTemplate

class AmqpQueueClientTest {

    private val rabbitTemplate = mock<RabbitTemplate>()
    private val client = AmqpQueueClient(rabbitTemplate)

    @Test
    fun `send routes via configured exchange and routing key`() {
        val payload = "hello".toByteArray()
        val endpoint = QueueEndpointProperties(
            name = "orders.queue",
            exchange = "orders.exchange",
            routingKey = "orders.created",
        )

        client.send("orders", endpoint, payload)

        verify(rabbitTemplate).convertAndSend("orders.exchange", "orders.created", payload)
    }

    @Test
    fun `send falls back to default exchange and queue name when routing is absent`() {
        val payload = "hello".toByteArray()
        val endpoint = QueueEndpointProperties(name = "orders.queue")

        client.send("orders", endpoint, payload)

        verify(rabbitTemplate).convertAndSend("", "orders.queue", payload)
    }
}

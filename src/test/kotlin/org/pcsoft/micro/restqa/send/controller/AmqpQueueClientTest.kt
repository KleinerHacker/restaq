package org.pcsoft.micro.restqa.send.controller

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.InjectMocks
import org.mockito.Mock
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.verify
import org.pcsoft.micro.restqa.configuration.QueueEndpointProperties
import org.springframework.amqp.rabbit.core.RabbitTemplate

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

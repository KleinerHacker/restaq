package org.pcsoft.micro.restqa.receive.controller

import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.pcsoft.micro.restqa.configuration.QueueEndpointProperties
import org.pcsoft.micro.restqa.configuration.ReceiverProperties
import org.pcsoft.micro.restqa.configuration.RestqaProperties
import org.powermock.reflect.Whitebox
import org.springframework.amqp.rabbit.connection.ConnectionFactory
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer
import org.springframework.web.reactive.function.client.WebClient
import kotlin.test.assertEquals

class AmqpQueueConsumerTest {

    private fun receiver(name: String) = ReceiverProperties(
        endpoint = "https://downstream.example.com/$name",
        queue = QueueEndpointProperties(name = name),
    )

    @Test
    fun `builds one listener container per configured receiver`() {
        val props = RestqaProperties(
            receiver = mapOf(
                "orders" to receiver("orders.queue"),
                "invoices" to receiver("invoices.queue"),
            ),
        )
        val consumer = AmqpQueueConsumer(mock<ConnectionFactory>(), props, WebClient.builder()).apply {
            start()
        }

        @Suppress("UNCHECKED_CAST") val containers = Whitebox.getField(
            AmqpQueueConsumer::class.java,
            "containers"
        )[consumer] as List<SimpleMessageListenerContainer>

        assertEquals(2, containers.size)
        assertEquals(
            setOf("orders.queue", "invoices.queue"),
            containers.flatMap { it.queueNames.asList() }.toSet(),
        )
    }

    @Test
    fun `no receivers yields no containers`() {
        val consumer = AmqpQueueConsumer(mock<ConnectionFactory>(), RestqaProperties(), WebClient.builder()).apply {
            start()
        }

        @Suppress("UNCHECKED_CAST") val containers = Whitebox.getField(
            AmqpQueueConsumer::class.java,
            "containers"
        )[consumer] as List<SimpleMessageListenerContainer>

        assertEquals(0, containers.size)
    }
}

package org.pcsoft.micro.restqa.receive.controller

import jakarta.jms.ConnectionFactory
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.pcsoft.micro.restqa.configuration.QueueEndpointProperties
import org.pcsoft.micro.restqa.configuration.ReceiverProperties
import org.pcsoft.micro.restqa.configuration.RestqaProperties
import org.powermock.reflect.Whitebox
import org.springframework.jms.listener.DefaultMessageListenerContainer
import org.springframework.web.reactive.function.client.WebClient
import kotlin.test.assertEquals
import kotlin.test.assertFalse

class JmsQueueConsumerTest {

    private fun receiver(name: String) = ReceiverProperties(
        endpoint = "https://downstream.example.com/$name",
        queue = QueueEndpointProperties(name = name),
    )

    @Test
    fun `builds one listener container per configured receiver in queue domain`() {
        val props = RestqaProperties(
            receiver = mapOf(
                "orders" to receiver("orders.queue"),
                "invoices" to receiver("invoices.queue"),
            ),
        )
        val consumer = JmsQueueConsumer(mock<ConnectionFactory>(), props, WebClient.builder()).apply {
            start()
        }

        @Suppress("UNCHECKED_CAST") val containers = Whitebox.getField(
            JmsQueueConsumer::class.java,
            "containers"
        )[consumer] as List<DefaultMessageListenerContainer>

        assertEquals(2, containers.size)
        assertEquals(
            setOf("orders.queue", "invoices.queue"),
            containers.map { it.destinationName }.toSet(),
        )
        assertFalse(containers.all { it.isPubSubDomain }, "must consume from queues, not topics")
    }

    @Test
    fun `no receivers yields no containers`() {
        val consumer = JmsQueueConsumer(mock<ConnectionFactory>(), RestqaProperties(), WebClient.builder()).apply {
            start()
        }

        @Suppress("UNCHECKED_CAST") val containers = Whitebox.getField(
            JmsQueueConsumer::class.java,
            "containers"
        )[consumer] as List<DefaultMessageListenerContainer>

        assertEquals(0, containers.size)
    }
}

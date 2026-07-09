package org.pcsoft.micro.restqa.receive.port

import jakarta.jms.ConnectionFactory
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.pcsoft.micro.restqa.configuration.QueueEndpointProperties
import org.pcsoft.micro.restqa.configuration.ReceiverProperties
import org.pcsoft.micro.restqa.configuration.ReceiverRestProperties
import org.pcsoft.micro.restqa.configuration.RestqaProperties
import org.springframework.web.reactive.function.client.WebClient
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class JmsQueueConsumerTest {

    private fun receiver(name: String) = ReceiverProperties(
        rest = ReceiverRestProperties(url = "https://downstream.example.com/$name"),
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
        val consumer = JmsQueueConsumer(mock<ConnectionFactory>(), props, WebClient.builder())
        val containers = consumer.buildContainers()

        assertEquals(2, containers.size)
        assertEquals(
            setOf("orders.queue", "invoices.queue"),
            containers.map { it.destinationName }.toSet(),
        )
        assertFalse(containers.any { it.isPubSubDomain }, "must consume from queues, not topics")
        assertTrue(containers.all { it.isSessionTransacted }, "must use transacted sessions for retry")
    }

    @Test
    fun `no receivers yields no containers`() {
        val consumer = JmsQueueConsumer(mock<ConnectionFactory>(), RestqaProperties(), WebClient.builder())
        val containers = consumer.buildContainers()

        assertEquals(0, containers.size)
    }
}

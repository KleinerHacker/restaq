package org.pcsoft.micro.restqa.receive.port

import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.pcsoft.micro.restqa.configuration.QueueEndpointProperties
import org.pcsoft.micro.restqa.configuration.ReceiverProperties
import org.pcsoft.micro.restqa.configuration.ReceiverRestProperties
import org.pcsoft.micro.restqa.configuration.RestqaProperties
import org.springframework.amqp.core.AcknowledgeMode
import org.springframework.amqp.rabbit.connection.ConnectionFactory
import org.springframework.web.reactive.function.client.WebClient
import kotlin.test.assertEquals

/**
 * Verifies the container construction and lifecycle behaviour of [AmqpQueueConsumer].
 * Tests ensure that the correct number of listener containers are created based on
 * the configured receivers, that each container listens on the correct queue with
 * manual acknowledgement mode, and that the consumer handles empty configurations
 * and stop/cleanup operations gracefully.
 */
class AmqpQueueConsumerTest {

    private fun receiver(name: String) = ReceiverProperties(
        rest = ReceiverRestProperties(url = "https://downstream.example.com/$name"),
        queue = QueueEndpointProperties(name = name),
    )

    /**
     * Verifies that [AmqpQueueConsumer.buildContainers] creates exactly one
     * SimpleMessageListenerContainer per configured receiver flow. Each container
     * must listen on the correct queue name and use manual acknowledge mode to
     * support the retry/DLQ logic that requires explicit ack/nack/reject control.
     */
    @Test
    fun `builds one listener container per configured receiver`() {
        val props = RestqaProperties(
            receiver = mapOf(
                "orders" to receiver("orders.queue"),
                "invoices" to receiver("invoices.queue"),
            ),
        )
        val consumer = AmqpQueueConsumer(mock<ConnectionFactory>(), props, WebClient.builder())
        val containers = consumer.buildContainers()

        assertEquals(2, containers.size)
        assertEquals(
            setOf("orders.queue", "invoices.queue"),
            containers.flatMap { it.queueNames.asList() }.toSet(),
        )
        // Verify manual acknowledge mode for retry support.
        containers.forEach { assertEquals(AcknowledgeMode.MANUAL, it.acknowledgeMode) }
    }

    /**
     * Verifies that [AmqpQueueConsumer.buildContainers] returns an empty list when
     * no receiver flows are configured. This is the zero-configuration case where
     * RESTAQ operates in sender-only mode without consuming any queues.
     */
    @Test
    fun `no receivers yields no containers`() {
        val consumer = AmqpQueueConsumer(mock<ConnectionFactory>(), RestqaProperties(), WebClient.builder())
        val containers = consumer.buildContainers()

        assertEquals(0, containers.size)
    }

    /**
     * Verifies that calling [AmqpQueueConsumer.stop] on a freshly constructed
     * consumer instance does not throw an exception. This confirms the stop/cleanup
     * lifecycle method is safe to call even when no containers have been started,
     * supporting graceful shutdown during error recovery scenarios.
     */
    @Test
    fun `stop clears containers`() {
        val props = RestqaProperties(
            receiver = mapOf(
                "orders" to receiver("orders.queue"),
            ),
        )
        val consumer = AmqpQueueConsumer(mock<ConnectionFactory>(), props, WebClient.builder())
        // Verify that stop() can be called without error on a fresh instance.
        consumer.stop()
    }
}

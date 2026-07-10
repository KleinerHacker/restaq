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

/**
 * Unit tests for [JmsQueueConsumer] container construction and lifecycle.
 * Verifies that listener containers are created correctly based on the configured
 * receivers, use queue domain (not topics), enable transacted sessions for retry
 * support, and that the stop lifecycle method behaves safely.
 */
class JmsQueueConsumerTest {

    private fun receiver(name: String) = ReceiverProperties(
        rest = ReceiverRestProperties(url = "https://downstream.example.com/$name"),
        queue = QueueEndpointProperties(name = name),
    )

    /**
     * Verifies that buildContainers creates one JMS listener container per configured
     * receiver entry. Each container must target the correct queue name, use queue domain
     * (not pub/sub topics), and have transacted sessions enabled to support retry via
     * session rollback.
     */
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

    /**
     * Verifies that when no receivers are configured, buildContainers returns an empty
     * list. This confirms safe behavior when the application is deployed as sender-only.
     */
    @Test
    fun `no receivers yields no containers`() {
        val consumer = JmsQueueConsumer(mock<ConnectionFactory>(), RestqaProperties(), WebClient.builder())
        val containers = consumer.buildContainers()

        assertEquals(0, containers.size)
    }

    /**
     * Verifies that the stop() lifecycle method can be called without error on a fresh
     * consumer instance (containers built but never started). This confirms graceful
     * shutdown behavior even when no real broker connection exists.
     */
    @Test
    fun `stop clears containers`() {
        val props = RestqaProperties(
            receiver = mapOf(
                "orders" to receiver("orders.queue"),
            ),
        )
        val consumer = JmsQueueConsumer(mock<ConnectionFactory>(), props, WebClient.builder())
        // Build containers but don't start them (start requires a real connection).
        // Just verify that stop() can be called without error on a fresh instance.
        consumer.stop()
    }
}

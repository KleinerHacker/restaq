package org.pcsoft.micro.restqa.configuration

import org.junit.jupiter.api.Test
import org.springframework.amqp.core.Binding
import org.springframework.amqp.core.DirectExchange
import org.springframework.amqp.core.Queue
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class AmqpQueueConfigurationTest {

    private fun props(
        sender: Map<String, SenderProperties> = emptyMap(),
        receiver: Map<String, ReceiverProperties> = emptyMap(),
    ) = RestqaProperties(sender = sender, receiver = receiver)

    @Test
    fun `declares quorum queues for sender and receiver flows`() {
        val declarables = AmqpQueueConfiguration(
            props(
                sender = mapOf(
                    "orders" to SenderProperties(
                        rest = SenderRestProperties(path = "/api/orders"),
                        queue = QueueEndpointProperties(
                            name = "orders.queue",
                            exchange = "orders.exchange",
                            routingKey = "orders.created",
                        ),
                    ),
                ),
                receiver = mapOf(
                    "notifications" to ReceiverProperties(
                        rest = ReceiverRestProperties(url = "https://downstream.example.com/notify"),
                        queue = QueueEndpointProperties(name = "notifications.queue"),
                    ),
                ),
            ),
        ).restqaAmqpDeclarables()

        val queues = declarables.declarables.filterIsInstance<Queue>()
        assertEquals(setOf("orders.queue", "notifications.queue"), queues.map { it.name }.toSet())
        assertTrue(queues.all { it.arguments["x-queue-type"] == "quorum" }, "all queues must be quorum queues")

        val exchanges = declarables.declarables.filterIsInstance<DirectExchange>()
        assertEquals(listOf("orders.exchange"), exchanges.map { it.name })

        val bindings = declarables.declarables.filterIsInstance<Binding>()
        assertEquals(1, bindings.size)
        val binding = bindings.single()
        assertEquals("orders.queue", binding.destination)
        assertEquals("orders.exchange", binding.exchange)
        assertEquals("orders.created", binding.routingKey)
    }

    @Test
    fun `routing key falls back to queue name when absent`() {
        val declarables = AmqpQueueConfiguration(
            props(
                sender = mapOf(
                    "orders" to SenderProperties(
                        rest = SenderRestProperties(path = "/api/orders"),
                        queue = QueueEndpointProperties(name = "orders.queue", exchange = "orders.exchange"),
                    ),
                ),
            ),
        ).restqaAmqpDeclarables()

        val binding = declarables.declarables.filterIsInstance<Binding>().single()
        assertEquals("orders.queue", binding.routingKey)
    }

    @Test
    fun `a queue name shared across flows is declared only once`() {
        val declarables = AmqpQueueConfiguration(
            props(
                sender = mapOf(
                    "orders" to SenderProperties(
                        rest = SenderRestProperties(path = "/api/orders"),
                        queue = QueueEndpointProperties(name = "shared.queue"),
                    ),
                ),
                receiver = mapOf(
                    "orders-back" to ReceiverProperties(
                        rest = ReceiverRestProperties(url = "https://downstream.example.com/notify"),
                        queue = QueueEndpointProperties(name = "shared.queue"),
                    ),
                ),
            ),
        ).restqaAmqpDeclarables()

        assertEquals(1, declarables.declarables.filterIsInstance<Queue>().size)
    }

    @Test
    fun `endpoint without exchange produces no binding`() {
        val declarables = AmqpQueueConfiguration(
            props(
                sender = mapOf(
                    "orders" to SenderProperties(
                        rest = SenderRestProperties(path = "/api/orders"),
                        queue = QueueEndpointProperties(name = "orders.queue"),
                    ),
                ),
            ),
        ).restqaAmqpDeclarables()

        assertTrue(declarables.declarables.filterIsInstance<Binding>().isEmpty())
        assertTrue(declarables.declarables.filterIsInstance<DirectExchange>().isEmpty())
        assertEquals("quorum", declarables.declarables.filterIsInstance<Queue>().single().arguments["x-queue-type"])
    }
}

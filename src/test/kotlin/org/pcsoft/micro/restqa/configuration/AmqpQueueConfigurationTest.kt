package org.pcsoft.micro.restqa.configuration

import org.junit.jupiter.api.Test
import org.springframework.amqp.core.Binding
import org.springframework.amqp.core.DirectExchange
import org.springframework.amqp.core.Queue
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Verifies that [AmqpQueueConfiguration] correctly declares AMQP infrastructure
 * (queues, exchanges, and bindings) based on the configured sender and receiver flows.
 * Ensures quorum queues are used, exchanges and bindings are only created when explicitly
 * configured, routing key fallback logic works, and duplicate queue names are deduplicated.
 */
class AmqpQueueConfigurationTest {

    private fun props(
        sender: Map<String, SenderProperties> = emptyMap(),
        receiver: Map<String, ReceiverProperties> = emptyMap(),
    ) = RestqaProperties(sender = sender, receiver = receiver)

    /**
     * Verifies that the AMQP declarables bean creates quorum queues for each
     * configured sender and receiver flow. Ensures the correct queue type argument
     * is set so RabbitMQ provisions durable, replicated queues. Also validates that
     * exchanges and bindings are declared when an exchange is specified in the
     * sender configuration, with the correct routing key linking queue to exchange.
     */
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

    /**
     * Verifies that when a sender specifies an exchange but omits the routing key,
     * the binding uses the queue name as the default routing key. This ensures
     * messages are routed correctly even without explicit routing key configuration.
     */
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

    /**
     * Verifies that when the same queue name is referenced by both a sender and a
     * receiver flow, the queue is declared only once in the resulting declarables.
     * This prevents duplicate queue declarations that could cause broker errors or
     * conflicting configuration.
     */
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

    /**
     * Verifies that when a sender does not specify an exchange, no exchange or
     * binding declarables are produced. The queue is still declared as a quorum
     * queue. This supports the simple case where messages are sent directly to
     * the default exchange using the queue name as routing key.
     */
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

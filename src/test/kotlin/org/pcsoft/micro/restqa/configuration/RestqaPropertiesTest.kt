package org.pcsoft.micro.restqa.configuration

import org.junit.jupiter.api.Test
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class RestqaPropertiesTest {

    @Test
    fun `defaults to AMQP with empty flows and no max payload size`() {
        val props = RestqaProperties()

        assertEquals(QueueType.AMQP, props.type)
        assertTrue(props.sender.isEmpty())
        assertTrue(props.receiver.isEmpty())
        assertNull(props.maxPayloadSize)
    }

    @Test
    fun `queue endpoint defaults leave AMQP specifics unset`() {
        val endpoint = QueueEndpointProperties(name = "orders.queue")

        assertNull(endpoint.exchange)
        assertNull(endpoint.routingKey)
        assertTrue(endpoint.properties.isEmpty())
    }

    @Test
    fun `receiver retry defaults to 3 retries with 5s backoff`() {
        val retry = RetryProperties()

        assertEquals(3, retry.maxRetries)
        assertEquals(Duration.ofSeconds(5), retry.backoffPeriod)
    }

    @Test
    fun `receiver defaults have no time-to-live`() {
        val receiver = ReceiverProperties(
            rest = ReceiverRestProperties(url = "https://example.com"),
            queue = QueueEndpointProperties(name = "test.queue"),
        )

        assertNull(receiver.timeToLive)
        assertEquals(3, receiver.retry.maxRetries)
        assertEquals(Duration.ofSeconds(5), receiver.retry.backoffPeriod)
    }
}

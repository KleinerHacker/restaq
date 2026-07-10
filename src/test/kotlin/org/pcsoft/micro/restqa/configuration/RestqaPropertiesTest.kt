package org.pcsoft.micro.restqa.configuration

import org.junit.jupiter.api.Test
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Verifies the default values and structure of [RestqaProperties] and its nested
 * configuration classes. Ensures that sensible defaults are applied when the user
 * does not explicitly configure queue type, retry behaviour, payload limits, or
 * AMQP-specific properties like exchange and routing key.
 */
class RestqaPropertiesTest {

    /**
     * Verifies that a freshly constructed [RestqaProperties] instance defaults to
     * AMQP queue type, has no configured sender or receiver flows, and does not
     * impose a maximum payload size limit. These are the safe zero-configuration
     * defaults the application starts with.
     */
    @Test
    fun `defaults to AMQP with empty flows and no max payload size`() {
        val props = RestqaProperties()

        assertEquals(QueueType.AMQP, props.type)
        assertTrue(props.sender.isEmpty())
        assertTrue(props.receiver.isEmpty())
        assertNull(props.maxPayloadSize)
    }

    /**
     * Verifies that a [QueueEndpointProperties] instance with only a queue name
     * leaves the AMQP-specific exchange and routing key fields unset (null).
     * Also confirms that the additional properties map starts empty. This ensures
     * the minimal configuration path works without requiring AMQP-specific details.
     */
    @Test
    fun `queue endpoint defaults leave AMQP specifics unset`() {
        val endpoint = QueueEndpointProperties(name = "orders.queue")

        assertNull(endpoint.exchange)
        assertNull(endpoint.routingKey)
        assertTrue(endpoint.properties.isEmpty())
    }

    /**
     * Verifies that [RetryProperties] defaults to 3 maximum retries with a 5-second
     * backoff period between attempts. These defaults provide a reasonable retry
     * strategy for transient downstream failures without overwhelming the target.
     */
    @Test
    fun `receiver retry defaults to 3 retries with 5s backoff`() {
        val retry = RetryProperties()

        assertEquals(3, retry.maxRetries)
        assertEquals(Duration.ofSeconds(5), retry.backoffPeriod)
    }

    /**
     * Verifies that a [ReceiverProperties] instance defaults to no time-to-live
     * constraint and inherits the standard retry defaults (3 retries, 5s backoff).
     * This ensures that messages are not silently expired unless TTL is explicitly
     * configured by the user.
     */
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

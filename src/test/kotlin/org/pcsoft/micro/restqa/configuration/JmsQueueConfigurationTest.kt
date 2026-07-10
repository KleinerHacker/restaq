package org.pcsoft.micro.restqa.configuration

import jakarta.jms.ConnectionFactory
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.kotlin.mock
import kotlin.test.assertFalse

/**
 * Verifies that [JmsQueueConfiguration] correctly configures the JMS infrastructure.
 * Tests cover JmsTemplate domain settings (point-to-point vs pub/sub) and the
 * destination validation logic that rejects blank or empty queue names at startup.
 */
class JmsQueueConfigurationTest {

    private fun invokeValidateDestinations(config: JmsQueueConfiguration) {
        val method = JmsQueueConfiguration::class.java.getDeclaredMethod("validateDestinations")
        method.isAccessible = true
        try {
            method.invoke(config)
        } catch (e: java.lang.reflect.InvocationTargetException) {
            throw e.targetException
        }
    }

    /**
     * Verifies that the JmsTemplate produced by [JmsQueueConfiguration] operates
     * in the point-to-point (queue) domain rather than pub/sub (topic). This ensures
     * messages are delivered to exactly one consumer, matching RESTAQ's queue-based
     * messaging semantics.
     */
    @Test
    fun `jmsTemplate operates in queue domain`() {
        val template = JmsQueueConfiguration(RestqaProperties()).jmsTemplate(mock<ConnectionFactory>())

        assertFalse(template.isPubSubDomain, "JMS template must use the queue (point-to-point) domain")
    }

    /**
     * Verifies that destination validation passes without error when all configured
     * sender and receiver flows have valid (non-blank) queue names. This is the
     * happy-path scenario confirming the validator accepts well-formed configuration.
     */
    @Test
    fun `validateDestinations succeeds with valid sender and receiver destinations`() {
        val props = RestqaProperties(
            sender = mapOf(
                "orders" to SenderProperties(
                    rest = SenderRestProperties(path = "/api/orders"),
                    queue = QueueEndpointProperties(name = "orders.queue"),
                ),
            ),
            receiver = mapOf(
                "notifications" to ReceiverProperties(
                    rest = ReceiverRestProperties(url = "http://localhost/notify"),
                    queue = QueueEndpointProperties(name = "notifications.queue"),
                ),
            ),
        )
        val config = JmsQueueConfiguration(props)

        // Should not throw.
        invokeValidateDestinations(config)
    }

    /**
     * Verifies that destination validation succeeds when no senders or receivers
     * are configured at all. An empty configuration is valid because there are
     * simply no destinations to validate.
     */
    @Test
    fun `validateDestinations succeeds with empty configuration`() {
        val config = JmsQueueConfiguration(RestqaProperties())

        // No destinations → nothing to validate → should not throw.
        invokeValidateDestinations(config)
    }

    /**
     * Verifies that destination validation throws an [IllegalArgumentException]
     * when a sender's queue name consists only of whitespace. Blank destination
     * names are invalid because the JMS broker cannot route messages to them.
     */
    @Test
    fun `validateDestinations throws when destination name is blank`() {
        val props = RestqaProperties(
            sender = mapOf(
                "broken" to SenderProperties(
                    rest = SenderRestProperties(path = "/api/broken"),
                    queue = QueueEndpointProperties(name = "   "),
                ),
            ),
        )
        val config = JmsQueueConfiguration(props)

        assertThrows<IllegalArgumentException> {
            invokeValidateDestinations(config)
        }
    }

    /**
     * Verifies that destination validation throws an [IllegalArgumentException]
     * when a receiver's queue name is an empty string. Empty destination names
     * are invalid because the JMS broker cannot create or resolve them.
     */
    @Test
    fun `validateDestinations throws when destination name is empty`() {
        val props = RestqaProperties(
            receiver = mapOf(
                "empty" to ReceiverProperties(
                    rest = ReceiverRestProperties(url = "http://localhost/empty"),
                    queue = QueueEndpointProperties(name = ""),
                ),
            ),
        )
        val config = JmsQueueConfiguration(props)

        assertThrows<IllegalArgumentException> {
            invokeValidateDestinations(config)
        }
    }
}

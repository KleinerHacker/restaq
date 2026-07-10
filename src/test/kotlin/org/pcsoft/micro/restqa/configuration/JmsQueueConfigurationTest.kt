package org.pcsoft.micro.restqa.configuration

import jakarta.jms.ConnectionFactory
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.kotlin.mock
import kotlin.test.assertFalse

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

    @Test
    fun `jmsTemplate operates in queue domain`() {
        val template = JmsQueueConfiguration(RestqaProperties()).jmsTemplate(mock<ConnectionFactory>())

        assertFalse(template.isPubSubDomain, "JMS template must use the queue (point-to-point) domain")
    }

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

    @Test
    fun `validateDestinations succeeds with empty configuration`() {
        val config = JmsQueueConfiguration(RestqaProperties())

        // No destinations → nothing to validate → should not throw.
        invokeValidateDestinations(config)
    }

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

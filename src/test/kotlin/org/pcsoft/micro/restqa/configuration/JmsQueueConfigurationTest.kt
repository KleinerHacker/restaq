package org.pcsoft.micro.restqa.configuration

import jakarta.jms.ConnectionFactory
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import kotlin.test.assertFalse

class JmsQueueConfigurationTest {

    @Test
    fun `jmsTemplate operates in queue domain`() {
        val template = JmsQueueConfiguration(RestqaProperties()).jmsTemplate(mock<ConnectionFactory>())

        assertFalse(template.isPubSubDomain, "JMS template must use the queue (point-to-point) domain")
    }
}

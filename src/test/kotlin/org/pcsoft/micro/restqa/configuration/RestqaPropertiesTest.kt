package org.pcsoft.micro.restqa.configuration

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class RestqaPropertiesTest {

    @Test
    fun `defaults to AMQP with empty flows`() {
        val props = RestqaProperties()

        assertEquals(QueueType.AMQP, props.queue.type)
        assertTrue(props.sender.isEmpty())
        assertTrue(props.receiver.isEmpty())
    }

    @Test
    fun `queue endpoint defaults leave AMQP specifics unset`() {
        val endpoint = QueueEndpointProperties(name = "orders.queue")

        assertNull(endpoint.exchange)
        assertNull(endpoint.routingKey)
        assertTrue(endpoint.properties.isEmpty())
    }
}

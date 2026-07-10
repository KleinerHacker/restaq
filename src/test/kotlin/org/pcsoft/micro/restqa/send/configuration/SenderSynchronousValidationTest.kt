package org.pcsoft.micro.restqa.send.configuration

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.kotlin.mock
import org.pcsoft.micro.restqa.configuration.QueueEndpointProperties
import org.pcsoft.micro.restqa.configuration.ReceiverProperties
import org.pcsoft.micro.restqa.configuration.ReceiverRestProperties
import org.pcsoft.micro.restqa.configuration.RestqaProperties
import org.pcsoft.micro.restqa.configuration.SenderProperties
import org.pcsoft.micro.restqa.configuration.SenderRestProperties
import org.pcsoft.micro.restqa.configuration.SenderSynchronousProperties
import org.pcsoft.micro.restqa.internal.SynchronousResponseRegistry
import org.pcsoft.micro.restqa.send.port.MessageQueueClient
import kotlin.test.assertTrue

/**
 * Tests for the startup validation of synchronous configuration.
 */
class SenderSynchronousValidationTest {

    private fun invokeInit(config: SenderEndpointConfiguration) {
        val method = SenderEndpointConfiguration::class.java.getDeclaredMethod("init")
        method.isAccessible = true
        try {
            method.invoke(config)
        } catch (e: java.lang.reflect.InvocationTargetException) {
            throw e.targetException
        }
    }

    @Test
    fun `init succeeds when receiver-ref points to existing receiver without URL`() {
        val props = RestqaProperties(
            sender = mapOf(
                "orders" to SenderProperties(
                    rest = SenderRestProperties(path = "/api/orders"),
                    queue = QueueEndpointProperties(name = "orders.queue"),
                    synchronous = SenderSynchronousProperties(receiverRef = "order-processor"),
                ),
            ),
            receiver = mapOf(
                "order-processor" to ReceiverProperties(
                    rest = ReceiverRestProperties(url = null),
                    queue = QueueEndpointProperties(name = "orders.queue"),
                ),
            ),
        )
        val config = SenderEndpointConfiguration(props, mock<MessageQueueClient>(), SynchronousResponseRegistry())

        // Should not throw.
        invokeInit(config)
    }

    @Test
    fun `init throws when receiver-ref points to non-existing receiver`() {
        val props = RestqaProperties(
            sender = mapOf(
                "orders" to SenderProperties(
                    rest = SenderRestProperties(path = "/api/orders"),
                    queue = QueueEndpointProperties(name = "orders.queue"),
                    synchronous = SenderSynchronousProperties(receiverRef = "non-existing"),
                ),
            ),
            receiver = emptyMap(),
        )
        val config = SenderEndpointConfiguration(props, mock<MessageQueueClient>(), SynchronousResponseRegistry())

        val ex = assertThrows<IllegalArgumentException> { invokeInit(config) }
        assertTrue(ex.message!!.contains("non-existing"))
    }

    @Test
    fun `init throws when receiver-ref points to receiver WITH URL`() {
        val props = RestqaProperties(
            sender = mapOf(
                "orders" to SenderProperties(
                    rest = SenderRestProperties(path = "/api/orders"),
                    queue = QueueEndpointProperties(name = "orders.queue"),
                    synchronous = SenderSynchronousProperties(receiverRef = "order-processor"),
                ),
            ),
            receiver = mapOf(
                "order-processor" to ReceiverProperties(
                    rest = ReceiverRestProperties(url = "http://localhost/process"),
                    queue = QueueEndpointProperties(name = "orders.queue"),
                ),
            ),
        )
        val config = SenderEndpointConfiguration(props, mock<MessageQueueClient>(), SynchronousResponseRegistry())

        val ex = assertThrows<IllegalArgumentException> { invokeInit(config) }
        assertTrue(ex.message!!.contains("rest.url"))
    }

    @Test
    fun `init throws when receiver without URL is not referenced by any sender`() {
        val props = RestqaProperties(
            sender = mapOf(
                "orders" to SenderProperties(
                    rest = SenderRestProperties(path = "/api/orders"),
                    queue = QueueEndpointProperties(name = "orders.queue"),
                    // No synchronous config — does not reference the URL-less receiver.
                ),
            ),
            receiver = mapOf(
                "orphan" to ReceiverProperties(
                    rest = ReceiverRestProperties(url = null),
                    queue = QueueEndpointProperties(name = "orphan.queue"),
                ),
            ),
        )
        val config = SenderEndpointConfiguration(props, mock<MessageQueueClient>(), SynchronousResponseRegistry())

        val ex = assertThrows<IllegalArgumentException> { invokeInit(config) }
        assertTrue(ex.message!!.contains("orphan"))
    }

    @Test
    fun `init throws when receiver with URL is used as sync reference`() {
        // This is covered by rule 2 (receiver-ref receiver must NOT have URL),
        // tested from the sender's perspective. This test approaches it from the
        // receiver's perspective via rule 4.
        val props = RestqaProperties(
            sender = mapOf(
                "orders" to SenderProperties(
                    rest = SenderRestProperties(path = "/api/orders"),
                    queue = QueueEndpointProperties(name = "orders.queue"),
                    synchronous = SenderSynchronousProperties(receiverRef = "with-url"),
                ),
            ),
            receiver = mapOf(
                "with-url" to ReceiverProperties(
                    rest = ReceiverRestProperties(url = "http://target/endpoint"),
                    queue = QueueEndpointProperties(name = "orders.queue"),
                ),
            ),
        )
        val config = SenderEndpointConfiguration(props, mock<MessageQueueClient>(), SynchronousResponseRegistry())

        val ex = assertThrows<IllegalArgumentException> { invokeInit(config) }
        assertTrue(ex.message!!.contains("rest.url"))
    }

    @Test
    fun `init succeeds with mixed sync and async configuration`() {
        val props = RestqaProperties(
            sender = mapOf(
                "sync-orders" to SenderProperties(
                    rest = SenderRestProperties(path = "/api/sync-orders"),
                    queue = QueueEndpointProperties(name = "orders.queue"),
                    synchronous = SenderSynchronousProperties(receiverRef = "order-processor"),
                ),
                "async-events" to SenderProperties(
                    rest = SenderRestProperties(path = "/api/events"),
                    queue = QueueEndpointProperties(name = "events.queue"),
                ),
            ),
            receiver = mapOf(
                "order-processor" to ReceiverProperties(
                    rest = ReceiverRestProperties(url = null),
                    queue = QueueEndpointProperties(name = "orders.queue"),
                ),
                "notifications" to ReceiverProperties(
                    rest = ReceiverRestProperties(url = "http://downstream/notify"),
                    queue = QueueEndpointProperties(name = "notifications.queue"),
                ),
            ),
        )
        val config = SenderEndpointConfiguration(props, mock<MessageQueueClient>(), SynchronousResponseRegistry())

        // Should not throw.
        invokeInit(config)
    }

    @Test
    fun `init succeeds when no sender has synchronous config`() {
        val props = RestqaProperties(
            sender = mapOf(
                "orders" to SenderProperties(
                    rest = SenderRestProperties(path = "/api/orders"),
                    queue = QueueEndpointProperties(name = "orders.queue"),
                ),
            ),
            receiver = mapOf(
                "notifications" to ReceiverProperties(
                    rest = ReceiverRestProperties(url = "http://downstream/notify"),
                    queue = QueueEndpointProperties(name = "notifications.queue"),
                ),
            ),
        )
        val config = SenderEndpointConfiguration(props, mock<MessageQueueClient>(), SynchronousResponseRegistry())

        // Should not throw.
        invokeInit(config)
    }
}

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
 * Tests for the startup validation of synchronous sender-receiver configuration.
 *
 * Verifies that [SenderEndpointConfiguration.init] enforces the following invariants:
 * - A sender's `synchronous.receiver-ref` must reference an existing receiver.
 * - The referenced receiver must NOT have a `rest.url` configured (it is sync-only).
 * - A receiver without a `rest.url` must be referenced by at least one sender's `synchronous.receiver-ref`.
 * - Mixed (sync + async) and purely async configurations pass validation.
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

    /**
     * Verifies that initialization succeeds when a sender's `synchronous.receiver-ref`
     * references an existing receiver that has no `rest.url` configured, which is the
     * valid configuration for synchronous request-reply mode.
     */
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

    /**
     * Verifies that initialization fails with an [IllegalArgumentException] when a sender's
     * `synchronous.receiver-ref` references a receiver name that does not exist in the
     * configuration, preventing misconfigurations from going undetected at startup.
     */
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

    /**
     * Verifies that initialization fails with an [IllegalArgumentException] when a sender's
     * `synchronous.receiver-ref` references a receiver that has a `rest.url` configured.
     * A synchronous receiver must be URL-less because it replies through the queue rather
     * than forwarding to an HTTP target.
     */
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

    /**
     * Verifies that initialization fails with an [IllegalArgumentException] when a receiver
     * is configured without a `rest.url` but is not referenced by any sender's
     * `synchronous.receiver-ref`. Such a receiver would never receive messages since it has
     * no HTTP callback target and no sender is waiting for its reply.
     */
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

    /**
     * Verifies that initialization fails with an [IllegalArgumentException] when a sender's
     * `synchronous.receiver-ref` points to a receiver that has a `rest.url` set. This tests
     * the constraint from the receiver's perspective — a receiver with a URL is intended for
     * asynchronous callback delivery and cannot be used as a synchronous reply source.
     */
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

    /**
     * Verifies that initialization succeeds when the configuration contains both synchronous
     * senders (with `receiver-ref`) and asynchronous senders (without `receiver-ref`),
     * alongside receivers of both types — URL-less (sync) and URL-bearing (async callback).
     */
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

    /**
     * Verifies that initialization succeeds when no sender has synchronous configuration at all,
     * meaning all receivers must have a `rest.url` (async callback mode). This is the simplest
     * valid configuration with no request-reply semantics.
     */
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

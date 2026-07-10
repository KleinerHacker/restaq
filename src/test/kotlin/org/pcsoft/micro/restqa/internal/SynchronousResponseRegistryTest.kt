package org.pcsoft.micro.restqa.internal

import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.Executors
import kotlin.test.*

class SynchronousResponseRegistryTest {

    @Test
    fun `register creates a unique correlation ID and future`() {
        val registry = SynchronousResponseRegistry()

        val (id1, future1) = registry.register()
        val (id2, future2) = registry.register()

        assertNotNull(id1)
        assertNotNull(id2)
        assertTrue(id1 != id2)
        assertFalse(future1.isDone)
        assertFalse(future2.isDone)
        assertEquals(2, registry.pendingCount())
    }

    @Test
    fun `complete resolves a pending future`() {
        val registry = SynchronousResponseRegistry()
        val (correlationId, future) = registry.register()
        val response = SynchronousResponse(200, "OK".toByteArray())

        val result = registry.complete(correlationId, response)

        assertTrue(result)
        assertTrue(future.isDone)
        assertEquals(200, future.get().statusCode)
        assertEquals("OK", String(future.get().body))
        assertEquals(0, registry.pendingCount())
    }

    @Test
    fun `complete returns false for unknown correlation ID`() {
        val registry = SynchronousResponseRegistry()

        val result = registry.complete("unknown-id", SynchronousResponse(200, ByteArray(0)))

        assertFalse(result)
    }

    @Test
    fun `await returns response when completed before timeout`() {
        val registry = SynchronousResponseRegistry()
        val (correlationId, future) = registry.register()
        val response = SynchronousResponse(201, "Created".toByteArray(), mapOf("Location" to "/items/1"))

        // Complete asynchronously.
        Executors.newSingleThreadExecutor().submit {
            Thread.sleep(50)
            registry.complete(correlationId, response)
        }

        val result = registry.await(correlationId, future, Duration.ofSeconds(5))

        assertNotNull(result)
        assertEquals(201, result.statusCode)
        assertEquals("Created", String(result.body))
        assertEquals("/items/1", result.headers["Location"])
        assertEquals(0, registry.pendingCount())
    }

    @Test
    fun `await returns null on timeout`() {
        val registry = SynchronousResponseRegistry()
        val (correlationId, future) = registry.register()

        val result = registry.await(correlationId, future, Duration.ofMillis(50))

        assertNull(result)
        assertEquals(0, registry.pendingCount())
    }

    @Test
    fun `await cleans up pending entry even on timeout`() {
        val registry = SynchronousResponseRegistry()
        val (correlationId, future) = registry.register()
        assertEquals(1, registry.pendingCount())

        registry.await(correlationId, future, Duration.ofMillis(10))

        assertEquals(0, registry.pendingCount())
    }

    @Test
    fun `complete after timeout returns false (entry already removed)`() {
        val registry = SynchronousResponseRegistry()
        val (correlationId, future) = registry.register()

        registry.await(correlationId, future, Duration.ofMillis(10))

        // Try to complete after timeout — should fail.
        val result = registry.complete(correlationId, SynchronousResponse(200, ByteArray(0)))
        assertFalse(result)
    }

    @Test
    fun `SynchronousResponse equals and hashCode`() {
        val r1 = SynchronousResponse(200, "body".toByteArray(), mapOf("A" to "1"))
        val r2 = SynchronousResponse(200, "body".toByteArray(), mapOf("A" to "1"))
        val r3 = SynchronousResponse(201, "body".toByteArray(), mapOf("A" to "1"))

        assertEquals(r1, r2)
        assertEquals(r1.hashCode(), r2.hashCode())
        assertFalse(r1 == r3)
    }
}

package org.pcsoft.micro.restqa.internal

import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.Executors
import kotlin.test.*

/**
 * Verifies the behaviour of [SynchronousResponseRegistry], which manages the
 * correlation between outbound synchronous requests and their asynchronous
 * responses. Tests cover registration of pending futures, completion with
 * response data, timeout handling, cleanup of expired entries, and value
 * equality of [SynchronousResponse].
 */
class SynchronousResponseRegistryTest {

    /**
     * Verifies that each call to [SynchronousResponseRegistry.register] produces
     * a unique correlation ID and a corresponding incomplete future. Confirms
     * that the pending count reflects the number of outstanding registrations,
     * ensuring no ID collisions occur.
     */
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

    /**
     * Verifies that calling [SynchronousResponseRegistry.complete] with a valid
     * correlation ID resolves the associated future with the provided response.
     * The future becomes done, the response data is accessible, and the pending
     * count drops to zero as the entry is removed from the registry.
     */
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

    /**
     * Verifies that attempting to complete with an unknown correlation ID returns
     * false without side effects. This handles the case where a response arrives
     * for a request that was never registered or has already been cleaned up.
     */
    @Test
    fun `complete returns false for unknown correlation ID`() {
        val registry = SynchronousResponseRegistry()

        val result = registry.complete("unknown-id", SynchronousResponse(200, ByteArray(0)))

        assertFalse(result)
    }

    /**
     * Verifies that [SynchronousResponseRegistry.await] returns the response when
     * it is completed asynchronously before the timeout expires. Simulates a
     * delayed completion on a separate thread and confirms the response data
     * (status code, body, headers) is correctly propagated to the caller.
     */
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

    /**
     * Verifies that [SynchronousResponseRegistry.await] returns null when the
     * timeout elapses before a response is received. This simulates a downstream
     * service that never responds within the allowed time window.
     */
    @Test
    fun `await returns null on timeout`() {
        val registry = SynchronousResponseRegistry()
        val (correlationId, future) = registry.register()

        val result = registry.await(correlationId, future, Duration.ofMillis(50))

        assertNull(result)
        assertEquals(0, registry.pendingCount())
    }

    /**
     * Verifies that the registry cleans up the pending entry even when a timeout
     * occurs, preventing memory leaks. After await times out, the pending count
     * must be zero regardless of whether the response ever arrives.
     */
    @Test
    fun `await cleans up pending entry even on timeout`() {
        val registry = SynchronousResponseRegistry()
        val (correlationId, future) = registry.register()
        assertEquals(1, registry.pendingCount())

        registry.await(correlationId, future, Duration.ofMillis(10))

        assertEquals(0, registry.pendingCount())
    }

    /**
     * Verifies that completing a correlation ID after the await has already timed
     * out returns false, because the entry was already removed during timeout
     * cleanup. This ensures late responses do not corrupt registry state.
     */
    @Test
    fun `complete after timeout returns false (entry already removed)`() {
        val registry = SynchronousResponseRegistry()
        val (correlationId, future) = registry.register()

        registry.await(correlationId, future, Duration.ofMillis(10))

        // Try to complete after timeout — should fail.
        val result = registry.complete(correlationId, SynchronousResponse(200, ByteArray(0)))
        assertFalse(result)
    }

    /**
     * Verifies the structural equality and hashCode contract of [SynchronousResponse].
     * Two instances with identical status code, body content, and headers must be
     * equal and produce the same hash code, while a differing status code breaks equality.
     */
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

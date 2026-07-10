package org.pcsoft.micro.restqa.internal

import org.pcsoft.micro.restqa.internal.utils.logger
import org.springframework.stereotype.Component
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

/**
 * The response payload returned by the downstream service through the receiver,
 * correlated back to the waiting sender.
 */
data class SynchronousResponse(
    /** HTTP status code returned by the downstream target. */
    val statusCode: Int,
    /** Response body from the downstream target (may be empty). */
    val body: ByteArray,
    /** Response headers from the downstream target. */
    val headers: Map<String, String> = emptyMap(),
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is SynchronousResponse) return false
        return statusCode == other.statusCode && body.contentEquals(other.body) && headers == other.headers
    }

    override fun hashCode(): Int {
        var result = statusCode
        result = 31 * result + body.contentHashCode()
        result = 31 * result + headers.hashCode()
        return result
    }
}

/**
 * In-memory registry that correlates synchronous request/response pairs between
 * sender and receiver flows.
 *
 * The sender registers a pending request with a unique correlation ID and waits
 * on the returned [CompletableFuture]. The receiver completes the future once the
 * downstream HTTP response is available. This avoids using queue-level reply
 * mechanisms (e.g. `sendAndReceive`) and keeps the correlation entirely within the
 * RESTAQ service instance.
 *
 * **Important:** This mechanism requires the sender and receiver to run in the same
 * JVM instance. It is not suitable for distributed deployments where sender and
 * receiver are separate processes.
 */
@Component
class SynchronousResponseRegistry {

    companion object {
        private val log = logger()

        /** Message property/header name carrying the correlation ID. */
        const val HEADER_CORRELATION_ID = "X-Restqa-Correlation-Id"
    }

    private val pending = ConcurrentHashMap<String, CompletableFuture<SynchronousResponse>>()

    /**
     * Registers a new pending synchronous request.
     *
     * @return a pair of (correlationId, future) — the caller must include the correlation ID
     *         in the outgoing message and then await the future.
     */
    fun register(): Pair<String, CompletableFuture<SynchronousResponse>> {
        val correlationId = UUID.randomUUID().toString()
        val future = CompletableFuture<SynchronousResponse>()
        pending[correlationId] = future
        log.debug("Registered pending synchronous request: {}", correlationId)
        return correlationId to future
    }

    /**
     * Waits for a response with the given timeout.
     *
     * @param correlationId the correlation ID returned by [register]
     * @param future the future returned by [register]
     * @param timeout maximum time to wait
     * @return the response, or `null` if the timeout expired
     */
    fun await(correlationId: String, future: CompletableFuture<SynchronousResponse>, timeout: Duration): SynchronousResponse? =
        try {
            val response = future.get(timeout.toMillis(), TimeUnit.MILLISECONDS)
            log.debug("Received synchronous response for correlation {}", correlationId)
            response
        } catch (_: TimeoutException) {
            log.warn("Synchronous request timed out after {} for correlation {}", timeout, correlationId)
            null
        } finally {
            pending.remove(correlationId)
        }

    /**
     * Completes a pending request with the downstream response.
     *
     * Called by the receiver after successfully delivering the message and obtaining
     * the downstream HTTP response.
     *
     * @param correlationId the correlation ID extracted from the message properties
     * @param response the downstream response
     * @return `true` if a pending request was found and completed, `false` otherwise
     */
    fun complete(correlationId: String, response: SynchronousResponse): Boolean {
        val future = pending.remove(correlationId)
        if (future == null) {
            log.debug("No pending request for correlation {} (expired or not synchronous)", correlationId)
            return false
        }
        future.complete(response)
        log.debug("Completed synchronous response for correlation {}", correlationId)
        return true
    }

    /**
     * Returns the number of currently pending requests. Primarily for diagnostics/testing.
     */
    fun pendingCount(): Int = pending.size

    /**
     * Cancels a pending request, removing it from the registry without completing it.
     *
     * @param correlationId the correlation ID to cancel
     */
    fun cancel(correlationId: String) {
        pending.remove(correlationId)?.cancel(false)
        log.debug("Cancelled pending synchronous request: {}", correlationId)
    }
}

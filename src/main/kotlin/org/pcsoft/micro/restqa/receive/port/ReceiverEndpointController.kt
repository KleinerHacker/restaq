package org.pcsoft.micro.restqa.receive.port

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import org.pcsoft.micro.restqa.configuration.ReceiverProperties
import org.pcsoft.micro.restqa.internal.SynchronousResponse
import org.pcsoft.micro.restqa.internal.SynchronousResponseRegistry
import org.pcsoft.micro.restqa.internal.utils.logger
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.WebClientResponseException

/**
 * Forwards a message consumed from a queue to the external REST endpoint of a single
 * configured receiver, or — in synchronous mode — acknowledges the message back to a
 * waiting sender without making an HTTP call.
 *
 * **Asynchronous mode** (`rest.url` is set): POSTs the message to the target URL,
 * injects `X-Retry-Count`, and supports retry with backoff.
 *
 * **Synchronous mode** (`rest.url` is null, correlation ID present): Resolves the
 * pending future in [SynchronousResponseRegistry] with the message payload. No HTTP
 * call is made. No retry — on failure the message goes directly to DLQ.
 */
class ReceiverEndpointController(
    private val properties: ReceiverProperties,
    private val webClient: WebClient,
    private val synchronousRegistry: SynchronousResponseRegistry? = null,
) {

    companion object {
        private val log = logger()

        /** Header injected into outgoing requests carrying the zero-based delivery attempt. */
        const val HEADER_RETRY_COUNT = "X-Retry-Count"
    }

    /**
     * Forwards the [payload] with [headers] to the configured REST URL, or resolves
     * a pending synchronous request if operating in synchronous mode.
     *
     * @param payload   the message body
     * @param headers   message properties mapped to HTTP headers
     * @param retryCount zero-based delivery attempt counter (only relevant for async mode)
     * @return [Either.Right] on success, [Either.Left] with the exception on failure
     */
    fun forward(payload: ByteArray, headers: Map<String, String>, retryCount: Int = 0): Either<Exception, Unit> {
        val correlationId = headers[SynchronousResponseRegistry.HEADER_CORRELATION_ID]
        val isSynchronous = correlationId != null && synchronousRegistry != null

        return if (properties.rest.url == null) {
            // Synchronous receiver: resolve the pending future with the message payload.
            handleSynchronousReceiver(payload, headers, correlationId, isSynchronous)
        } else {
            // Asynchronous receiver: POST to downstream target.
            handleAsynchronousReceiver(payload, headers, retryCount, correlationId, isSynchronous)
        }
    }

    private fun handleSynchronousReceiver(
        payload: ByteArray,
        headers: Map<String, String>,
        correlationId: String?,
        isSynchronous: Boolean,
    ): Either<Exception, Unit> {
        if (!isSynchronous || correlationId == null) {
            log.warn("Synchronous receiver consumed message without correlation ID — discarding")
            return RuntimeException("No correlation ID in message for synchronous receiver").left()
        }

        log.debug("Synchronous receiver resolving correlation {}", correlationId)

        val contentType = headers["Content-Type"]
        val responseHeaders = if (contentType != null) mapOf("Content-Type" to contentType) else emptyMap()

        val response = SynchronousResponse(
            statusCode = 200,
            body = payload,
            headers = responseHeaders,
        )

        val completed = synchronousRegistry!!.complete(correlationId, response)
        return if (completed) {
            log.info("Synchronous response resolved for correlation {}", correlationId)
            Unit.right()
        } else {
            log.warn("No pending request for correlation {} (sender may have timed out)", correlationId)
            RuntimeException("No pending sender for correlation $correlationId").left()
        }
    }

    private fun handleAsynchronousReceiver(
        payload: ByteArray,
        headers: Map<String, String>,
        retryCount: Int,
        correlationId: String?,
        isSynchronous: Boolean,
    ): Either<Exception, Unit> {
        log.debug("Forwarding {} bytes to '{}' (attempt {})", payload.size, properties.rest.url, retryCount)

        return try {
            val responseEntity = webClient.post()
                .uri(properties.rest.url!!)
                .headers { httpHeaders ->
                    headers.forEach { (name, value) ->
                        // Don't propagate the internal correlation header to the downstream target.
                        if (name != SynchronousResponseRegistry.HEADER_CORRELATION_ID) {
                            httpHeaders.add(name, value)
                        }
                    }
                    // Only inject X-Retry-Count for non-synchronous messages.
                    if (!isSynchronous) {
                        httpHeaders.set(HEADER_RETRY_COUNT, retryCount.toString())
                    }
                }
                .bodyValue(payload)
                .retrieve()
                .toEntity(ByteArray::class.java)
                .block()

            log.info("Callback delivered successfully to '{}'", properties.rest.url)

            // Feed response back to the synchronous registry if applicable.
            if (isSynchronous && responseEntity != null && correlationId != null) {
                val responseHeaders = buildMap<String, String> {
                    responseEntity.headers.forEach { name, values ->
                        put(name, values.joinToString(", "))
                    }
                }
                val syncResponse = SynchronousResponse(
                    statusCode = responseEntity.statusCode.value(),
                    body = responseEntity.body ?: ByteArray(0),
                    headers = responseHeaders,
                )
                synchronousRegistry!!.complete(correlationId, syncResponse)
            }

            Unit.right()
        } catch (ex: WebClientResponseException) {
            log.debug("HTTP {} from '{}': {}", ex.statusCode, properties.rest.url, ex.message)

            // Even on non-2xx, if synchronous, feed the error response back.
            if (isSynchronous && correlationId != null && synchronousRegistry != null) {
                val responseHeaders = buildMap<String, String> {
                    ex.headers.forEach { name, values ->
                        put(name, values.joinToString(", "))
                    }
                }
                val syncResponse = SynchronousResponse(
                    statusCode = ex.statusCode.value(),
                    body = ex.responseBodyAsByteArray,
                    headers = responseHeaders,
                )
                synchronousRegistry.complete(correlationId, syncResponse)
            }

            ex.left()
        } catch (ex: Exception) {
            log.debug("Connection error to '{}': {}", properties.rest.url, ex.message)
            ex.left()
        }
    }
}

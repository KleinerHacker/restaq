package org.pcsoft.micro.restqa.send.port

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import org.pcsoft.micro.restqa.configuration.SenderProperties
import org.pcsoft.micro.restqa.configuration.SenderSynchronousProperties
import org.pcsoft.micro.restqa.internal.SynchronousResponse
import org.pcsoft.micro.restqa.internal.SynchronousResponseRegistry
import org.pcsoft.micro.restqa.internal.utils.HeaderFilter
import org.pcsoft.micro.restqa.internal.utils.logger
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ProblemDetail
import org.springframework.util.unit.DataSize
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.reactive.function.server.ServerResponse
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import java.net.URI

/**
 * Handles incoming HTTP requests for a single configured sender.
 *
 * One instance is created per `restqa.sender.<key>` entry by
 * [org.pcsoft.micro.restqa.send.configuration.SenderEndpointConfiguration] and bound to
 * the sender's [SenderProperties.rest] path. The sender key is provided via the MDC
 * (set by the configuration), not passed in here.
 *
 * The inbound request body is read and forwarded onto the configured queue
 * ([SenderProperties.queue]) via the active [MessageQueueClient], together with the
 * request's HTTP headers (multi-valued headers are joined with ", "). A request without
 * a body forwards an empty payload.
 *
 * **Asynchronous mode** (default): replies `202 Accepted` immediately after enqueue.
 *
 * **Synchronous mode** (when [SenderProperties.synchronous] is configured): injects a
 * correlation ID into the message headers, waits for the receiver's downstream response
 * via [SynchronousResponseRegistry], and returns it to the caller. If the timeout expires,
 * replies `504 Gateway Timeout`.
 *
 * Error responses use [RFC 9457 Problem Details](https://www.rfc-editor.org/rfc/rfc9457)
 * via Spring's built-in [ProblemDetail]:
 * - **413 Payload Too Large** – when the request body exceeds `restqa.max-payload-size`
 * - **502 Bad Gateway** – when the queue broker is unreachable or rejects the message
 * - **504 Gateway Timeout** – when synchronous mode times out waiting for a response
 */
class SenderEndpointController(
    private val properties: SenderProperties,
    private val queueClient: MessageQueueClient,
    private val maxPayloadSize: DataSize? = null,
    private val synchronousRegistry: SynchronousResponseRegistry? = null,
) {

    companion object {
        private val log = logger()
        private val PROBLEM_JSON = MediaType.valueOf("application/problem+json")
    }

    fun handle(request: ServerRequest): Mono<ServerResponse> {
        log.info("Request received on '{}'", properties.rest.path)
        // Flatten multi-valued HTTP headers into single comma-joined values.
        val httpHeaders = request.headers().asHttpHeaders()
        val headers = HeaderFilter.filter(
            httpHeaders.headerNames().associateWith { name ->
                httpHeaders.getValuesAsList(name).joinToString(", ")
            }
        ).toMutableMap()

        return request.bodyToMono(ByteArray::class.java)
            .defaultIfEmpty(ByteArray(0))
            .flatMap { payload ->
                // Check payload size limit.
                val sizeCheck = checkPayloadSize(payload, request)
                if (sizeCheck is Either.Left) {
                    return@flatMap problemResponse(sizeCheck.value)
                }

                val syncConfig = properties.synchronous
                if (syncConfig != null && synchronousRegistry != null) {
                    handleSynchronous(payload, headers, syncConfig)
                } else {
                    handleAsynchronous(payload, headers)
                }
            }
    }

    private fun handleAsynchronous(payload: ByteArray, headers: Map<String, String>): Mono<ServerResponse> =
        Mono.fromCallable { sendToQueue(payload, headers) }
            .subscribeOn(Schedulers.boundedElastic())
            .flatMap { result ->
                result.fold(
                    ifLeft = { problem -> problemResponse(problem) },
                    ifRight = {
                        log.info("Message placed on queue '{}'", properties.queue.name)
                        ServerResponse.accepted().build()
                    },
                )
            }

    private fun handleSynchronous(
        payload: ByteArray,
        headers: MutableMap<String, String>,
        syncConfig: SenderSynchronousProperties,
    ): Mono<ServerResponse> =
        Mono.fromCallable<Either<ProblemDetail, SynchronousResponse>> {
            // Register pending response and inject correlation ID.
            val (correlationId, future) = synchronousRegistry!!.register()
            headers[SynchronousResponseRegistry.HEADER_CORRELATION_ID] = correlationId

            // Send the message onto the queue.
            val sendResult = sendToQueue(payload, headers)
            if (sendResult is Either.Left) {
                // Clean up the pending registration on send failure.
                synchronousRegistry.cancel(correlationId)
                return@fromCallable sendResult.value.left()
            }

            log.info("Message placed on queue '{}', waiting for synchronous response (correlation={})", properties.queue.name, correlationId)

            // Wait for the receiver to complete the response.
            val response = synchronousRegistry.await(correlationId, future, properties.timeout)
            if (response == null) {
                ProblemDetail.forStatus(HttpStatus.GATEWAY_TIMEOUT).apply {
                    type = URI.create("urn:restqa:error:synchronous-timeout")
                    title = "Gateway Timeout"
                    detail = "Timed out after ${this@SenderEndpointController.properties.timeout} waiting for synchronous response from receiver '${syncConfig.receiverRef}'."
                    instance = URI.create(this@SenderEndpointController.properties.rest.path)
                }.left()
            } else {
                response.right()
            }
        }
            .subscribeOn(Schedulers.boundedElastic())
            .flatMap { result ->
                result.fold(
                    ifLeft = { problem -> problemResponse(problem) },
                    ifRight = { syncResponse ->
                        val statusCode = HttpStatus.valueOf(syncResponse.statusCode)
                        val builder = ServerResponse.status(statusCode)
                        syncResponse.headers["Content-Type"]?.let { builder.contentType(MediaType.parseMediaType(it)) }
                        if (syncResponse.body.isNotEmpty()) {
                            builder.bodyValue(syncResponse.body)
                        } else {
                            builder.build()
                        }
                    },
                )
            }

    private fun sendToQueue(
        payload: ByteArray,
        headers: Map<String, String>,
    ): Either<ProblemDetail, Unit> =
        try {
            queueClient.send(properties.queue, payload, headers)
            Unit.right()
        } catch (ex: Exception) {
            log.error("Failed to publish message to queue '{}': {}", properties.queue.name, ex.message, ex)
            ProblemDetail.forStatus(HttpStatus.BAD_GATEWAY).apply {
                type = URI.create("urn:restqa:error:broker-unavailable")
                title = "Bad Gateway"
                detail = "Failed to deliver message to queue '${this@SenderEndpointController.properties.queue.name}': ${ex.message}"
                instance = URI.create(this@SenderEndpointController.properties.rest.path)
            }.left()
        }

    private fun checkPayloadSize(payload: ByteArray, request: ServerRequest): Either<ProblemDetail, Unit> {
        val limit = maxPayloadSize ?: return Unit.right()
        if (payload.size <= limit.toBytes()) return Unit.right()
        return ProblemDetail.forStatus(HttpStatus.CONTENT_TOO_LARGE).apply {
            type = URI.create("urn:restqa:error:payload-too-large")
            title = "Payload Too Large"
            detail = "Request body of ${payload.size} bytes exceeds the configured limit of $limit."
            instance = URI.create(request.path())
        }.left()
    }

    private fun problemResponse(problem: ProblemDetail): Mono<ServerResponse> =
        ServerResponse.status(problem.status)
            .contentType(PROBLEM_JSON)
            .bodyValue(problem)
}

package org.pcsoft.micro.restqa.send.port

import org.pcsoft.micro.restqa.configuration.SenderProperties
import org.pcsoft.micro.restqa.internal.utils.logger
import org.pcsoft.micro.restqa.send.controller.MessageQueueClient
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.reactive.function.server.ServerResponse
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers

/**
 * Handles incoming HTTP requests for a single configured sender.
 *
 * One instance is created per `restqa.sender.<key>` entry by
 * [org.pcsoft.micro.restqa.send.configuration.SenderEndpointConfiguration] and bound to
 * the sender's [SenderProperties.endpoint] path. The sender key is provided via the MDC
 * (set by the configuration), not passed in here.
 *
 * The inbound request body is read and forwarded onto the configured queue
 * ([SenderProperties.queue]) via the active [MessageQueueClient], together with the
 * request's HTTP headers (multi-valued headers are joined with ", "). A request without
 * a body forwards an empty payload. On success the handler replies `200 OK` (empty body).
 */
class SenderEndpointController(
    private val properties: SenderProperties,
    private val queueClient: MessageQueueClient,
) {

    companion object {
        private val log = logger()
    }

    fun handle(request: ServerRequest): Mono<ServerResponse> {
        log.debug(
            "Received {} {} -> forwarding to queue '{}'",
            request.method(), properties.endpoint, properties.queue.name,
        )
        // Flatten multi-valued HTTP headers into single comma-joined values.
        val httpHeaders = request.headers().asHttpHeaders()
        val headers = httpHeaders.headerNames().associateWith { name ->
            httpHeaders.getValuesAsList(name).joinToString(", ")
        }
        return request.bodyToMono(ByteArray::class.java)
            // No body -> forward an empty payload instead of skipping the send.
            .defaultIfEmpty(ByteArray(0))
            .flatMap { payload ->
                // The queue client is blocking; keep it off the event loop.
                Mono.fromCallable { queueClient.send(properties.queue, payload, headers) }
                    .subscribeOn(Schedulers.boundedElastic())
            }
            .then(ServerResponse.ok().build())
    }
}

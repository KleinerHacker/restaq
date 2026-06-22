package org.pcsoft.micro.restqa.send.port

import org.pcsoft.micro.restqa.configuration.SenderProperties
import org.pcsoft.micro.restqa.internal.utils.logger
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.reactive.function.server.ServerResponse
import reactor.core.publisher.Mono

/**
 * Handles incoming HTTP requests for a single configured sender.
 *
 * One instance is created per `restqa.sender.<key>` entry by
 * [org.pcsoft.micro.restqa.send.configuration.SenderEndpointConfiguration] and bound to
 * the sender's [SenderProperties.endpoint] path. The sender key is provided via the MDC
 * (set by the configuration), not passed in here.
 *
 * For now this is a no-op: it only logs the inbound request and replies with `200 OK`
 * (empty body). Forwarding the request into the configured queue is added later.
 */
class SenderEndpointController(
    private val properties: SenderProperties,
) {

    companion object {
        private val log = logger()
    }

    fun handle(request: ServerRequest): Mono<ServerResponse> {
        log.debug(
            "Received {} {} (queue='{}') - no-op",
            request.method(), properties.endpoint, properties.queue.name,
        )
        return ServerResponse.ok().build()
    }
}

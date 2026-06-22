package org.pcsoft.micro.restqa.send.port

import org.pcsoft.micro.restqa.configuration.SenderProperties
import org.pcsoft.micro.restqa.internal.utils.logger
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.reactive.function.server.ServerResponse
import reactor.core.publisher.Mono

/**
 * Handles incoming HTTP requests for a single configured sender flow.
 *
 * One instance is created per `restqa.sender.<flow>` entry by
 * [org.pcsoft.micro.restqa.send.configuration.SenderEndpointConfiguration] and bound to
 * the flow's [SenderProperties.endpoint] path.
 *
 * For now this is a no-op: it only logs the inbound request and replies with `200 OK`
 * (empty body). Forwarding the request into the configured queue is added later.
 */
class SenderEndpointController(
    private val flow: String,
    private val properties: SenderProperties,
) {

    companion object {
        private val log = logger()
    }

    fun handle(request: ServerRequest): Mono<ServerResponse> {
        log.debug(
            "[{}] Received {} {} (queue='{}') - no-op",
            flow, request.method(), properties.endpoint, properties.queue.name,
        )
        return ServerResponse.ok().build()
    }
}

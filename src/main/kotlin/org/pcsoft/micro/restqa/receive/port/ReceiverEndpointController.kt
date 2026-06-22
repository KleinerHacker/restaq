package org.pcsoft.micro.restqa.receive.port

import org.pcsoft.micro.restqa.configuration.ReceiverProperties
import org.pcsoft.micro.restqa.internal.utils.logger
import org.springframework.web.reactive.function.client.WebClient
import reactor.core.publisher.Mono

/**
 * Forwards a message consumed from a queue to the external REST endpoint of a single
 * configured receiver.
 *
 * One instance is created per `restqa.receiver.<key>` entry by the active queue consumer
 * ([org.pcsoft.micro.restqa.receive.controller.AmqpQueueConsumer] /
 * [org.pcsoft.micro.restqa.receive.controller.JmsQueueConsumer]). The receiver key is
 * provided via the MDC (set by the consumer), not passed in here.
 *
 * The message payload becomes the HTTP request body; the supplied \[headers] (mapped from
 * the message properties) are propagated as HTTP headers (see project docs: HTTP Body ↔
 * Message Payload, HTTP Headers ↔ Message Properties).
 */
class ReceiverEndpointController(
    private val properties: ReceiverProperties,
    private val webClient: WebClient,
) {

    companion object {
        private val log = logger()
    }

    fun forward(payload: ByteArray, headers: Map<String, String>): Mono<Void> {
        log.debug("Forwarding {} bytes to '{}'", payload.size, properties.endpoint)
        return webClient.post()
            .uri(properties.endpoint)
            .headers { httpHeaders -> headers.forEach(httpHeaders::add) }
            .bodyValue(payload)
            .retrieve()
            .toBodilessEntity()
            .then()
    }
}

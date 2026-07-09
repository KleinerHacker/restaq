package org.pcsoft.micro.restqa.receive.port

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import org.pcsoft.micro.restqa.configuration.ReceiverProperties
import org.pcsoft.micro.restqa.internal.utils.logger
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.WebClientResponseException

/**
 * Forwards a message consumed from a queue to the external REST endpoint of a single
 * configured receiver.
 *
 * One instance is created per `restqa.receiver.<key>` entry by the active queue consumer
 * ([AmqpQueueConsumer] / [JmsQueueConsumer]). The receiver key is provided via the MDC
 * (set by the consumer), not passed in here.
 *
 * The message payload becomes the HTTP request body; the supplied [headers] (mapped from
 * the message properties) are propagated as HTTP headers. The `X-Retry-Count` header is
 * injected with the zero-based delivery attempt counter.
 */
class ReceiverEndpointController(
    private val properties: ReceiverProperties,
    private val webClient: WebClient,
) {

    companion object {
        private val log = logger()

        /** Header injected into outgoing requests carrying the zero-based delivery attempt. */
        const val HEADER_RETRY_COUNT = "X-Retry-Count"
    }

    /**
     * Forwards the [payload] with [headers] to the configured REST URL.
     *
     * @param payload   the message body
     * @param headers   message properties mapped to HTTP headers
     * @param retryCount zero-based delivery attempt counter
     * @return [Either.Right] on success (2xx), [Either.Left] with the exception on failure
     */
    fun forward(payload: ByteArray, headers: Map<String, String>, retryCount: Int = 0): Either<Exception, Unit> {
        log.debug("Forwarding {} bytes to '{}' (attempt {})", payload.size, properties.rest.url, retryCount)
        return try {
            webClient.post()
                .uri(properties.rest.url)
                .headers { httpHeaders ->
                    headers.forEach(httpHeaders::add)
                    httpHeaders.set(HEADER_RETRY_COUNT, retryCount.toString())
                }
                .bodyValue(payload)
                .retrieve()
                .toBodilessEntity()
                .block()
            log.info("Callback delivered successfully to '{}'", properties.rest.url)
            Unit.right()
        } catch (ex: WebClientResponseException) {
            log.debug("HTTP {} from '{}': {}", ex.statusCode, properties.rest.url, ex.message)
            ex.left()
        } catch (ex: Exception) {
            log.debug("Connection error to '{}': {}", properties.rest.url, ex.message)
            ex.left()
        }
    }
}

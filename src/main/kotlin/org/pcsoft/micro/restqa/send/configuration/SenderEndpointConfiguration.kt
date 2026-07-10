package org.pcsoft.micro.restqa.send.configuration

import jakarta.annotation.PostConstruct
import org.pcsoft.micro.restqa.configuration.RestqaProperties
import org.pcsoft.micro.restqa.internal.SynchronousResponseRegistry
import org.pcsoft.micro.restqa.internal.utils.logger
import org.pcsoft.micro.restqa.send.port.MessageQueueClient
import org.pcsoft.micro.restqa.send.port.SenderEndpointController
import org.slf4j.MDC
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.web.reactive.function.server.RequestPredicates
import org.springframework.web.reactive.function.server.RouterFunction
import org.springframework.web.reactive.function.server.RouterFunctions
import org.springframework.web.reactive.function.server.ServerResponse
import reactor.core.publisher.Mono

/**
 * Builds the REST routes for the sender side. For every configured `restqa.sender.<key>`
 * entry a dedicated [SenderEndpointController] is created and bound to the sender's
 * [org.pcsoft.micro.restqa.configuration.SenderProperties.rest] path. The sender key
 * is exposed to logging via the [MDC] (field `sender`), not threaded through the handler.
 *
 * The paths are only known at runtime (from configuration), hence the functional
 * [RouterFunction] approach instead of static `@RestController` mappings.
 */
@Configuration
class SenderEndpointConfiguration(
    private val properties: RestqaProperties,
    private val queueClient: MessageQueueClient,
    private val synchronousRegistry: SynchronousResponseRegistry,
) {

    companion object {
        private val log = logger()

        /** MDC field carrying the configured sender key during request handling. */
        const val MDC_SENDER = "sender"
    }

    @PostConstruct
    private fun init() {
        validateSynchronousConfiguration()
        log.info("Configured senders")
    }

    private fun validateSynchronousConfiguration() {
        // Collect all receiver names referenced by senders for synchronous mode.
        val syncReceiverRefs = mutableSetOf<String>()

        properties.sender.forEach { (senderKey, senderProperties) ->
            val syncConfig = senderProperties.synchronous ?: return@forEach
            val receiverRef = syncConfig.receiverRef
            syncReceiverRefs += receiverRef

            // Rule 1: Referenced receiver must exist.
            val receiver = properties.receiver[receiverRef]
            requireNotNull(receiver) {
                "Sender '$senderKey' references receiver '$receiverRef' via synchronous.receiver-ref, " +
                    "but no receiver with that name is configured."
            }

            // Rule 2: Referenced receiver must NOT have a URL (it's a sync-only channel).
            require(receiver.rest.url == null) {
                "Sender '$senderKey' references receiver '$receiverRef' via synchronous.receiver-ref, " +
                    "but that receiver has a rest.url configured. " +
                    "A synchronous receiver must not have a URL — it only acknowledges back to the sender."
            }
        }

        // Validate receivers.
        properties.receiver.forEach { (receiverKey, receiverProperties) ->
            if (receiverProperties.rest.url == null) {
                // Rule 3: Receiver without URL must be referenced by at least one sender.
                require(receiverKey in syncReceiverRefs) {
                    "Receiver '$receiverKey' has no rest.url configured but is not referenced " +
                        "by any sender's synchronous.receiver-ref. " +
                        "A receiver without URL can only serve as a synchronous response channel."
                }
            } else {
                // Rule 4: Receiver with URL must NOT be used as a synchronous reference.
                require(receiverKey !in syncReceiverRefs) {
                    "Receiver '$receiverKey' has a rest.url configured but is referenced " +
                        "by a sender's synchronous.receiver-ref. " +
                        "A synchronous receiver must not have a URL."
                }
            }
        }
    }

    @Bean
    fun senderRouter(): RouterFunction<ServerResponse> {
        // An empty builder cannot be built (Spring requires at least one route), so a
        // router that matches nothing is returned when no sender flows are configured.
        if (properties.sender.isEmpty()) {
            return RouterFunction { Mono.empty() }
        }
        val builder = RouterFunctions.route()
        properties.sender.forEach { (senderKey, senderProperties) ->
            val handler = SenderEndpointController(
                senderProperties,
                queueClient,
                properties.maxPayloadSize,
                synchronousRegistry,
            )
            builder.route(RequestPredicates.path(senderProperties.rest.path)) { request ->
                MDC.putCloseable(MDC_SENDER, senderKey).use { handler.handle(request) }
            }
            log.info("Registered sender endpoint [{}] on path '{}'", senderKey, senderProperties.rest.path)
        }
        return builder.build()
    }
}

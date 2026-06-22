package org.pcsoft.micro.restqa.send.configuration

import jakarta.annotation.PostConstruct
import org.pcsoft.micro.restqa.configuration.RestqaProperties
import org.pcsoft.micro.restqa.internal.utils.logger
import org.pcsoft.micro.restqa.send.port.SenderEndpointController
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.web.reactive.function.server.RequestPredicates
import org.springframework.web.reactive.function.server.RouterFunction
import org.springframework.web.reactive.function.server.RouterFunctions
import org.springframework.web.reactive.function.server.ServerResponse
import reactor.core.publisher.Mono

/**
 * Builds the REST routes for the sender side. For every configured `restqa.sender.<flow>`
 * entry a dedicated [SenderEndpointController] is created and bound to the flow's
 * [org.pcsoft.micro.restqa.configuration.SenderProperties.endpoint] path.
 *
 * The paths are only known at runtime (from configuration), hence the functional
 * [RouterFunction] approach instead of static `@RestController` mappings.
 */
@Configuration
class SenderEndpointConfiguration(
    private val properties: RestqaProperties,
) {

    companion object {
        private val log = logger()
    }

    @PostConstruct
    private fun init() {
        log.info("Configured senders")
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
            val handler = SenderEndpointController(senderKey, senderProperties)
            builder.route(RequestPredicates.path(senderProperties.endpoint), handler::handle)
            log.info("Registered sender endpoint [{}] on path '{}'", senderKey, senderProperties.endpoint)
        }
        return builder.build()
    }
}

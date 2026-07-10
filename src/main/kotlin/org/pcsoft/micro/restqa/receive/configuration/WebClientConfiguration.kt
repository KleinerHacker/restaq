package org.pcsoft.micro.restqa.receive.configuration

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.web.reactive.function.client.WebClient

/**
 * Provides the [WebClient.Builder] used on the receive side to forward consumed
 * messages to the external REST endpoint configured per receiver flow.
 */
@Configuration
class WebClientConfiguration {

    /**
     * Provides a default [WebClient.Builder] bean for constructing HTTP clients on
     * the receiver side. Receivers use this builder to create per-flow clients that
     * POST consumed messages to the configured target URL.
     */
    @Bean
    fun webClientBuilder(): WebClient.Builder = WebClient.builder()
}

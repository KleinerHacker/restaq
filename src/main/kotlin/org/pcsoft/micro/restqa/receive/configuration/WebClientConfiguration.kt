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

    @Bean
    fun webClientBuilder(): WebClient.Builder = WebClient.builder()
}

package org.pcsoft.micro.restqa.receive.port

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.aResponse
import com.github.tomakehurst.wiremock.client.WireMock.equalTo
import com.github.tomakehurst.wiremock.client.WireMock.post
import com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor
import com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.options
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import org.springframework.amqp.core.MessageProperties
import org.springframework.amqp.rabbit.core.RabbitTemplate
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.containers.RabbitMQContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.time.Duration

/**
 * End-to-end receive test for the AMQP variant: a message is published onto a real
 * RabbitMQ queue (Testcontainers) with a body and a custom header; the gateway consumes
 * it and POSTs it to a real downstream HTTP endpoint (WireMock). The test asserts the
 * forwarded request has the correct body and the complete set of HTTP headers, including
 * the custom one.
 */
@SpringBootTest
@Testcontainers
@org.springframework.test.annotation.DirtiesContext
class ReceiverAmqpIntegrationTest {

    companion object {
        private const val PATH = "/notify"
        private const val QUEUE = "it.receive.amqp.queue"

        private val wireMock = WireMockServer(options().dynamicPort()).apply {
            start()
            stubFor(post(urlEqualTo(PATH)).willReturn(aResponse().withStatus(200)))
        }

        @Container
        @JvmStatic
        private val rabbit = RabbitMQContainer("rabbitmq:3.13-management-alpine")

        @Suppress("unused")
        @DynamicPropertySource
        @JvmStatic
        fun properties(registry: DynamicPropertyRegistry) {
            registry.add("spring.rabbitmq.host", rabbit::getHost)
            registry.add("spring.rabbitmq.port", rabbit::getAmqpPort)
            registry.add("spring.rabbitmq.username", rabbit::getAdminUsername)
            registry.add("spring.rabbitmq.password", rabbit::getAdminPassword)

            registry.add("restqa.type") { "amqp" }
            registry.add("restqa.receiver.test.rest.url") { "http://localhost:${wireMock.port()}$PATH" }
            registry.add("restqa.receiver.test.queue.name") { QUEUE }
        }

        @AfterAll
        @JvmStatic
        fun stopWireMock() = wireMock.stop()
    }

    @Autowired
    private lateinit var rabbitTemplate: RabbitTemplate

    /**
     * Verifies the full AMQP receive pipeline end-to-end: publishes a message with a body
     * ("Hello World"), a contentType property, and a custom header onto a RabbitMQ queue.
     * Asserts that the gateway consumes the message and forwards it via HTTP POST to
     * WireMock with the correct body, the custom header propagated, and Content-Type
     * mapped from the AMQP message properties back to an HTTP header.
     */
    @Test
    fun `consumes an AMQP message and forwards body and headers (incl custom) over HTTP`() {
        rabbitTemplate.convertAndSend(QUEUE, "Hello World".toByteArray()) { message ->
            message.messageProperties.contentType = "text/plain"
            message.messageProperties.setHeader("custom", "demo")
            message
        }

        await().atMost(Duration.ofSeconds(15)).untilAsserted {
            wireMock.verify(
                postRequestedFor(urlEqualTo(PATH))
                    .withRequestBody(equalTo("Hello World"))
                    .withHeader("custom", equalTo("demo"))
                    // contentType is mapped back onto the Content-Type HTTP header.
                    .withHeader("Content-Type", equalTo(MessageProperties.CONTENT_TYPE_TEXT_PLAIN)),
            )
        }
    }
}

package org.pcsoft.micro.restqa.receive.controller

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
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.jms.core.JmsTemplate
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.activemq.ArtemisContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.time.Duration

/**
 * End-to-end receive test for the JMS variant: a message is published onto a real
 * ActiveMQ Artemis queue (Testcontainers) with a body and a custom property; the gateway
 * consumes it and POSTs it to a real downstream HTTP endpoint (WireMock). The test asserts
 * the forwarded request has the correct body and the complete set of HTTP headers,
 * including the custom one.
 */
@SpringBootTest
@Testcontainers
class ReceiverJmsIntegrationTest {

    companion object {
        private const val PATH = "/notify"
        private const val QUEUE = "it.receive.jms.queue"

        private val wireMock = WireMockServer(options().dynamicPort()).apply {
            start()
            stubFor(post(urlEqualTo(PATH)).willReturn(aResponse().withStatus(200)))
        }

        @Container
        @JvmStatic
        private val artemis = ArtemisContainer("apache/activemq-artemis:2.31.2")
            .withUser("artemis")
            .withPassword("artemis")

        @Suppress("unused")
        @DynamicPropertySource
        @JvmStatic
        fun properties(registry: DynamicPropertyRegistry) {
            registry.add("spring.artemis.mode") { "native" }
            registry.add("spring.artemis.broker-url", artemis::getBrokerUrl)
            registry.add("spring.artemis.user", artemis::getUser)
            registry.add("spring.artemis.password", artemis::getPassword)

            registry.add("restqa.queue.type") { "jms" }
            registry.add("restqa.receiver.test.endpoint") { "http://localhost:${wireMock.port()}$PATH" }
            registry.add("restqa.receiver.test.queue.name") { QUEUE }
        }

        @AfterAll
        @JvmStatic
        fun stopWireMock() = wireMock.stop()
    }

    @Autowired
    private lateinit var jmsTemplate: JmsTemplate

    @Test
    fun `consumes a JMS message and forwards body and headers (incl custom) over HTTP`() {
        // A ByteArray payload is converted to a BytesMessage (matching JmsQueueConsumer).
        jmsTemplate.convertAndSend(QUEUE, "Hello World".toByteArray()) { message ->
            message.setStringProperty("custom", "demo")
            message
        }

        await().atMost(Duration.ofSeconds(15)).untilAsserted {
            wireMock.verify(
                postRequestedFor(urlEqualTo(PATH))
                    .withRequestBody(equalTo("Hello World"))
                    .withHeader("custom", equalTo("demo")),
            )
        }
    }
}

package org.pcsoft.micro.restqa.send.port

import jakarta.jms.BytesMessage
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.http.MediaType
import org.springframework.jms.core.JmsTemplate
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.springframework.test.web.reactive.server.WebTestClient
import org.testcontainers.activemq.ArtemisContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

/**
 * End-to-end send test for the JMS variant: a real HTTP request hits the configured
 * sender endpoint, the gateway publishes it onto a real ActiveMQ Artemis queue
 * (Testcontainers), and the test asserts the message body and all forwarded HTTP headers
 * — including the custom one — arrived intact.
 *
 * Note JMS property-name sanitization in [org.pcsoft.micro.restqa.send.controller.JmsQueueClient]:
 * non-alphanumeric characters become '_', so `Content-Type` arrives as `Content_Type`
 * while `custom` is unaffected.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class SenderJmsIntegrationTest {

    companion object {
        private const val ENDPOINT = "/api/send"
        private const val QUEUE = "it.send.jms.queue"

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
            // Avoid blocking forever if no message arrives.
            registry.add("spring.jms.template.receive-timeout") { "10s" }

            registry.add("restqa.queue.type") { "jms" }
            registry.add("restqa.sender.test.endpoint") { ENDPOINT }
            registry.add("restqa.sender.test.queue.name") { QUEUE }
        }
    }

    @Autowired
    private lateinit var client: WebTestClient

    @Autowired
    private lateinit var jmsTemplate: JmsTemplate

    @Test
    fun `forwards body and HTTP headers (incl custom) onto the JMS queue`() {
        client.post()
            .uri(ENDPOINT)
            .contentType(MediaType.TEXT_PLAIN)
            .header("custom", "demo")
            .bodyValue("Hello World".toByteArray())
            .exchange()
            .expectStatus().isOk

        val message = jmsTemplate.receive(QUEUE)
        assertThat(message).withFailMessage("expected a message on destination '%s'", QUEUE).isNotNull
        assertThat(message).isInstanceOf(BytesMessage::class.java)

        val bytes = (message as BytesMessage).let { ByteArray(it.bodyLength.toInt()).also { buf -> it.readBytes(buf) } }
        assertThat(String(bytes, Charsets.UTF_8)).isEqualTo("Hello World")

        // Custom header survives unchanged; standard headers arrive with sanitized names.
        assertThat(message.getStringProperty("custom")).isEqualTo("demo")
        assertThat(message.getStringProperty("Content_Type")).contains(MediaType.TEXT_PLAIN_VALUE)
        assertThat(message.getStringProperty("content_length")).isNotNull()
    }
}

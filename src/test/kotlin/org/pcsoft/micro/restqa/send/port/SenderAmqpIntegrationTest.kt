package org.pcsoft.micro.restqa.send.port

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.amqp.core.Message
import org.springframework.amqp.rabbit.core.RabbitTemplate
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.http.MediaType
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.springframework.test.web.reactive.server.WebTestClient
import org.testcontainers.containers.RabbitMQContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

/**
 * End-to-end send test for the AMQP variant: a real HTTP request hits the configured
 * sender endpoint, the gateway publishes it onto a real RabbitMQ queue (Testcontainers),
 * and the test asserts the message body and all forwarded HTTP headers — including the
 * custom one — arrived intact.
 *
 * Only a sender is configured (no receiver), so nothing competes for / drains the queue.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient
@Testcontainers
@org.springframework.test.annotation.DirtiesContext
class SenderAmqpIntegrationTest {

    companion object {
        private const val ENDPOINT = "/api/send"
        private const val QUEUE = "it.send.amqp.queue"

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
            registry.add("restqa.sender.test.rest.path") { ENDPOINT }
            registry.add("restqa.sender.test.queue.name") { QUEUE }
        }
    }

    @Autowired
    private lateinit var client: WebTestClient

    @Autowired
    private lateinit var rabbitTemplate: RabbitTemplate

    @Test
    fun `forwards body and HTTP headers (incl custom) onto the AMQP queue`() {
        client.post()
            .uri(ENDPOINT)
            .contentType(MediaType.TEXT_PLAIN)
            .header("custom", "demo")
            .bodyValue("Hello World".toByteArray())
            .exchange()
            .expectStatus().isAccepted

        val message: Message? = rabbitTemplate.receive(QUEUE, 10_000)
        assertThat(message).withFailMessage("expected a message on queue '%s'", QUEUE).isNotNull
        message!!

        assertThat(String(message.body, Charsets.UTF_8)).isEqualTo("Hello World")

        val headers = message.messageProperties.headers
        // Custom header survives unchanged.
        assertThat(headers).containsEntry("custom", "demo")
        // Content-Type is propagated; transport headers (host, content-length) are filtered.
        assertThat(headers).containsKey("Content-Type")
        assertThat(headers).doesNotContainKey("host")
        assertThat(headers["Content-Type"].toString()).contains(MediaType.TEXT_PLAIN_VALUE)
    }
}

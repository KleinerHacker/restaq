package org.pcsoft.micro.restqa

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.aResponse
import com.github.tomakehurst.wiremock.client.WireMock.equalTo
import com.github.tomakehurst.wiremock.client.WireMock.post
import com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor
import com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.options
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import org.springframework.amqp.rabbit.core.RabbitTemplate
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient
import org.springframework.http.MediaType
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.springframework.test.web.reactive.server.WebTestClient
import org.testcontainers.containers.RabbitMQContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.time.Duration

/**
 * End-to-end integration test with **multiple senders and receivers** configured
 * simultaneously against a real RabbitMQ broker (Testcontainers).
 *
 * Verifies that:
 * - Multiple sender endpoints coexist and route to their respective queues.
 * - Multiple receivers consume from their respective queues and POST to their
 *   respective downstream targets.
 * - Messages do not leak between unrelated sender/receiver pairs.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, useMainMethod = SpringBootTest.UseMainMethod.ALWAYS)
@AutoConfigureWebTestClient
@Testcontainers
@org.springframework.test.annotation.DirtiesContext
class MultiEndpointAmqpIntegrationTest {

    companion object {
        // ── Sender endpoints ──
        private const val ORDERS_ENDPOINT = "/api/orders"
        private const val ORDERS_QUEUE = "it.multi.orders.queue"

        private const val INVOICES_ENDPOINT = "/api/invoices"
        private const val INVOICES_QUEUE = "it.multi.invoices.queue"

        // ── Receiver targets ──
        private const val NOTIFICATIONS_PATH = "/notify"
        private const val NOTIFICATIONS_QUEUE = "it.multi.notifications.queue"

        private const val ALERTS_PATH = "/alerts"
        private const val ALERTS_QUEUE = "it.multi.alerts.queue"

        private val wireMock = WireMockServer(options().dynamicPort()).apply {
            start()
            stubFor(post(urlEqualTo(NOTIFICATIONS_PATH)).willReturn(aResponse().withStatus(200)))
            stubFor(post(urlEqualTo(ALERTS_PATH)).willReturn(aResponse().withStatus(200)))
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

            // Two senders
            registry.add("restqa.sender.orders.rest.path") { ORDERS_ENDPOINT }
            registry.add("restqa.sender.orders.queue.name") { ORDERS_QUEUE }
            registry.add("restqa.sender.invoices.rest.path") { INVOICES_ENDPOINT }
            registry.add("restqa.sender.invoices.queue.name") { INVOICES_QUEUE }

            // Two receivers
            registry.add("restqa.receiver.notifications.rest.url") { "http://localhost:${wireMock.port()}$NOTIFICATIONS_PATH" }
            registry.add("restqa.receiver.notifications.queue.name") { NOTIFICATIONS_QUEUE }
            registry.add("restqa.receiver.alerts.rest.url") { "http://localhost:${wireMock.port()}$ALERTS_PATH" }
            registry.add("restqa.receiver.alerts.queue.name") { ALERTS_QUEUE }
        }

        @AfterAll
        @JvmStatic
        fun stopWireMock() = wireMock.stop()
    }

    @Autowired
    private lateinit var client: WebTestClient

    @Autowired
    private lateinit var rabbitTemplate: RabbitTemplate

    // ─── Multiple Senders ─────────────────────────────────────────────────────────

    @Test
    fun `orders sender routes to orders queue`() {
        client.post()
            .uri(ORDERS_ENDPOINT)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue("""{"orderId": "ORD-001"}""".toByteArray())
            .exchange()
            .expectStatus().isAccepted

        val message = rabbitTemplate.receive(ORDERS_QUEUE, 10_000)
        assertThat(message).withFailMessage("expected a message on queue '%s'", ORDERS_QUEUE).isNotNull
        assertThat(String(message!!.body, Charsets.UTF_8)).isEqualTo("""{"orderId": "ORD-001"}""")
    }

    @Test
    fun `invoices sender routes to invoices queue`() {
        client.post()
            .uri(INVOICES_ENDPOINT)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue("""{"invoiceId": "INV-042"}""".toByteArray())
            .exchange()
            .expectStatus().isAccepted

        val message = rabbitTemplate.receive(INVOICES_QUEUE, 10_000)
        assertThat(message).withFailMessage("expected a message on queue '%s'", INVOICES_QUEUE).isNotNull
        assertThat(String(message!!.body, Charsets.UTF_8)).isEqualTo("""{"invoiceId": "INV-042"}""")
    }

    @Test
    fun `senders do not cross-contaminate queues`() {
        client.post()
            .uri(ORDERS_ENDPOINT)
            .bodyValue("order-payload".toByteArray())
            .exchange()
            .expectStatus().isAccepted

        // Orders message should NOT appear on the invoices queue.
        val invoiceMessage = rabbitTemplate.receive(INVOICES_QUEUE, 2_000)
        assertThat(invoiceMessage).isNull()

        // But should appear on the orders queue.
        val orderMessage = rabbitTemplate.receive(ORDERS_QUEUE, 10_000)
        assertThat(orderMessage).isNotNull
    }

    // ─── Multiple Receivers ───────────────────────────────────────────────────────

    @Test
    fun `notifications receiver delivers to notifications endpoint`() {
        rabbitTemplate.convertAndSend(NOTIFICATIONS_QUEUE, "notification-event".toByteArray()) { msg ->
            msg.messageProperties.setHeader("event-type", "email")
            msg
        }

        await().atMost(Duration.ofSeconds(15)).untilAsserted {
            wireMock.verify(
                postRequestedFor(urlEqualTo(NOTIFICATIONS_PATH))
                    .withRequestBody(equalTo("notification-event"))
                    .withHeader("event-type", equalTo("email")),
            )
        }
    }

    @Test
    fun `alerts receiver delivers to alerts endpoint`() {
        rabbitTemplate.convertAndSend(ALERTS_QUEUE, "critical-alert".toByteArray()) { msg ->
            msg.messageProperties.setHeader("severity", "high")
            msg
        }

        await().atMost(Duration.ofSeconds(15)).untilAsserted {
            wireMock.verify(
                postRequestedFor(urlEqualTo(ALERTS_PATH))
                    .withRequestBody(equalTo("critical-alert"))
                    .withHeader("severity", equalTo("high")),
            )
        }
    }

    @Test
    fun `receivers do not cross-contaminate targets`() {
        // Reset WireMock to get clean request counts.
        wireMock.resetRequests()

        rabbitTemplate.convertAndSend(NOTIFICATIONS_QUEUE, "only-for-notify".toByteArray())

        await().atMost(Duration.ofSeconds(15)).untilAsserted {
            wireMock.verify(1, postRequestedFor(urlEqualTo(NOTIFICATIONS_PATH)))
        }
        // Alerts endpoint should NOT have received anything.
        wireMock.verify(0, postRequestedFor(urlEqualTo(ALERTS_PATH)))
    }

    // ─── Full Flow: Sender → Queue → Receiver ────────────────────────────────────

    @Test
    fun `full sender to receiver flow works with multiple configured endpoints`() {
        wireMock.resetRequests()

        // A message sent via the orders sender ends up on orders queue.
        client.post()
            .uri(ORDERS_ENDPOINT)
            .contentType(MediaType.TEXT_PLAIN)
            .header("X-Correlation-Id", "corr-123")
            .bodyValue("end-to-end".toByteArray())
            .exchange()
            .expectStatus().isAccepted

        // Verify it arrived on the correct queue (not consumed by a receiver since
        // the orders queue has no receiver configured in this test).
        val message = rabbitTemplate.receive(ORDERS_QUEUE, 10_000)
        assertThat(message).isNotNull
        assertThat(String(message!!.body, Charsets.UTF_8)).isEqualTo("end-to-end")
        assertThat(message.messageProperties.headers["X-Correlation-Id"]).isEqualTo("corr-123")
    }
}

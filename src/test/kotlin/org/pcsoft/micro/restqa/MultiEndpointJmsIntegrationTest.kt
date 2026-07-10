package org.pcsoft.micro.restqa

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.aResponse
import com.github.tomakehurst.wiremock.client.WireMock.equalTo
import com.github.tomakehurst.wiremock.client.WireMock.post
import com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor
import com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.options
import jakarta.jms.BytesMessage
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient
import org.springframework.http.MediaType
import org.springframework.jms.core.JmsTemplate
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.springframework.test.web.reactive.server.WebTestClient
import org.testcontainers.activemq.ArtemisContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.time.Duration

/**
 * End-to-end integration test with **multiple senders and receivers** configured
 * simultaneously against a real ActiveMQ Artemis broker (Testcontainers).
 *
 * Verifies that:
 * - Multiple sender endpoints coexist and route to their respective JMS destinations.
 * - Multiple receivers consume from their respective destinations and POST to their
 *   respective downstream targets.
 * - Messages do not leak between unrelated sender/receiver pairs.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, useMainMethod = SpringBootTest.UseMainMethod.ALWAYS)
@AutoConfigureWebTestClient
@Testcontainers
@org.springframework.test.annotation.DirtiesContext
class MultiEndpointJmsIntegrationTest {

    companion object {
        // ── Sender endpoints ──
        private const val ORDERS_ENDPOINT = "/api/orders"
        private const val ORDERS_QUEUE = "it.multi.jms.orders"

        private const val INVOICES_ENDPOINT = "/api/invoices"
        private const val INVOICES_QUEUE = "it.multi.jms.invoices"

        // ── Receiver targets ──
        private const val NOTIFICATIONS_PATH = "/notify"
        private const val NOTIFICATIONS_QUEUE = "it.multi.jms.notifications"

        private const val ALERTS_PATH = "/alerts"
        private const val ALERTS_QUEUE = "it.multi.jms.alerts"

        private val wireMock = WireMockServer(options().dynamicPort()).apply {
            start()
            stubFor(post(urlEqualTo(NOTIFICATIONS_PATH)).willReturn(aResponse().withStatus(200)))
            stubFor(post(urlEqualTo(ALERTS_PATH)).willReturn(aResponse().withStatus(200)))
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
            registry.add("spring.jms.template.receive-timeout") { "10s" }

            registry.add("restqa.type") { "jms" }

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
    private lateinit var jmsTemplate: JmsTemplate

    // ─── Multiple Senders ─────────────────────────────────────────────────────────

    /**
     * Verifies that a POST to the orders sender endpoint results in a JMS BytesMessage
     * being placed on the orders destination with the correct body content. Confirms
     * the sender returns HTTP 202 Accepted and the JMS message payload matches the
     * original JSON request body exactly.
     */
    @Test
    fun `orders sender routes to orders destination`() {
        client.post()
            .uri(ORDERS_ENDPOINT)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue("""{"orderId": "ORD-001"}""".toByteArray())
            .exchange()
            .expectStatus().isAccepted

        val message = jmsTemplate.receive(ORDERS_QUEUE)
        assertThat(message).withFailMessage("expected a message on destination '%s'", ORDERS_QUEUE).isNotNull
        assertThat(message).isInstanceOf(BytesMessage::class.java)
        val bytes = (message as BytesMessage).let { ByteArray(it.bodyLength.toInt()).also { buf -> it.readBytes(buf) } }
        assertThat(String(bytes, Charsets.UTF_8)).isEqualTo("""{"orderId": "ORD-001"}""")
    }

    /**
     * Verifies that a POST to the invoices sender endpoint results in a JMS BytesMessage
     * being placed on the invoices destination with the correct body content. Confirms
     * that the second sender endpoint operates independently of the first and routes
     * to its own dedicated JMS destination.
     */
    @Test
    fun `invoices sender routes to invoices destination`() {
        client.post()
            .uri(INVOICES_ENDPOINT)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue("""{"invoiceId": "INV-042"}""".toByteArray())
            .exchange()
            .expectStatus().isAccepted

        val message = jmsTemplate.receive(INVOICES_QUEUE)
        assertThat(message).withFailMessage("expected a message on destination '%s'", INVOICES_QUEUE).isNotNull
        assertThat(message).isInstanceOf(BytesMessage::class.java)
        val bytes = (message as BytesMessage).let { ByteArray(it.bodyLength.toInt()).also { buf -> it.readBytes(buf) } }
        assertThat(String(bytes, Charsets.UTF_8)).isEqualTo("""{"invoiceId": "INV-042"}""")
    }

    /**
     * Verifies that messages sent through one sender endpoint do not appear on
     * another sender's JMS destination. A message posted to the orders endpoint
     * must only be present on the orders destination, and the invoices destination
     * must remain empty. This confirms proper routing isolation between configured
     * sender flows in JMS mode.
     */
    @Test
    fun `senders do not cross-contaminate destinations`() {
        client.post()
            .uri(ORDERS_ENDPOINT)
            .bodyValue("order-only".toByteArray())
            .exchange()
            .expectStatus().isAccepted

        // Set a short receive timeout for the negative check.
        val shortTemplate = JmsTemplate(jmsTemplate.connectionFactory!!).apply {
            receiveTimeout = 2_000
            isPubSubDomain = false
        }

        // Orders message should NOT appear on the invoices destination.
        val invoiceMessage = shortTemplate.receive(INVOICES_QUEUE)
        assertThat(invoiceMessage).isNull()

        // But should appear on the orders destination.
        val orderMessage = jmsTemplate.receive(ORDERS_QUEUE)
        assertThat(orderMessage).isNotNull
    }

    // ─── Multiple Receivers ───────────────────────────────────────────────────────

    /**
     * Verifies that the notifications receiver consumes a message from the
     * notifications JMS destination and delivers it via HTTP POST to the configured
     * downstream notifications endpoint. Confirms both the message body and custom
     * JMS string properties are propagated as HTTP headers to the target URL.
     */
    @Test
    fun `notifications receiver delivers to notifications endpoint`() {
        jmsTemplate.convertAndSend(NOTIFICATIONS_QUEUE, "notification-event".toByteArray()) { msg ->
            msg.setStringProperty("event_type", "email")
            msg
        }

        await().atMost(Duration.ofSeconds(15)).untilAsserted {
            wireMock.verify(
                postRequestedFor(urlEqualTo(NOTIFICATIONS_PATH))
                    .withRequestBody(equalTo("notification-event"))
                    .withHeader("event_type", equalTo("email")),
            )
        }
    }

    /**
     * Verifies that the alerts receiver consumes a message from the alerts JMS
     * destination and delivers it via HTTP POST to the configured downstream alerts
     * endpoint. Confirms both the message body and custom JMS string properties
     * (severity) are propagated correctly and independently of the notifications receiver.
     */
    @Test
    fun `alerts receiver delivers to alerts endpoint`() {
        jmsTemplate.convertAndSend(ALERTS_QUEUE, "critical-alert".toByteArray()) { msg ->
            msg.setStringProperty("severity", "high")
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

    /**
     * Verifies that messages consumed by one receiver are not delivered to another
     * receiver's target URL. A message placed on the notifications destination must
     * only trigger a POST to the notifications endpoint; the alerts endpoint must
     * receive zero requests. This confirms proper consumer isolation between
     * receiver flows in JMS mode.
     */
    @Test
    fun `receivers do not cross-contaminate targets`() {
        wireMock.resetRequests()

        jmsTemplate.convertAndSend(NOTIFICATIONS_QUEUE, "only-for-notify".toByteArray())

        await().atMost(Duration.ofSeconds(15)).untilAsserted {
            wireMock.verify(1, postRequestedFor(urlEqualTo(NOTIFICATIONS_PATH)))
        }
        // Alerts endpoint should NOT have received anything.
        wireMock.verify(0, postRequestedFor(urlEqualTo(ALERTS_PATH)))
    }

    // ─── Full Flow: Sender → Queue → Receiver ────────────────────────────────────

    /**
     * Verifies the complete sender-to-queue flow when multiple JMS endpoints are configured.
     * A message sent via the orders sender endpoint (which has no receiver configured in
     * this test) arrives on the orders JMS destination with the correct body payload and
     * propagated custom headers. Confirms that HTTP header names are sanitized to valid
     * JMS property names (hyphens replaced with underscores) during transit.
     */
    @Test
    fun `full sender to receiver flow with multiple endpoints`() {
        wireMock.resetRequests()

        // Send via orders sender (this queue has no receiver, so we can read it manually).
        client.post()
            .uri(ORDERS_ENDPOINT)
            .contentType(MediaType.TEXT_PLAIN)
            .header("X-Correlation-Id", "corr-456")
            .bodyValue("end-to-end-jms".toByteArray())
            .exchange()
            .expectStatus().isAccepted

        // Verify it arrived on the correct destination.
        val message = jmsTemplate.receive(ORDERS_QUEUE)
        assertThat(message).isNotNull
        assertThat(message).isInstanceOf(BytesMessage::class.java)
        val bytes = (message as BytesMessage).let { ByteArray(it.bodyLength.toInt()).also { buf -> it.readBytes(buf) } }
        assertThat(String(bytes, Charsets.UTF_8)).isEqualTo("end-to-end-jms")
        // JMS property name sanitization: X-Correlation-Id → X_Correlation_Id
        assertThat(message.getStringProperty("X_Correlation_Id")).isEqualTo("corr-456")
    }
}

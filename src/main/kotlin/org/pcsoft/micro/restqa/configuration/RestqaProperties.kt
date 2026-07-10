package org.pcsoft.micro.restqa.configuration

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.util.unit.DataSize
import java.time.Duration

/**
 * Selectable message-queue technology. The active one is chosen globally via the
 * `restqa.type` property and switches the corresponding configuration beans
 * on/off through `@ConditionalOnProperty`.
 */
enum class QueueType {
    AMQP,
    JMS,
}

/**
 * Root configuration of RESTAQ, bound from the `restqa.*` namespace.
 *
 * Both [sender] and [receiver] are keyed maps: the **key** is a human-readable name
 * used purely for logging/diagnostics, while the value carries the REST endpoint and
 * the queue endpoint for that flow.
 */
@ConfigurationProperties(prefix = "restqa")
data class RestqaProperties(
    /** Global queue technology selector. Defaults to [QueueType.AMQP] (RabbitMQ). */
    val type: QueueType = QueueType.AMQP,
    /** Sender flows: REST in -> queue out. Map key is the logging name. */
    val sender: Map<String, SenderProperties> = emptyMap(),
    /** Receiver flows: queue in -> REST out. Map key is the logging name. */
    val receiver: Map<String, ReceiverProperties> = emptyMap(),
    /** Optional maximum payload size. Requests exceeding this size are rejected. */
    val maxPayloadSize: DataSize? = null,
)

/**
 * Queue endpoint settings for a single flow. [name] is the queue/destination name.
 * [exchange] and [routingKey] are AMQP-specific and ignored for JMS, where only
 * [name] (the destination) is used.
 *
 * @property name The queue or destination name used by both AMQP and JMS.
 * @property exchange The AMQP exchange to publish to (optional, AMQP only). When `null`, the default exchange is used.
 * @property routingKey The AMQP routing key (optional, AMQP only). Defaults to [name] when not specified.
 * @property properties Arbitrary key-value pairs forwarded as broker-specific message properties.
 */
data class QueueEndpointProperties(
    val name: String,
    val exchange: String? = null,
    val routingKey: String? = null,
    val properties: Map<String, String> = emptyMap(),
)

/**
 * A sender flow. [rest] defines the REST path on which RESTAQ accepts requests to be
 * forwarded into [queue]. Optionally, [synchronous] enables synchronous request/response
 * mode where the sender waits for the receiver to process and acknowledge the message.
 *
 * [timeout] defines the maximum time the sender will wait for a response — regardless
 * of whether the flow is synchronous or asynchronous. Default is 30 seconds.
 *
 * @property rest REST endpoint configuration (path) on which this sender accepts requests.
 * @property queue Queue endpoint to which accepted requests are forwarded.
 * @property synchronous Optional synchronous mode configuration referencing a receiver for request/reply.
 * @property timeout Maximum duration to wait for queue acknowledgement or synchronous response. Defaults to 30 s.
 */
data class SenderProperties(
    val rest: SenderRestProperties,
    val queue: QueueEndpointProperties,
    val synchronous: SenderSynchronousProperties? = null,
    val timeout: Duration = Duration.ofSeconds(30),
)

/**
 * REST configuration for a sender endpoint.
 *
 * @property path The HTTP path (e.g. `/api/orders`) on which the sender endpoint is exposed.
 */
data class SenderRestProperties(
    val path: String,
)

/**
 * Synchronous configuration for a sender endpoint.
 *
 * When configured, the sender will wait for the referenced receiver to process
 * the message and wake the sender up, instead of immediately replying with
 * 202 Accepted.
 */
data class SenderSynchronousProperties(
    /** Name of the receiver flow (map key in `restqa.receiver`) that handles the response. */
    val receiverRef: String,
)

/**
 * A receiver flow. Consumes messages from [queue] and either:
 * - forwards them via HTTP POST to [rest.url] (asynchronous mode), or
 * - acknowledges processing back to a waiting sender (synchronous mode, no URL).
 *
 * [timeout] defines the maximum time the receiver will wait for the downstream
 * target to respond (when [rest.url] is set). Default is 30 seconds.
 *
 * @property rest REST callback configuration. When [ReceiverRestProperties.url] is set, consumed messages are forwarded there.
 * @property queue Queue endpoint from which messages are consumed.
 * @property retry Retry configuration (max attempts and backoff) for failed deliveries (async only).
 * @property timeToLive Optional maximum message age. Messages older than this are discarded on receipt.
 * @property timeout Maximum duration to wait for the downstream target to respond. Defaults to 30 s.
 */
data class ReceiverProperties(
    val rest: ReceiverRestProperties = ReceiverRestProperties(),
    val queue: QueueEndpointProperties,
    val retry: RetryProperties = RetryProperties(),
    val timeToLive: Duration? = null,
    val timeout: Duration = Duration.ofSeconds(30),
)

/**
 * REST configuration for a receiver endpoint.
 *
 * [url] is the target HTTP URL for callback delivery. It is `null` when the receiver
 * acts as a synchronous response channel (referenced by a sender's `synchronous.receiver-ref`).
 *
 * @property url The target HTTP URL for callback delivery, or `null` for synchronous-only receivers.
 */
data class ReceiverRestProperties(
    val url: String? = null,
)

/**
 * Retry configuration for receiver flows. Only applies to asynchronous receivers
 * (those with a configured [ReceiverRestProperties.url]).
 *
 * @property maxRetries Maximum number of delivery attempts before routing to the dead-letter queue. Defaults to 3.
 * @property backoffPeriod Delay between consecutive retry attempts. Defaults to 5 seconds.
 */
data class RetryProperties(
    val maxRetries: Int = 3,
    val backoffPeriod: Duration = Duration.ofSeconds(5),
)

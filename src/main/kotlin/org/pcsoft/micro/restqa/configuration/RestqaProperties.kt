package org.pcsoft.micro.restqa.configuration

import org.springframework.boot.context.properties.ConfigurationProperties

/**
 * Selectable message-queue technology. The active one is chosen globally via the
 * `restqa.queue.type` property and switches the corresponding configuration beans
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
    val queue: QueueProperties = QueueProperties(),
    /** Sender flows: REST in -> queue out. Map key is the logging name. */
    val sender: Map<String, SenderProperties> = emptyMap(),
    /** Receiver flows: queue in -> REST out. Map key is the logging name. */
    val receiver: Map<String, ReceiverProperties> = emptyMap(),
)

data class QueueProperties(
    val type: QueueType = QueueType.AMQP,
)

/**
 * Queue endpoint settings for a single flow. [name] is the queue/destination name.
 * [exchange] and [routingKey] are AMQP-specific and ignored for JMS, where only
 * [name] (the destination) is used.
 */
data class QueueEndpointProperties(
    val name: String,
    val exchange: String? = null,
    val routingKey: String? = null,
    val properties: Map<String, String> = emptyMap(),
)

/**
 * A sender flow. [endpoint] is the REST path on which RESTAQ accepts requests to be
 * forwarded into [queue].
 */
data class SenderProperties(
    val endpoint: String,
    val queue: QueueEndpointProperties,
)

/**
 * A receiver flow. [endpoint] is the external REST URL to which a message consumed
 * from [queue] is forwarded.
 */
data class ReceiverProperties(
    val endpoint: String,
    val queue: QueueEndpointProperties,
)

package org.pcsoft.micro.restqa.send.controller

import org.pcsoft.micro.restqa.configuration.QueueEndpointProperties
import org.slf4j.LoggerFactory
import org.springframework.amqp.rabbit.core.RabbitTemplate
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.stereotype.Component

/**
 * [MessageQueueClient] backed by a [RabbitTemplate]. The endpoint's
 * [QueueEndpointProperties.exchange] and [QueueEndpointProperties.routingKey] are used
 * for routing; when no routing key is given the queue [QueueEndpointProperties.name] is
 * used (direct-to-queue via the default exchange).
 */
@Component
@ConditionalOnProperty(prefix = "restqa.queue", name = ["type"], havingValue = "amqp", matchIfMissing = true)
class AmqpQueueClient(
    private val rabbitTemplate: RabbitTemplate,
) : MessageQueueClient {

    private val log = LoggerFactory.getLogger(javaClass)

    override fun send(flow: String, endpoint: QueueEndpointProperties, payload: ByteArray) {
        val exchange = endpoint.exchange ?: ""
        val routingKey = endpoint.routingKey ?: endpoint.name
        log.debug(
            "[{}] Publishing {} bytes via AMQP (exchange='{}', routingKey='{}')",
            flow, payload.size, exchange, routingKey,
        )
        rabbitTemplate.convertAndSend(exchange, routingKey, payload)
    }
}

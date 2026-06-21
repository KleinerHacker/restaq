package org.pcsoft.micro.restqa.send.service

import org.pcsoft.micro.restqa.configuration.QueueEndpointProperties
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.jms.core.JmsTemplate
import org.springframework.stereotype.Component

/**
 * [MessageQueueClient] backed by a [JmsTemplate]. Only the queue
 * [QueueEndpointProperties.name] is used as the JMS destination; AMQP-specific fields
 * are ignored.
 */
@Component
@ConditionalOnProperty(prefix = "restqa.queue", name = ["type"], havingValue = "amqp", matchIfMissing = true)
class JmsQueueClient(
    private val jmsTemplate: JmsTemplate,
) : MessageQueueClient {

    private val log = LoggerFactory.getLogger(javaClass)

    override fun send(flow: String, endpoint: QueueEndpointProperties, payload: ByteArray) {
        log.debug(
            "[{}] Publishing {} bytes via JMS (destination='{}')",
            flow, payload.size, endpoint.name,
        )
        jmsTemplate.convertAndSend(endpoint.name, payload)
    }
}

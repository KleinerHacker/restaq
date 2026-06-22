package org.pcsoft.micro.restqa.send.controller

import org.pcsoft.micro.restqa.configuration.QueueEndpointProperties
import org.pcsoft.micro.restqa.internal.utils.logger
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

    companion object {
        private val log = logger()
    }

    override fun send(endpoint: QueueEndpointProperties, payload: ByteArray) {
        log.debug(
            "Publishing {} bytes via JMS (destination='{}')",
            payload.size, endpoint.name,
        )
        jmsTemplate.convertAndSend(endpoint.name, payload)
    }
}

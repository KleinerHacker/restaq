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

    override fun send(endpoint: QueueEndpointProperties, payload: ByteArray, headers: Map<String, String>) {
        log.debug(
            "Publishing {} bytes via JMS (destination='{}', headers={})",
            payload.size, endpoint.name, headers.size,
        )
        jmsTemplate.convertAndSend(endpoint.name, payload) { message ->
            // JMS property names must be valid Java identifiers, so e.g. the '-' in
            // "Content-Type" is mapped to '_' ("Content_Type").
            headers.forEach { (name, value) -> message.setStringProperty(jmsPropertyName(name), value) }
            message
        }
    }

    private fun jmsPropertyName(header: String): String =
        header.map { if (it.isLetterOrDigit() || it == '_') it else '_' }.joinToString("")
}

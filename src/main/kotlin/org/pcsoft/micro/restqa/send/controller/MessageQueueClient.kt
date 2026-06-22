package org.pcsoft.micro.restqa.send.controller

import org.pcsoft.micro.restqa.configuration.QueueEndpointProperties

/**
 * Technology-agnostic abstraction for publishing a message onto a queue.
 *
 * Exactly one implementation is active at runtime, selected globally via
 * `restqa.queue.type` (AMQP/RabbitMQ by default, or JMS/Artemis).
 */
interface MessageQueueClient {
    /**
     * Publish [payload] onto the queue described by [endpoint].
     */
    fun send(endpoint: QueueEndpointProperties, payload: ByteArray)
}

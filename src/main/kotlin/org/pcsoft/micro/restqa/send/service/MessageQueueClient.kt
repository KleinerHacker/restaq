package org.pcsoft.micro.restqa.send.service

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
     *
     * @param flow the configuration map key of the originating flow, used for logging.
     */
    fun send(flow: String, endpoint: QueueEndpointProperties, payload: ByteArray)
}

package org.pcsoft.micro.restqa.send.port

import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import org.pcsoft.micro.restqa.configuration.QueueEndpointProperties
import org.pcsoft.micro.restqa.configuration.SenderProperties
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
import org.springframework.web.reactive.function.server.ServerRequest
import kotlin.test.assertEquals

class SenderEndpointControllerTest {

    private val properties = SenderProperties(
        endpoint = "/api/orders",
        queue = QueueEndpointProperties(name = "orders.queue"),
    )

    @Test
    fun `handle replies 200 OK without doing anything`() {
        val request = mock<ServerRequest> {
            whenever(it.method()).thenReturn(HttpMethod.POST)
        }
        val handler = SenderEndpointController(properties)

        val response = handler.handle(request).block()

        assertEquals(HttpStatus.OK, response?.statusCode())
    }
}

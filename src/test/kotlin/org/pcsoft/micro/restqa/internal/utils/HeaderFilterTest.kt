package org.pcsoft.micro.restqa.internal.utils

import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class HeaderFilterTest {

    @ParameterizedTest
    @ValueSource(
        strings = [
            "Host", "host", "HOST",
            "Connection", "connection",
            "Keep-Alive", "keep-alive",
            "Transfer-Encoding", "transfer-encoding",
            "TE", "te",
            "Trailer", "trailer",
            "Upgrade", "upgrade",
            "Proxy-Authorization", "proxy-authorization",
            "Proxy-Authenticate", "proxy-authenticate",
            "X-Forwarded-Host", "x-forwarded-host",
            "X-Forwarded-Port", "x-forwarded-port",
            "X-Forwarded-Proto", "x-forwarded-proto",
            "X-Forwarded-For", "x-forwarded-for",
            "X-Forwarded-Client-Cert", "x-forwarded-client-cert",
            "X-Real-IP", "x-real-ip",
            "Forwarded", "forwarded",
            "Via", "via",
            "X-SSL-Client-CN", "x-ssl-session-id",
            "X-Client-Cert-DN", "x-client-cert-serial",
        ]
    )
    fun `excluded headers are filtered out`(header: String) {
        assertTrue(HeaderFilter.isExcluded(header), "Expected '$header' to be excluded")
    }

    @ParameterizedTest
    @ValueSource(
        strings = [
            "Content-Type",
            "Authorization",
            "X-Trace-Id",
            "X-Correlation-Id",
            "Accept",
            "X-Custom-Header",
            "content-length",
        ]
    )
    fun `allowed headers pass through`(header: String) {
        assertFalse(HeaderFilter.isExcluded(header), "Expected '$header' to pass through")
    }

    @Test
    fun `filter returns only allowed headers`() {
        val input = mapOf(
            "Content-Type" to "application/json",
            "Host" to "example.com",
            "X-Trace-Id" to "abc-123",
            "Connection" to "keep-alive",
            "X-Forwarded-Client-Cert" to "CERT_DATA",
            "X-SSL-Client-CN" to "client.example.com",
            "Authorization" to "Bearer token",
        )

        val result = HeaderFilter.filter(input)

        assertEquals(
            mapOf(
                "Content-Type" to "application/json",
                "X-Trace-Id" to "abc-123",
                "Authorization" to "Bearer token",
            ),
            result,
        )
    }

    @Test
    fun `filter with empty map returns empty map`() {
        assertEquals(emptyMap(), HeaderFilter.filter(emptyMap()))
    }
}

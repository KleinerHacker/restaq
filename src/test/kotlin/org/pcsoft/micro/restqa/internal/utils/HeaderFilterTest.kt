package org.pcsoft.micro.restqa.internal.utils

import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Verifies the header filtering logic in [HeaderFilter], which strips hop-by-hop,
 * proxy, forwarding, and TLS/transport-related headers before propagating HTTP
 * headers across the messaging boundary. Ensures that application-level headers
 * (Content-Type, Authorization, custom X- headers) pass through while infrastructure
 * headers are excluded regardless of casing.
 */
class HeaderFilterTest {

    /**
     * Verifies that headers belonging to the exclusion list (hop-by-hop transport
     * headers, proxy headers, forwarding headers, and TLS client certificate headers)
     * are correctly identified as excluded. Tests case-insensitive matching to ensure
     * headers are filtered regardless of how upstream systems capitalize them.
     */
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

    /**
     * Verifies that application-level headers that are NOT on the exclusion list
     * pass through the filter. These include content negotiation headers, authentication
     * tokens, tracing identifiers, and custom application headers that must be
     * preserved when forwarding messages across the queue boundary.
     */
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

    /**
     * Verifies the [HeaderFilter.filter] function correctly removes excluded headers
     * from an input map while retaining all allowed headers. The resulting map must
     * contain only application-relevant headers suitable for propagation to the
     * downstream target or queue message properties.
     */
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

    /**
     * Verifies that filtering an empty header map returns an empty map without
     * errors. This edge case ensures the filter handles the absence of headers
     * gracefully, as can happen with minimal HTTP requests.
     */
    @Test
    fun `filter with empty map returns empty map`() {
        assertEquals(emptyMap(), HeaderFilter.filter(emptyMap()))
    }
}

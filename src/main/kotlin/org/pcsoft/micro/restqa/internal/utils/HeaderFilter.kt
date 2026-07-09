package org.pcsoft.micro.restqa.internal.utils

/**
 * Filters HTTP headers before propagation into queue message properties.
 *
 * Excluded categories:
 * - TLS/certificate headers (e.g. `X-Forwarded-Client-Cert`, `X-SSL-*`)
 * - Transport/connection metadata (e.g. `Host`, `Connection`, `Transfer-Encoding`)
 * - URL/path forwarding headers (reconstructed on the receiving side)
 *
 * Header matching is case-insensitive.
 */
object HeaderFilter {

    private val EXCLUDED_EXACT = setOf(
        "host",
        "connection",
        "keep-alive",
        "transfer-encoding",
        "te",
        "trailer",
        "upgrade",
        "proxy-authorization",
        "proxy-authenticate",
        "x-forwarded-host",
        "x-forwarded-port",
        "x-forwarded-proto",
        "x-forwarded-for",
        "x-forwarded-client-cert",
        "x-real-ip",
        "forwarded",
        "via",
    )

    private val EXCLUDED_PREFIXES = listOf(
        "x-ssl-",
        "x-client-cert-",
    )

    /**
     * Returns a filtered copy of [headers] with TLS/transport/path headers removed.
     */
    fun filter(headers: Map<String, String>): Map<String, String> =
        headers.filterKeys { name -> !isExcluded(name) }

    /**
     * Returns `true` if the given header [name] should be excluded from propagation.
     */
    fun isExcluded(name: String): Boolean {
        val lower = name.lowercase()
        if (lower in EXCLUDED_EXACT) return true
        return EXCLUDED_PREFIXES.any { prefix -> lower.startsWith(prefix) }
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import inet.ipaddr.IPAddressString
import org.apache.logging.log4j.LogManager
import java.net.InetAddress
import java.net.URL

object ValidationHelpers {

    private val logger = LogManager.getLogger()

    const val FQDN_REGEX =
        "^(?!.*?_.*?)(?!(?:\\w+?\\.)?-[\\w.\\-]*?)(?!\\w+?-\\.[\\w.\\-]+?)" +
            "(?=\\w)(?=[\\w.\\-]*?\\.+[\\w.\\-]*?)(?![\\w.\\-]{254})(?!(?:" +
            "\\.?[\\w\\-.]*?[\\w\\-]{64,}\\.)+?)[\\w.\\-]+?(?<![\\w\\-.]?\\." +
            "\\d?)(?<=[\\w\\-]{2,})(?<![\\w\\-]{25})\$"

    /**
     * Resolve host to a list of IPAddressString objects.
     * If host is already a literal IP, returns it directly.
     */
    fun getResolvedIps(host: String): List<IPAddressString> {
        try {
            // Try parsing as literal IP first
            val ip = IPAddressString(host)
            if (ip.isValid) return listOf(ip)
        } catch (_: Exception) {
            // ignore, proceed to DNS
        }

        return try {
            InetAddress.getAllByName(host).map { inetAddress ->
                IPAddressString(inetAddress.hostAddress)
            }
        } catch (e: Exception) {
            logger.error("Unable to resolve host ips for $host: ${e.message}")
            emptyList()
        }
    }

    /**
     * Checks if a given URL's host or resolved IPs are in the deny list.
     */
    fun isHostInDenylist(urlString: String, hostDenyList: List<String>): Boolean {
        val denyNetworks = hostDenyList.map { IPAddressString(it) }

        val hostIps: List<IPAddressString> = try {
            val literal = IPAddressString(urlString)
            if (literal.isValid) {
                listOf(literal) // treat urlString itself as literal IP
            } else {
                val url = URL(urlString)
                val host = url.host ?: return false
                getResolvedIps(host)
            }
        } catch (e: Exception) {
            // If parsing as URL fails, attempt resolving as host literal
            getResolvedIps(urlString)
        }

        for (ip in hostIps) {
            for (deny in denyNetworks) {
                if ((ip.isZero && deny.isZero) || deny.contains(ip)) {
                    LogManager.getLogger()
                        .error("$urlString is   denied by rule $deny (matched $ip)")
                    return true
                }
            }
        }

        return false
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchException
import org.opensearch.OpenSearchSecurityException
import org.opensearch.OpenSearchStatusException
import org.opensearch.common.Strings
import org.opensearch.index.IndexNotFoundException
import org.opensearch.index.engine.VersionConflictEngineException
import org.opensearch.indices.InvalidIndexNameException
import org.opensearch.rest.RestStatus

private val log = LogManager.getLogger(AlertingException::class.java)

/**
 * Converts into a user friendly message.
 */
class AlertingException(message: String, val status: RestStatus, ex: Exception) : OpenSearchException(message, ex) {

    override fun status(): RestStatus {
        return status
    }

    companion object {
        @JvmStatic
        fun wrap(ex: Exception): OpenSearchException {
            log.error("Alerting error: $ex")

            var friendlyMsg = "Unknown error"
            var status = RestStatus.INTERNAL_SERVER_ERROR
            when (ex) {
                is IndexNotFoundException -> {
                    status = ex.status()
                    friendlyMsg = "Configured indices are not found: ${ex.index}"
                }
                is OpenSearchSecurityException -> {
                    status = ex.status()
                    friendlyMsg = "User doesn't have permissions to execute this action. Contact administrator."
                }
                is OpenSearchStatusException -> {
                    status = ex.status()
                    friendlyMsg = ex.message as String
                }
                is IllegalArgumentException -> {
                    status = RestStatus.BAD_REQUEST
                    friendlyMsg = ex.message as String
                }
                is VersionConflictEngineException -> {
                    status = ex.status()
                    friendlyMsg = ex.message as String
                }
                is InvalidIndexNameException -> {
                    status = RestStatus.BAD_REQUEST
                    friendlyMsg = ex.message as String
                }
                else -> {
                    if (!Strings.isNullOrEmpty(ex.message)) {
                        friendlyMsg = ex.message as String
                    }
                }
            }
            // Wrapping the origin exception as runtime to avoid it being formatted.
            // Currently, alerting-kibana is using `error.root_cause.reason` as text in the toast message.
            // Below logic is to set friendly message to error.root_cause.reason.
            return AlertingException(friendlyMsg, status, Exception("${ex.javaClass.name}: ${ex.message}"))
        }
    }
}

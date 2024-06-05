/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.get.GetResponse
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.alerting.model.Workflow
import org.opensearch.commons.alerting.util.AlertingException
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.NamedXContentRegistry

private val log = LogManager.getLogger(ScheduledJobUtils::class.java)

class ScheduledJobUtils {
    companion object {
        const val WORKFLOW_DELEGATE_PATH = "workflow.inputs.composite_input.sequence.delegates"
        const val WORKFLOW_MONITOR_PATH = "workflow.inputs.composite_input.sequence.delegates.monitor_id"
        fun parseWorkflowFromScheduledJobDocSource(xContentRegistry: NamedXContentRegistry, response: GetResponse): Workflow {
            XContentHelper.createParser(
                xContentRegistry, LoggingDeprecationHandler.INSTANCE,
                response.sourceAsBytesRef, XContentType.JSON
            ).use { xcp ->
                try {
                    val workflow = ScheduledJob.parse(xcp, response.id, response.version)
                    if (workflow is Workflow) {
                        return workflow
                    } else {
                        log.error("Unable to parse workflow from ${response.source}")
                        throw OpenSearchStatusException(
                            "Unable to parse workflow from ${response.source}",
                            RestStatus.INTERNAL_SERVER_ERROR
                        )
                    }
                } catch (e: java.lang.Exception) {
                    throw AlertingException("Unable to parse workflow from ${response.source}", RestStatus.INTERNAL_SERVER_ERROR, e)
                }
            }
        }

        fun parseMonitorFromScheduledJobDocSource(xContentRegistry: NamedXContentRegistry, response: GetResponse): Monitor {
            XContentHelper.createParser(
                xContentRegistry, LoggingDeprecationHandler.INSTANCE,
                response.sourceAsBytesRef, XContentType.JSON
            ).use { xcp ->
                try {
                    val monitor = ScheduledJob.parse(xcp, response.id, response.version)
                    if (monitor is Monitor) {
                        return monitor
                    } else {
                        log.error("Unable to parse monitor from ${response.source}")
                        throw OpenSearchStatusException(
                            "Unable to parse monitor from ${response.source}",
                            RestStatus.INTERNAL_SERVER_ERROR
                        )
                    }
                } catch (e: java.lang.Exception) {
                    throw AlertingException("Unable to parse monitor from ${response.source}", RestStatus.INTERNAL_SERVER_ERROR, e)
                }
            }
        }
    }
}

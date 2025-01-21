/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.alerting.resthandler

import org.apache.logging.log4j.LogManager
import org.opensearch.action.support.WriteRequest
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.alerts.AlertIndices
import org.opensearch.alerting.util.IF_PRIMARY_TERM
import org.opensearch.alerting.util.IF_SEQ_NO
import org.opensearch.alerting.util.REFRESH
import org.opensearch.client.node.NodeClient
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.action.IndexMonitorRequest
import org.opensearch.commons.alerting.action.IndexMonitorResponse
import org.opensearch.commons.alerting.model.BucketLevelTrigger
import org.opensearch.commons.alerting.model.DocLevelMonitorInput
import org.opensearch.commons.alerting.model.DocumentLevelTrigger
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.QueryLevelTrigger
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.alerting.util.AlertingException
import org.opensearch.commons.alerting.util.isMonitorOfStandardType
import org.opensearch.commons.utils.getInvalidNameChars
import org.opensearch.commons.utils.isValidId
import org.opensearch.commons.utils.isValidName
import org.opensearch.commons.utils.isValidQueryName
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentParser.Token
import org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BaseRestHandler.RestChannelConsumer
import org.opensearch.rest.BytesRestResponse
import org.opensearch.rest.RestChannel
import org.opensearch.rest.RestHandler.ReplacedRoute
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.POST
import org.opensearch.rest.RestRequest.Method.PUT
import org.opensearch.rest.RestResponse
import org.opensearch.rest.action.RestResponseListener
import java.io.IOException
import java.time.Instant
import java.util.*

private val log = LogManager.getLogger(RestIndexMonitorAction::class.java)

/**
 * Rest handlers to create and update monitors.
 */
class RestIndexMonitorAction : BaseRestHandler() {

    override fun getName(): String {
        return "index_monitor_action"
    }

    override fun routes(): List<Route> {
        return listOf()
    }

    override fun replacedRoutes(): MutableList<ReplacedRoute> {
        return mutableListOf(
            ReplacedRoute(
                POST,
                AlertingPlugin.MONITOR_BASE_URI,
                POST,
                AlertingPlugin.LEGACY_OPENDISTRO_MONITOR_BASE_URI
            ),
            ReplacedRoute(
                PUT,
                "${AlertingPlugin.MONITOR_BASE_URI}/{monitorID}",
                PUT,
                "${AlertingPlugin.LEGACY_OPENDISTRO_MONITOR_BASE_URI}/{monitorID}"
            )
        )
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        log.debug("${request.method()} ${AlertingPlugin.MONITOR_BASE_URI}")

        val id = request.param("monitorID", Monitor.NO_ID)
        if (request.method() == PUT && Monitor.NO_ID == id) {
            throw AlertingException.wrap(IllegalArgumentException("Missing monitor ID"))
        }

        // Check if the ID is valid
        if (request.method() == PUT && !isValidId(id)) {
            throw IllegalArgumentException(
                "Invalid monitor ID [$id]. " +
                    "Monitor ID should be alphanumeric string with +, /, _, or - characters only"
            )
        }

        // Validate request by parsing JSON to Monitor
        val xcp = request.contentParser()
        ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp)

        val monitor: Monitor
        val rbacRoles: List<String>?
        try {
            monitor = Monitor.parse(xcp, id).copy(lastUpdateTime = Instant.now())

            // Validate if the monitor name is valid
            if (!isValidName(monitor.name)) {
                throw IllegalArgumentException(
                    "Invalid monitor name [${monitor.name}]. " +
                        "Monitor Name should be alphanumeric (4-50 chars) starting with letter or underscore"
                )
            }

            rbacRoles = request.contentParser().map()["rbac_roles"] as List<String>?

            validateDataSources(monitor)
            val monitorType = monitor.monitorType
            val triggers = monitor.triggers

            if (monitor.isMonitorOfStandardType()) {
                when (Monitor.MonitorType.valueOf(monitor.monitorType.uppercase(Locale.ROOT))) {
                    Monitor.MonitorType.QUERY_LEVEL_MONITOR -> {
                        triggers.forEach {
                            if (it !is QueryLevelTrigger) {
                                throw (IllegalArgumentException("Illegal trigger type, ${it.javaClass.name}, for query level monitor"))
                            }
                            if (!isValidName(it.name)) {
                                throw IllegalArgumentException(
                                    "Invalid trigger name [${it.name}]. " +
                                        "Trigger Name should be alphanumeric (4-50 chars) starting with letter or underscore"
                                )
                            }
                            it.actions.forEach { action ->
                                val destinationId = action.destinationId
                                if (!isValidId(destinationId)) {
                                    throw IllegalArgumentException(
                                        "Invalid destination ID [$destinationId]. " +
                                            "Destination ID should be alphanumeric string with +, /, _, or - characters only"
                                    )
                                }
                            }
                        }
                    }

                    Monitor.MonitorType.BUCKET_LEVEL_MONITOR -> {
                        triggers.forEach {
                            if (it !is BucketLevelTrigger) {
                                throw IllegalArgumentException("Illegal trigger type, ${it.javaClass.name}, for bucket level monitor")
                            }
                            if (!isValidName(it.name)) {
                                throw IllegalArgumentException(
                                    "Invalid trigger name [${it.name}]. " +
                                        "Trigger Name should be alphanumeric (4-50 chars) starting with letter or underscore"
                                )
                            }
                            it.actions.forEach { action ->
                                val destinationId = action.destinationId
                                if (!isValidId(destinationId)) {
                                    throw IllegalArgumentException(
                                        "Invalid destination ID [$destinationId]. " +
                                            "Destination ID should be alphanumeric string with +, /, _, or - characters only"
                                    )
                                }
                            }
                        }
                    }

                    Monitor.MonitorType.CLUSTER_METRICS_MONITOR -> {
                        triggers.forEach {
                            if (it !is QueryLevelTrigger) {
                                throw IllegalArgumentException("Illegal trigger type, ${it.javaClass.name}, for cluster metrics monitor")
                            }
                            if (!isValidName(it.name)) {
                                throw IllegalArgumentException(
                                    "Invalid trigger name [${it.name}]. " +
                                        "Trigger Name should be alphanumeric (4-50 chars) starting with letter or underscore"
                                )
                            }
                            it.actions.forEach { action ->
                                val destinationId = action.destinationId
                                if (!isValidId(destinationId)) {
                                    throw IllegalArgumentException(
                                        "Invalid destination ID [$destinationId]. " +
                                            "Destination ID should be alphanumeric string with +, /, _, or - characters only"
                                    )
                                }
                            }
                        }
                    }

                    Monitor.MonitorType.DOC_LEVEL_MONITOR -> {
                        validateDocLevelQueryName(monitor)
                        triggers.forEach {
                            if (it !is DocumentLevelTrigger) {
                                throw IllegalArgumentException("Illegal trigger type, ${it.javaClass.name}, for document level monitor")
                            }
                            if (!isValidName(it.name)) {
                                throw IllegalArgumentException(
                                    "Invalid trigger name [${it.name}]. " +
                                        "Trigger Name should be alphanumeric (4-50 chars) starting with letter or underscore"
                                )
                            }
                            it.actions.forEach { action ->
                                val destinationId = action.destinationId
                                if (!isValidId(destinationId)) {
                                    throw IllegalArgumentException(
                                        "Invalid destination ID [$destinationId]. " +
                                            "Destination ID should be alphanumeric string with +, /, _, or - characters only"
                                    )
                                }
                            }
                        }
                    }
                }
            }
        } catch (e: Exception) {
            throw AlertingException.wrap(e)
        }

        val seqNo = request.paramAsLong(IF_SEQ_NO, SequenceNumbers.UNASSIGNED_SEQ_NO)
        val primaryTerm = request.paramAsLong(IF_PRIMARY_TERM, SequenceNumbers.UNASSIGNED_PRIMARY_TERM)
        val refreshPolicy = if (request.hasParam(REFRESH)) {
            WriteRequest.RefreshPolicy.parse(request.param(REFRESH))
        } else {
            WriteRequest.RefreshPolicy.IMMEDIATE
        }
        val indexMonitorRequest = IndexMonitorRequest(id, seqNo, primaryTerm, refreshPolicy, request.method(), monitor, rbacRoles)

        return RestChannelConsumer { channel ->
            client.execute(AlertingActions.INDEX_MONITOR_ACTION_TYPE, indexMonitorRequest, indexMonitorResponse(channel, request.method()))
        }
    }

    private fun validateDocLevelQueryName(monitor: Monitor) {
        monitor.inputs.filterIsInstance<DocLevelMonitorInput>().forEach { docLevelMonitorInput ->
            docLevelMonitorInput.queries.forEach { dlq ->
                if (!isValidQueryName(dlq.name)) {
                    throw IllegalArgumentException(
                        "Doc level query name may not start with [_, +, -], contain '..', or contain: " +
                            getInvalidNameChars().replace("\\", "")
                    )
                }
            }
        }
    }

    private fun validateDataSources(monitor: Monitor) { // Data Sources will currently be supported only at transport layer.
        if (monitor.dataSources != null) {
            if (
                monitor.dataSources.queryIndex != ScheduledJob.DOC_LEVEL_QUERIES_INDEX ||
                monitor.dataSources.findingsIndex != AlertIndices.FINDING_HISTORY_WRITE_INDEX ||
                monitor.dataSources.alertsIndex != AlertIndices.ALERT_INDEX
            ) {
                throw IllegalArgumentException("Custom Data Sources are not allowed.")
            }
        }
    }

    private fun indexMonitorResponse(channel: RestChannel, restMethod: RestRequest.Method):
        RestResponseListener<IndexMonitorResponse> {
        return object : RestResponseListener<IndexMonitorResponse>(channel) {
            @Throws(Exception::class)
            override fun buildResponse(response: IndexMonitorResponse): RestResponse {
                var returnStatus = RestStatus.CREATED
                if (restMethod == RestRequest.Method.PUT)
                    returnStatus = RestStatus.OK

                val restResponse = BytesRestResponse(returnStatus, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS))
                if (returnStatus == RestStatus.CREATED) {
                    val location = "${AlertingPlugin.MONITOR_BASE_URI}/${response.id}"
                    restResponse.addHeader("Location", location)
                }
                return restResponse
            }
        }
    }
}

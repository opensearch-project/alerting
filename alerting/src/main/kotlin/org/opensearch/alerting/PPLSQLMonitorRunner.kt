/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.apache.logging.log4j.LogManager
import org.json.JSONArray
import org.json.JSONObject
import org.opensearch.ExceptionsHelper
import org.opensearch.action.DocWriteRequest
import org.opensearch.action.bulk.BackoffPolicy
import org.opensearch.action.bulk.BulkRequest
import org.opensearch.action.bulk.BulkResponse
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.support.WriteRequest
import org.opensearch.alerting.PPLUtils.appendCustomCondition
import org.opensearch.alerting.PPLUtils.appendDataRowsLimit
import org.opensearch.alerting.PPLUtils.capPPLQueryResultsSize
import org.opensearch.alerting.PPLUtils.executePplQuery
import org.opensearch.alerting.PPLUtils.findEvalResultVar
import org.opensearch.alerting.PPLUtils.findEvalResultVarIdxInSchema
import org.opensearch.alerting.alertsv2.AlertV2Indices
import org.opensearch.alerting.modelv2.AlertV2
import org.opensearch.alerting.modelv2.MonitorV2
import org.opensearch.alerting.modelv2.MonitorV2RunResult
import org.opensearch.alerting.modelv2.PPLSQLMonitor
import org.opensearch.alerting.modelv2.PPLSQLMonitorRunResult
import org.opensearch.alerting.modelv2.PPLSQLTrigger
import org.opensearch.alerting.modelv2.PPLSQLTrigger.ConditionType
import org.opensearch.alerting.modelv2.PPLSQLTrigger.NumResultsCondition
import org.opensearch.alerting.modelv2.PPLSQLTrigger.TriggerMode
import org.opensearch.alerting.modelv2.PPLSQLTriggerRunResult
import org.opensearch.alerting.modelv2.TriggerV2.Severity
import org.opensearch.alerting.opensearchapi.InjectorContextElement
import org.opensearch.alerting.opensearchapi.retry
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.opensearchapi.withClosableContext
import org.opensearch.alerting.script.PPLTriggerExecutionContext
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.commons.alerting.alerts.AlertError
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.commons.alerting.model.ScheduledJob.Companion.SCHEDULED_JOBS_INDEX
import org.opensearch.commons.alerting.model.action.Action
import org.opensearch.commons.alerting.model.userErrorMessage
import org.opensearch.core.common.Strings
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.node.NodeClient
import java.time.Instant
import java.time.ZoneOffset.UTC
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.Locale
import kotlin.math.min

object PPLSQLMonitorRunner : MonitorV2Runner() {
    private val logger = LogManager.getLogger(javaClass)

    override suspend fun runMonitorV2(
        monitorV2: MonitorV2,
        monitorCtx: MonitorRunnerExecutionContext, // MonitorV2 reads from same context as Monitor
        periodEnd: Instant,
        dryRun: Boolean,
        manual: Boolean,
        executionId: String,
        transportService: TransportService,
    ): MonitorV2RunResult<*> {
        if (monitorV2 !is PPLSQLMonitor) {
            throw IllegalStateException("Unexpected monitor type: ${monitorV2.javaClass.name}")
        }

        if (monitorV2.id == MonitorV2.NO_ID) {
            throw IllegalStateException("Received PPL Monitor to execute that unexpectedly has no ID")
        }

        logger.debug("Running PPL Monitor: ${monitorV2.name}. Thread: ${Thread.currentThread().name}")

        val pplSqlMonitor = monitorV2
        val nodeClient = monitorCtx.client as NodeClient

        // create some objects that will be used later
        val triggerResults = mutableMapOf<String, PPLSQLTriggerRunResult>()
        val pplSqlQueryResults = mutableMapOf<String, Map<String, Any>>()

        // set the current execution time
        // use threadpool time for cross node consistency
        val timeOfCurrentExecution = Instant.ofEpochMilli(MonitorRunnerService.monitorCtx.threadPool!!.absoluteTimeInMillis())

        // check for and create the active alerts and alert history indices
        // so we have indices to write alerts to
        try {
            monitorCtx.alertV2Indices!!.createOrUpdateAlertV2Index()
            monitorCtx.alertV2Indices!!.createOrUpdateInitialAlertV2HistoryIndex()
        } catch (e: Exception) {
            val id = if (pplSqlMonitor.id.trim().isEmpty()) "_na_" else pplSqlMonitor.id
            logger.error("Error loading alerts for monitorV2: $id", e)
            return PPLSQLMonitorRunResult(pplSqlMonitor.name, e, mapOf(), mapOf())
        }

        val timeFilteredQuery = if (pplSqlMonitor.lookBackWindow != null) {
            // if lookback window is specified, inject a top level lookback window time filter
            // into the PPL query
            val lookBackWindow = pplSqlMonitor.lookBackWindow!!
            val lookbackPeriodStart = periodEnd.minus(lookBackWindow, ChronoUnit.MINUTES)
            val timeFilteredQuery = addTimeFilter(pplSqlMonitor.query, lookbackPeriodStart, periodEnd, pplSqlMonitor.timestampField!!)
            logger.debug("time filtered query: $timeFilteredQuery")
            timeFilteredQuery
        } else {
            logger.debug("look back window not specified, proceeding with query: ${pplSqlMonitor.query}")
            // otherwise, don't inject any time filter whatsoever
            // unless the query itself has user-specified time filters, this query
            // will return all applicable data in the cluster
            pplSqlMonitor.query
        }

        // run each trigger
        for (pplSqlTrigger in pplSqlMonitor.triggers) {
            try {
                // check for throttle and skip execution
                // before even running the trigger itself
                val throttled = checkForThrottle(pplSqlTrigger, timeOfCurrentExecution, manual)
                if (throttled) {
                    logger.debug("throttling trigger ${pplSqlTrigger.name} from monitor ${pplSqlMonitor.name}")

                    // automatically return that this trigger is untriggered
                    triggerResults[pplSqlTrigger.id] = PPLSQLTriggerRunResult(pplSqlTrigger.name, false, null)

                    continue
                }
                logger.debug("throttle check passed, executing trigger ${pplSqlTrigger.name} from monitor ${pplSqlMonitor.name}")

                // if trigger uses custom condition, append the custom condition to query, otherwise simply proceed
                val queryToExecute = if (pplSqlTrigger.conditionType == ConditionType.NUMBER_OF_RESULTS) { // number of results trigger
                    timeFilteredQuery
                } else { // custom condition trigger
                    appendCustomCondition(timeFilteredQuery, pplSqlTrigger.customCondition!!)
                }

                // limit the number of PPL query result data rows returned
                val dataRowsLimit = monitorCtx.clusterService!!.clusterSettings.get(AlertingSettings.ALERTING_V2_QUERY_RESULTS_MAX_DATAROWS)
                val limitedQueryToExecute = appendDataRowsLimit(queryToExecute, dataRowsLimit)

                // TODO: after getting ppl query results, see if the number of results
                // retrieved equals the max allowed number of query results. this implies
                // query results might have been excluded, in which case a warning message
                // in the alert and notification must be added that results were excluded
                // and an alert that should have been generated might not have been

                // execute the PPL query
                val queryResponseJson = withClosableContext(
                    InjectorContextElement(
                        pplSqlMonitor.id,
                        monitorCtx.settings!!,
                        monitorCtx.threadPool!!.threadContext,
                        pplSqlMonitor.user?.roles,
                        pplSqlMonitor.user
                    )
                ) {
                    executePplQuery(limitedQueryToExecute, nodeClient)
                }
                logger.debug("query results for trigger ${pplSqlTrigger.name}: $queryResponseJson")

                // store the query results for Execute Monitor API response
                // unlike the query results stored in alerts and notifications, which must be size capped
                // (because they will be stored in the OpenSearch cluster or sent as notification) and must be based
                // on only the query results that met the trigger condition (because alerts should generate
                // on query results that met trigger condition, not those that didn't), the pplQueryResults
                // here will be returned as part of the Execute Monitor API response. This will return the original,
                // untouched set of query results, and whether this causes size exceed errors is deferred
                // to HTTP's response size limits
                pplSqlQueryResults[pplSqlTrigger.id] = queryResponseJson.toMap()

                // determine if the trigger condition has been met
                val triggered = if (pplSqlTrigger.conditionType == ConditionType.NUMBER_OF_RESULTS) { // number of results trigger
                    evaluateNumResultsTrigger(queryResponseJson, pplSqlTrigger.numResultsCondition!!, pplSqlTrigger.numResultsValue!!)
                } else { // custom condition trigger
                    evaluateCustomTrigger(queryResponseJson, pplSqlTrigger.customCondition!!)
                }

                logger.debug("PPLTrigger ${pplSqlTrigger.name} triggered: $triggered")

                // store the trigger execution results for Execute Monitor API response
                triggerResults[pplSqlTrigger.id] = PPLSQLTriggerRunResult(pplSqlTrigger.name, triggered, null)

                if (triggered) {
                    // retrieve some limits from settings
                    val maxQueryResultsSize =
                        monitorCtx.clusterService!!.clusterSettings.get(AlertingSettings.ALERT_V2_QUERY_RESULTS_MAX_SIZE)
                    val maxAlerts =
                        monitorCtx.clusterService!!.clusterSettings.get(AlertingSettings.ALERT_V2_PER_RESULT_TRIGGER_MAX_ALERTS)

                    // if trigger is on result set mode, this list will have exactly 1 element
                    // if trigger is on per result mode, this list will have as many elements as the query results had
                    // trigger condition-meeting rows, up to the max number of alerts a per result trigger can generate
                    val preparedQueryResults = splitUpQueryResults(pplSqlTrigger, queryResponseJson, maxQueryResultsSize, maxAlerts)

                    // generate alerts based on trigger mode
                    // if this trigger is on result_set mode, this list contains exactly 1 alert
                    // if this trigger is on per_result mode, this list has as many alerts as there are
                    // trigger condition-meeting query results
                    val thisTriggersGeneratedAlerts = generateAlerts(
                        pplSqlTrigger,
                        pplSqlMonitor,
                        preparedQueryResults,
                        executionId,
                        timeOfCurrentExecution
                    )

                    // update the trigger's last execution time for future throttle checks
                    pplSqlTrigger.lastTriggeredTime = timeOfCurrentExecution

                    // send alert notifications
                    for (action in pplSqlTrigger.actions) {
                        for (queryResult in preparedQueryResults) {
                            val pplTriggerExecutionContext = PPLTriggerExecutionContext(
                                pplSqlMonitor,
                                null,
                                pplSqlTrigger,
                                queryResult
                            )

                            runAction(
                                action,
                                pplTriggerExecutionContext,
                                monitorCtx,
                                pplSqlMonitor,
                                dryRun
                            )
                        }
                    }

                    // write the alerts to the alerts index
                    monitorCtx.retryPolicy?.let {
                        saveAlertsV2(thisTriggersGeneratedAlerts, pplSqlMonitor, it, nodeClient)
                    }
                }
            } catch (e: Exception) {
                logger.error("failed to run PPL Trigger ${pplSqlTrigger.name} from PPL Monitor ${pplSqlMonitor.name}", e)

                // generate an alert with an error message
                monitorCtx.retryPolicy?.let {
                    saveAlertsV2(
                        generateErrorAlert(pplSqlTrigger, pplSqlMonitor, e, executionId, timeOfCurrentExecution),
                        pplSqlMonitor,
                        it,
                        nodeClient
                    )
                }

                continue
            }
        }

        // for throttle checking purposes, reindex the PPL Monitor into the alerting-config index
        // with updated last triggered times for each of its triggers
        if (triggerResults.any { it.value.triggered }) {
            updateMonitorWithLastTriggeredTimes(pplSqlMonitor, nodeClient)
        }

        return PPLSQLMonitorRunResult(
            pplSqlMonitor.name,
            null,
            triggerResults,
            pplSqlQueryResults
        )
    }

    // returns true if the pplTrigger should be throttled
    private fun checkForThrottle(pplTrigger: PPLSQLTrigger, timeOfCurrentExecution: Instant, manual: Boolean): Boolean {
        // manual calls from the user to execute a monitor should never be throttled
        if (manual) {
            return false
        }

        // the interval between throttledTimeBound and now is the throttle window
        // i.e. any PPLTrigger whose last trigger time is in this window must be throttled
        val throttleTimeBound = pplTrigger.throttleDuration?.let {
            timeOfCurrentExecution.minus(pplTrigger.throttleDuration, ChronoUnit.MINUTES)
        }

        // the trigger must be throttled if...
        return pplTrigger.throttleDuration != null && // throttling is enabled on the PPLTrigger
            pplTrigger.lastTriggeredTime != null && // and it has triggered before at least once
            pplTrigger.lastTriggeredTime!!.isAfter(throttleTimeBound!!) // and it's not yet out of its throttle window
    }

    // adds monitor schedule-based time filter
    // query: the raw PPL Monitor query
    // lookbackPeriodStart: the lower bound of the query interval based on monitor schedule and look back window
    // periodEnd: the upper bound of the initially computed query interval based on monitor schedule
    // timestampField: the timestamp field that will be used to time bound the query results
    private fun addTimeFilter(query: String, lookbackPeriodStart: Instant, periodEnd: Instant, timestampField: String): String {
        // PPL plugin only accepts timestamp strings in this format
        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.ROOT).withZone(UTC)

        val periodStartPplTimestamp = formatter.format(lookbackPeriodStart)
        val periodEndPplTimeStamp = formatter.format(periodEnd)

        val timeFilterAppend = "| where $timestampField > TIMESTAMP('$periodStartPplTimestamp') and " +
            "$timestampField < TIMESTAMP('$periodEndPplTimeStamp')"
        val timeFilterReplace = "$timeFilterAppend |"

        val timeFilteredQuery: String = if (query.contains("|")) {
            // if Monitor query contains piped statements, inject the time filter
            // as the first piped statement (i.e. before more complex statements
            // like aggregations can take effect later in the query)
            query.replaceFirst("|", timeFilterReplace)
        } else {
            // otherwise the query contains no piped statements and is simply a
            // `search source=<index>` statement, simply append time filter at the end
            query + timeFilterAppend
        }

        return timeFilteredQuery
    }

    private fun evaluateNumResultsTrigger(
        pplQueryResponse: JSONObject,
        numResultsCondition: NumResultsCondition,
        numResultsValue: Long
    ): Boolean {
        val numResults = pplQueryResponse.getLong("total")
        return when (numResultsCondition) {
            NumResultsCondition.GREATER_THAN -> numResults > numResultsValue
            NumResultsCondition.GREATER_THAN_EQUAL -> numResults >= numResultsValue
            NumResultsCondition.LESS_THAN -> numResults < numResultsValue
            NumResultsCondition.LESS_THAN_EQUAL -> numResults <= numResultsValue
            NumResultsCondition.EQUAL -> numResults == numResultsValue
            NumResultsCondition.NOT_EQUAL -> numResults != numResultsValue
        }
    }

    private fun evaluateCustomTrigger(pplQueryResponse: JSONObject, customCondition: String): Boolean {
        // find the name of the eval result variable defined in custom condition
        val evalResultVarName = findEvalResultVar(customCondition)

        // find the index eval statement result variable in the PPL query response schema
        val evalResultVarIdx = findEvalResultVarIdxInSchema(pplQueryResponse, evalResultVarName)

        val dataRowList = pplQueryResponse.getJSONArray("datarows")
        for (i in 0 until dataRowList.length()) {
            val dataRow = dataRowList.getJSONArray(i)
            val evalResult = dataRow.getBoolean(evalResultVarIdx)
            if (evalResult) {
                return true
            }
        }

        return false
    }

    // prepares the query results to be passed into alerts and notifications based on trigger mode
    // if result set, alert and notification simply stores all query results.
    // if per result, each alert and notification stores a single row of the query results.
    // this function then ensures that only a capped number of results are returned to generate alerts
    // and notifications based on. it also caps the size of the query results themselves.
    private fun splitUpQueryResults(
        pplTrigger: PPLSQLTrigger,
        pplQueryResults: JSONObject,
        maxQueryResultsSize: Long,
        maxAlerts: Int
    ): List<JSONObject> {
        // case: result set
        // return the results as a single set of all the results
        if (pplTrigger.mode == TriggerMode.RESULT_SET) {
            val sizeCappedRelevantQueryResultRows = capPPLQueryResultsSize(pplQueryResults, maxQueryResultsSize)
            return listOf(sizeCappedRelevantQueryResultRows)
        }

        // case: per result
        // prepare to generate an alert for each relevant query result row,
        // up to the maxAlerts limit
        val individualRows = mutableListOf<JSONObject>()
        if (pplTrigger.conditionType == ConditionType.NUMBER_OF_RESULTS) {
            // nested case: number_of_results
            val numAlertsToGenerate = min(maxAlerts, pplQueryResults.getInt("total"))
            for (i in 0 until numAlertsToGenerate) {
                addRowToList(individualRows, pplQueryResults, i, maxQueryResultsSize)
            }
        } else {
            // nested case: custom
            val evalResultVarName = findEvalResultVar(pplTrigger.customCondition!!)
            val evalResultVarIdx = findEvalResultVarIdxInSchema(pplQueryResults, evalResultVarName)
            val dataRowList = pplQueryResults.getJSONArray("datarows")
            for (i in 0 until dataRowList.length()) {
                val dataRow = dataRowList.getJSONArray(i)
                val evalResult = dataRow.getBoolean(evalResultVarIdx)
                if (evalResult) {
                    addRowToList(individualRows, pplQueryResults, i, maxQueryResultsSize)
                }
                if (individualRows.size >= maxAlerts) {
                    break
                }
            }
        }

        logger.debug("individualRows: $individualRows")

        return individualRows
    }

    private fun addRowToList(
        individualRows: MutableList<JSONObject>,
        pplQueryResults: JSONObject,
        i: Int,
        maxQueryResultsSize: Long
    ) {
        val individualRow = JSONObject()
        individualRow.put("total", 1) // set the size explicitly to 1 for consistency
        individualRow.put("size", 1)
        individualRow.put("schema", JSONArray(pplQueryResults.getJSONArray("schema").toList()))
        individualRow.put(
            "datarows",
            JSONArray().put(
                JSONArray(pplQueryResults.getJSONArray("datarows").getJSONArray(i).toList())
            )
        )
        val sizeCappedIndividualRow = capPPLQueryResultsSize(individualRow, maxQueryResultsSize)
        individualRows.add(sizeCappedIndividualRow)
    }

    private fun generateAlerts(
        pplSqlTrigger: PPLSQLTrigger,
        pplSqlMonitor: PPLSQLMonitor,
        preparedQueryResults: List<JSONObject>,
        executionId: String,
        timeOfCurrentExecution: Instant
    ): List<AlertV2> {
        val alertV2s = mutableListOf<AlertV2>()
        for (queryResult in preparedQueryResults) {
            val alertV2 = AlertV2(
                monitorId = pplSqlMonitor.id,
                monitorName = pplSqlMonitor.name,
                monitorVersion = pplSqlMonitor.version,
                monitorUser = pplSqlMonitor.user,
                triggerId = pplSqlTrigger.id,
                triggerName = pplSqlTrigger.name,
                query = pplSqlMonitor.query,
                queryResults = queryResult.toMap(),
                triggeredTime = timeOfCurrentExecution,
                severity = pplSqlTrigger.severity,
                executionId = executionId
            )
            alertV2s.add(alertV2)
        }

        return alertV2s.toList() // return as immutable list
    }

    private fun generateErrorAlert(
        pplSqlTrigger: PPLSQLTrigger,
        pplSqlMonitor: PPLSQLMonitor,
        exception: Exception,
        executionId: String,
        timeOfCurrentExecution: Instant
    ): List<AlertV2> {
        val errorMessage = "Failed to run PPL Trigger ${pplSqlTrigger.name} from PPL Monitor ${pplSqlMonitor.name}: " +
            exception.userErrorMessage()
        val obfuscatedErrorMessage = AlertError.obfuscateIPAddresses(errorMessage)

        val alertV2 = AlertV2(
            monitorId = pplSqlMonitor.id,
            monitorName = pplSqlMonitor.name,
            monitorVersion = pplSqlMonitor.version,
            monitorUser = pplSqlMonitor.user,
            triggerId = pplSqlTrigger.id,
            triggerName = pplSqlTrigger.name,
            query = pplSqlMonitor.query,
            queryResults = mapOf(),
            triggeredTime = timeOfCurrentExecution,
            errorMessage = obfuscatedErrorMessage,
            severity = Severity.ERROR,
            executionId = executionId
        )

        return listOf(alertV2)
    }

    private suspend fun saveAlertsV2(
        alerts: List<AlertV2>,
        pplSqlMonitor: PPLSQLMonitor,
        retryPolicy: BackoffPolicy,
        client: NodeClient
    ) {
        logger.debug("received alerts: $alerts")

        var requestsToRetry = alerts.flatMap { alert ->
            listOf<DocWriteRequest<*>>(
                IndexRequest(AlertV2Indices.ALERT_V2_INDEX)
                    .routing(pplSqlMonitor.id) // set routing ID to PPL Monitor ID
                    .source(alert.toXContentWithUser(XContentFactory.jsonBuilder()))
                    .id(if (alert.id != Alert.NO_ID) alert.id else null)
            )
        }

        if (requestsToRetry.isEmpty()) return
        // Retry Bulk requests if there was any 429 response
        retryPolicy.retry(logger, listOf(RestStatus.TOO_MANY_REQUESTS)) {
            val bulkRequest = BulkRequest().add(requestsToRetry).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            val bulkResponse: BulkResponse = client.suspendUntil { client.bulk(bulkRequest, it) }
            val failedResponses = (bulkResponse.items ?: arrayOf()).filter { it.isFailed }
            failedResponses.forEach {
                logger.debug("write alerts failed responses: ${it.failureMessage}")
            }
            requestsToRetry = failedResponses.filter { it.status() == RestStatus.TOO_MANY_REQUESTS }
                .map { bulkRequest.requests()[it.itemId] as IndexRequest }

            if (requestsToRetry.isNotEmpty()) {
                val retryCause = failedResponses.first { it.status() == RestStatus.TOO_MANY_REQUESTS }.failure.cause
                throw ExceptionsHelper.convertToOpenSearchException(retryCause)
            }
        }
    }

    private suspend fun updateMonitorWithLastTriggeredTimes(pplSqlMonitor: PPLSQLMonitor, client: NodeClient) {
        val indexRequest = IndexRequest(SCHEDULED_JOBS_INDEX)
            .id(pplSqlMonitor.id)
            .source(
                pplSqlMonitor.toXContentWithUser(
                    XContentFactory.jsonBuilder(),
                    ToXContent.MapParams(
                        mapOf("with_type" to "true")
                    )
                )
            )
            .routing(pplSqlMonitor.id)
        val indexResponse = client.suspendUntil { index(indexRequest, it) }

        logger.debug("PPLSQLMonitor update with last execution times index response: ${indexResponse.result}")
    }

    suspend fun runAction(
        action: Action,
        triggerCtx: PPLTriggerExecutionContext,
        monitorCtx: MonitorRunnerExecutionContext,
        pplSqlMonitor: PPLSQLMonitor,
        dryrun: Boolean
    ) {
        // this function can throw an exception, which is caught by the try
        // catch in runMonitor() to generate an error alert

        val notifSubject = if (action.subjectTemplate != null)
            MonitorRunnerService.compileTemplateV2(action.subjectTemplate!!, triggerCtx)
        else ""

        var notifMessage = MonitorRunnerService.compileTemplateV2(action.messageTemplate, triggerCtx)
        if (Strings.isNullOrEmpty(notifMessage)) {
            throw IllegalStateException("Message content missing in the Destination with id: ${action.destinationId}")
        }

        if (!dryrun) {
            monitorCtx.client!!.threadPool().threadContext.stashContext().use {
                withClosableContext(
                    InjectorContextElement(
                        pplSqlMonitor.id,
                        monitorCtx.settings!!,
                        monitorCtx.threadPool!!.threadContext,
                        pplSqlMonitor.user?.roles,
                        pplSqlMonitor.user
                    )
                ) {
                    getConfigAndSendNotification(
                        action,
                        monitorCtx,
                        notifSubject,
                        notifMessage
                    )
                }
            }
        }
    }
}

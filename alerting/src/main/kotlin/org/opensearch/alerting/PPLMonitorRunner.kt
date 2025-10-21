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
import org.opensearch.alerting.AlertingV2Utils.capPplQueryResultsSize
import org.opensearch.alerting.QueryLevelMonitorRunner.getConfigAndSendNotification
import org.opensearch.alerting.alertsv2.AlertV2Indices
import org.opensearch.alerting.core.ppl.PPLPluginInterface
import org.opensearch.alerting.modelv2.AlertV2
import org.opensearch.alerting.modelv2.MonitorV2
import org.opensearch.alerting.modelv2.MonitorV2RunResult
import org.opensearch.alerting.modelv2.PPLMonitor
import org.opensearch.alerting.modelv2.PPLMonitorRunResult
import org.opensearch.alerting.modelv2.PPLTrigger
import org.opensearch.alerting.modelv2.PPLTrigger.ConditionType
import org.opensearch.alerting.modelv2.PPLTrigger.NumResultsCondition
import org.opensearch.alerting.modelv2.PPLTrigger.TriggerMode
import org.opensearch.alerting.modelv2.PPLTriggerRunResult
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
import org.opensearch.sql.plugin.transport.TransportPPLQueryRequest
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.node.NodeClient
import java.time.Instant
import java.time.ZoneOffset.UTC
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.Locale

object PPLMonitorRunner : MonitorV2Runner {
    private val logger = LogManager.getLogger(javaClass)

    private const val PPL_SQL_QUERY_FIELD = "query" // name of PPL query field when passing into PPL/SQL Execute API call

    override suspend fun runMonitorV2(
        monitorV2: MonitorV2,
        monitorCtx: MonitorRunnerExecutionContext, // MonitorV2 reads from same context as Monitor
        periodStart: Instant,
        periodEnd: Instant,
        dryRun: Boolean,
        manual: Boolean,
        executionId: String,
        transportService: TransportService,
    ): MonitorV2RunResult<*> {
        if (monitorV2 !is PPLMonitor) {
            throw IllegalStateException("Unexpected monitor type: ${monitorV2.javaClass.name}")
        }

        if (monitorV2.id == MonitorV2.NO_ID) {
            throw IllegalStateException("Received PPL Monitor to execute that unexpectedly has no ID")
        }

        if (periodStart == periodEnd) {
            logger.warn("Start and end time are the same: $periodStart. This PPL Monitor will probably only run once.")
        }

        logger.debug("Running PPL Monitor: ${monitorV2.name}. Thread: ${Thread.currentThread().name}")

        val pplMonitor = monitorV2
        val nodeClient = monitorCtx.client as NodeClient

        // create some objects that will be used later
        val triggerResults = mutableMapOf<String, PPLTriggerRunResult>()
        val pplQueryResults = mutableMapOf<String, Map<String, Any>>()

        // set the current execution time
        // use threadpool time for cross node consistency
        val timeOfCurrentExecution = Instant.ofEpochMilli(MonitorRunnerService.monitorCtx.threadPool!!.absoluteTimeInMillis())

        try {
            monitorCtx.alertV2Indices!!.createOrUpdateAlertV2Index()
            monitorCtx.alertV2Indices!!.createOrUpdateInitialAlertV2HistoryIndex()
        } catch (e: Exception) {
            val id = if (pplMonitor.id.trim().isEmpty()) "_na_" else pplMonitor.id
            logger.error("Error loading alerts for monitorV2: $id", e)
            return PPLMonitorRunResult(pplMonitor.name, e, mapOf(), mapOf())
        }

        val timeFilteredQuery = if (pplMonitor.lookBackWindow != null) {
            // if lookback window is specified, inject a top level lookback window time filter
            // into the PPL query
            val lookBackWindow = pplMonitor.lookBackWindow!!
            val lookbackPeriodStart = periodEnd.minus(lookBackWindow, ChronoUnit.MINUTES)
            val timeFilteredQuery = addTimeFilter(pplMonitor.query, lookbackPeriodStart, periodEnd, pplMonitor.timestampField!!)
            logger.info("time filtered query: $timeFilteredQuery")
            timeFilteredQuery
        } else {
            logger.info("look back window not specified, proceeding with query: ${pplMonitor.query}")
            // otherwise, don't inject any time filter whatsoever
            // unless the query itself has user-specified time filters, this query
            // will return all applicable data in the cluster
            pplMonitor.query
        }

        // run each trigger
        for (pplTrigger in pplMonitor.triggers) {
            try {
                // check for throttle and skip execution
                // before even running the trigger itself
                val throttled = checkForThrottle(pplTrigger, timeOfCurrentExecution, manual)
                if (throttled) {
                    logger.info("throttling trigger ${pplTrigger.name} from monitor ${pplMonitor.name}")

                    // automatically return that this trigger is untriggered
                    triggerResults[pplTrigger.id] = PPLTriggerRunResult(pplTrigger.name, false, null)

                    continue
                }
                logger.info("throttle check passed, executing trigger ${pplTrigger.name} from monitor ${pplMonitor.name}")

                // if trigger uses custom condition, append the custom condition to query, otherwise simply proceed
                val queryToExecute = if (pplTrigger.conditionType == ConditionType.NUMBER_OF_RESULTS) { // number of results trigger
                    timeFilteredQuery
                } else { // custom condition trigger
                    appendCustomCondition(timeFilteredQuery, pplTrigger.customCondition!!)
                }

                // limit the number of PPL query result data rows returned
                val dataRowsLimit = monitorCtx.clusterService!!.clusterSettings.get(AlertingSettings.ALERT_V2_QUERY_RESULTS_MAX_DATAROWS)
                val limitedQueryToExecute = appendDataRowsLimit(queryToExecute, dataRowsLimit)

                // execute the PPL query
                val queryResponseJson = withClosableContext(
                    InjectorContextElement(
                        pplMonitor.id,
                        monitorCtx.settings!!,
                        monitorCtx.threadPool!!.threadContext,
                        pplMonitor.user?.roles,
                        pplMonitor.user
                    )
                ) {
                    executePplQuery(limitedQueryToExecute, nodeClient)
                }
                logger.info("query execution results for trigger ${pplTrigger.name}: $queryResponseJson")

                // store the query results for Execute Monitor API response
                // unlike the query results stored in alerts and notifications, which must be size capped
                // (because they will be stored in the OpenSearch cluster or sent as notification) and based
                // on only the query results that met the trigger condition (because alerts should generate
                // on query results that met trigger condition, not those that didn't), the pplQueryResults
                // here will be returned as part of the Execute Monitor API response. This will return the original,
                // untouched set of query results, and whether this causes size exceed errors is deferred
                // to HTTP's response size limits
                pplQueryResults[pplTrigger.id] = queryResponseJson.toMap()

                // retrieve the number of results
                // for number of results triggers, this is simply the number of PPL query results
                // for custom triggers, this is the number of rows in the query response's eval result column that evaluated to true

                logger.info("number of results: ${queryResponseJson.getLong("total")}")

                // determine if the trigger condition has been met
                val triggered = if (pplTrigger.conditionType == ConditionType.NUMBER_OF_RESULTS) { // number of results trigger
                    evaluateNumResultsTrigger(queryResponseJson, pplTrigger.numResultsCondition!!, pplTrigger.numResultsValue!!)
                } else { // custom condition trigger
                    evaluateCustomTrigger(queryResponseJson, pplTrigger.customCondition!!)
                }

                logger.info("PPLTrigger ${pplTrigger.name} triggered: $triggered")

                // store the trigger execution results for Execute Monitor API response
                triggerResults[pplTrigger.id] = PPLTriggerRunResult(pplTrigger.name, triggered, null)

                if (triggered) {
                    // retrieve some limits from settings
                    val maxQueryResultsSize =
                        monitorCtx.clusterService!!.clusterSettings.get(AlertingSettings.ALERT_V2_QUERY_RESULTS_MAX_SIZE)
                    val maxAlerts =
                        monitorCtx.clusterService!!.clusterSettings.get(AlertingSettings.ALERT_V2_PER_RESULT_TRIGGER_MAX_ALERTS)

                    // if trigger is on result set mode, this list will have exactly 1 element
                    // if trigger is on per result mode, this list will have as many elements as the query results had rows
                    // up to the max number of alerts a per result trigger can generate
                    val preparedQueryResults = splitUpQueryResults(pplTrigger, queryResponseJson, maxQueryResultsSize, maxAlerts)

                    // generate alerts based on trigger mode
                    // if this trigger is on result_set mode, this list contains exactly 1 alert
                    // if this trigger is on per_result mode, this list has any alerts as there are relevant query results
                    val thisTriggersGeneratedAlerts = generateAlerts(
                        pplTrigger,
                        pplMonitor,
                        preparedQueryResults,
                        executionId,
                        timeOfCurrentExecution
                    )

                    // update the trigger's last execution time for future throttle checks
                    pplTrigger.lastTriggeredTime = timeOfCurrentExecution

                    // send alert notifications
                    for (action in pplTrigger.actions) {
                        for (queryResult in preparedQueryResults) {
                            val pplTriggerExecutionContext = PPLTriggerExecutionContext(
                                pplMonitor,
                                null,
                                pplTrigger,
                                queryResult
                            )

                            runAction(
                                action,
                                pplTriggerExecutionContext,
                                monitorCtx,
                                pplMonitor,
                                dryRun
                            )
                        }
                    }

                    // write the alerts to the alerts index
                    monitorCtx.retryPolicy?.let {
                        saveAlertsV2(thisTriggersGeneratedAlerts, pplMonitor, it, nodeClient)
                    }
                }
            } catch (e: Exception) {
                logger.error("failed to run PPL Trigger ${pplTrigger.name} from PPL Monitor ${pplMonitor.name}", e)

                // generate an alert with an error message
                monitorCtx.retryPolicy?.let {
                    saveAlertsV2(
                        generateErrorAlert(pplTrigger, pplMonitor, e, executionId, timeOfCurrentExecution),
                        pplMonitor,
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
            updateMonitorWithLastTriggeredTimes(pplMonitor, nodeClient)
        }

        return PPLMonitorRunResult(
            pplMonitor.name,
            null,
            triggerResults,
            pplQueryResults
        )
    }

    // returns true if the pplTrigger should be throttled
    private fun checkForThrottle(pplTrigger: PPLTrigger, timeOfCurrentExecution: Instant, manual: Boolean): Boolean {
        // manual calls from the user to execute a monitor should never be throttled
        if (manual) {
            return false
        }

        // the interval between throttledTimeBound and now is the throttle window
        // i.e. any PPLTrigger whose last trigger time is in this window must be throttled
        val throttleTimeBound = pplTrigger.throttleDuration?.let {
            timeOfCurrentExecution.minus(pplTrigger.throttleDuration!!, ChronoUnit.MINUTES)
        }

        // the trigger must be throttled if...
        return pplTrigger.throttleDuration != null && // throttling is enabled on the PPLTrigger
            pplTrigger.lastTriggeredTime != null && // and it has triggered before at least once
            pplTrigger.lastTriggeredTime!!.isAfter(throttleTimeBound!!) // and it's not yet out of the throttle window
    }

    // adds monitor schedule-based time filter
    // query: the raw PPL Monitor query
    // periodStart: the lower bound of the initially computed query interval based on monitor schedule
    // periodEnd: the upper bound of the initially computed query interval based on monitor schedule
    // lookBackWindow: customer's desired query look back window, overrides [periodStart, periodEnd] if not null
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
        pplTrigger: PPLTrigger,
        pplQueryResults: JSONObject,
        maxQueryResultsSize: Long,
        maxAlerts: Int
    ): List<JSONObject> {
        // case: result set
        // return the results as a single set of all the results
        if (pplTrigger.mode == TriggerMode.RESULT_SET) {
            val sizeCappedRelevantQueryResultRows = capPplQueryResultsSize(pplQueryResults, maxQueryResultsSize)
            return listOf(sizeCappedRelevantQueryResultRows)
        }

        // case: per result
        // prepare to generate an alert for each relevant query result row
        val individualRows = mutableListOf<JSONObject>()
        if (pplTrigger.conditionType == ConditionType.NUMBER_OF_RESULTS) { // nested case: number_of_results
            val numAlertsToGenerate = pplQueryResults.getInt("total")
            for (i in 0 until numAlertsToGenerate) {
                addRowToList(individualRows, pplQueryResults, i, maxQueryResultsSize)
            }
        } else { // nested case: custom
            val evalResultVarName = findEvalResultVar(pplTrigger.customCondition!!)
            val evalResultVarIdx = findEvalResultVarIdxInSchema(pplQueryResults, evalResultVarName)
            val dataRowList = pplQueryResults.getJSONArray("datarows")
            for (i in 0 until dataRowList.length()) {
                val dataRow = dataRowList.getJSONArray(i)
                val evalResult = dataRow.getBoolean(evalResultVarIdx)
                if (evalResult) {
                    addRowToList(individualRows, pplQueryResults, i, maxQueryResultsSize)
                }
            }
        }

        logger.info("individualRows: $individualRows")

        // there may be many query result rows, and generating an alert for each of them could lead to cluster issues,
        // so limit the number of per_result alerts that are generated
        val cappedIndividualRows = individualRows.take(maxAlerts)

        return cappedIndividualRows
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
        val sizeCappedIndividualRow = capPplQueryResultsSize(individualRow, maxQueryResultsSize)
        individualRows.add(sizeCappedIndividualRow)
    }

    private fun generateAlerts(
        pplTrigger: PPLTrigger,
        pplMonitor: PPLMonitor,
        preparedQueryResults: List<JSONObject>,
        executionId: String,
        timeOfCurrentExecution: Instant
    ): List<AlertV2> {
        val expirationTime = timeOfCurrentExecution.plus(pplTrigger.expireDuration, ChronoUnit.MINUTES)

        val alertV2s = mutableListOf<AlertV2>()
        for (queryResult in preparedQueryResults) {
            val alertV2 = AlertV2(
                monitorId = pplMonitor.id,
                monitorName = pplMonitor.name,
                monitorVersion = pplMonitor.version,
                monitorUser = pplMonitor.user,
                triggerId = pplTrigger.id,
                triggerName = pplTrigger.name,
                query = pplMonitor.query,
                queryResults = queryResult.toMap(),
                triggeredTime = timeOfCurrentExecution,
                expirationTime = expirationTime,
                severity = pplTrigger.severity,
                executionId = executionId
            )
            alertV2s.add(alertV2)
        }

        return alertV2s.toList() // return as immutable list
    }

    private fun generateErrorAlert(
        pplTrigger: PPLTrigger,
        pplMonitor: PPLMonitor,
        exception: Exception,
        executionId: String,
        timeOfCurrentExecution: Instant
    ): List<AlertV2> {
        val expirationTime = timeOfCurrentExecution.plus(pplTrigger.expireDuration, ChronoUnit.MILLIS)

        val errorMessage = "Failed to run PPL Trigger ${pplTrigger.name} from PPL Monitor ${pplMonitor.name}: " +
            exception.userErrorMessage()
        val obfuscatedErrorMessage = AlertError.obfuscateIPAddresses(errorMessage)

        val alertV2 = AlertV2(
            monitorId = pplMonitor.id,
            monitorName = pplMonitor.name,
            monitorVersion = pplMonitor.version,
            monitorUser = pplMonitor.user,
            triggerId = pplTrigger.id,
            triggerName = pplTrigger.name,
            query = pplMonitor.query,
            queryResults = mapOf(), // TODO: decouple alerts and notifs errors
            triggeredTime = timeOfCurrentExecution,
            expirationTime = expirationTime,
            errorMessage = obfuscatedErrorMessage,
            severity = Severity.ERROR,
            executionId = executionId
        )

        return listOf(alertV2)
    }

    private suspend fun saveAlertsV2(
        alerts: List<AlertV2>,
        pplMonitor: PPLMonitor,
        retryPolicy: BackoffPolicy,
        client: NodeClient
    ) {
        logger.info("received alerts: $alerts")

        var requestsToRetry = alerts.flatMap { alert ->
            listOf<DocWriteRequest<*>>(
                IndexRequest(AlertV2Indices.ALERT_V2_INDEX)
                    .routing(pplMonitor.id) // set routing ID to PPL Monitor ID
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
                logger.info("write alerts failed responses: ${it.failureMessage}")
            }
            requestsToRetry = failedResponses.filter { it.status() == RestStatus.TOO_MANY_REQUESTS }
                .map { bulkRequest.requests()[it.itemId] as IndexRequest }

            if (requestsToRetry.isNotEmpty()) {
                val retryCause = failedResponses.first { it.status() == RestStatus.TOO_MANY_REQUESTS }.failure.cause
                throw ExceptionsHelper.convertToOpenSearchException(retryCause)
            }
        }
    }

    // TODO: every time this is done, trigger and action IDs change, figure out how to retain IDs
    private suspend fun updateMonitorWithLastTriggeredTimes(pplMonitor: PPLMonitor, client: NodeClient) {
        val indexRequest = IndexRequest(SCHEDULED_JOBS_INDEX)
            .id(pplMonitor.id)
            .source(
                pplMonitor.toXContentWithUser(
                    XContentFactory.jsonBuilder(),
                    ToXContent.MapParams(
                        mapOf("with_type" to "true")
                    )
                )
            )
            .routing(pplMonitor.id)
        val indexResponse = client.suspendUntil { index(indexRequest, it) }

        logger.info("PPLMonitor update with last execution times index response: ${indexResponse.result}")
    }

    suspend fun runAction(
        action: Action,
        triggerCtx: PPLTriggerExecutionContext,
        monitorCtx: MonitorRunnerExecutionContext,
        pplMonitor: PPLMonitor,
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
                        pplMonitor.id,
                        monitorCtx.settings!!,
                        monitorCtx.threadPool!!.threadContext,
                        pplMonitor.user?.roles,
                        pplMonitor.user
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

    /* public util functions */

    // appends user-defined custom trigger condition to PPL query, only for custom condition Triggers
    fun appendCustomCondition(query: String, customCondition: String): String {
        return "$query | $customCondition"
    }

    fun appendDataRowsLimit(query: String, maxDataRows: Long): String {
        return "$query | head $maxDataRows"
    }

    // returns PPL query response as parsable JSONObject
    suspend fun executePplQuery(query: String, client: NodeClient): JSONObject {
        // call PPL plugin to execute time filtered query
        val transportPplQueryRequest = TransportPPLQueryRequest(
            query,
            JSONObject(mapOf(PPL_SQL_QUERY_FIELD to query)),
            null // null path falls back to a default path internal to SQL/PPL Plugin
        )

        val transportPplQueryResponse = PPLPluginInterface.suspendUntil {
            this.executeQuery(
                client,
                transportPplQueryRequest,
                it
            )
        }

        val queryResponseJson = JSONObject(transportPplQueryResponse.result)

        return queryResponseJson
    }

    // TODO: is there maybe some PPL plugin util function we can use to replace this?
    // searches a given custom condition eval statement for the name of
    // the eval result variable and returns it
    fun findEvalResultVar(customCondition: String): String {
        // the PPL keyword "eval", followed by a whitespace must be present, otherwise a syntax error from PPL plugin would've
        // been thrown when executing the query (without the whitespace, the query would've had something like "evalresult",
        // which is invalid PPL
        val regex = """\beval\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*=""".toRegex()
        val evalResultVar = regex.find(customCondition)?.groupValues?.get(1)
            ?: throw IllegalArgumentException("Given custom condition is invalid, could not find eval result variable")
        return evalResultVar
    }

    fun findEvalResultVarIdxInSchema(customConditionQueryResponse: JSONObject, evalResultVarName: String): Int {
        // find the index eval statement result variable in the PPL query response schema
        val schemaList = customConditionQueryResponse.getJSONArray("schema")
        var evalResultVarIdx = -1
        for (i in 0 until schemaList.length()) {
            val schemaObj = schemaList.getJSONObject(i)
            val columnName = schemaObj.getString("name")

            if (columnName == evalResultVarName) {
                evalResultVarIdx = i
                break
            }
        }

        // eval statement result variable should always be found
        if (evalResultVarIdx == -1) {
            throw IllegalStateException(
                "expected to find eval statement results variable \"$evalResultVarName\" in results " +
                    "of PPL query with custom condition, but did not."
            )
        }

        return evalResultVarIdx
    }
}

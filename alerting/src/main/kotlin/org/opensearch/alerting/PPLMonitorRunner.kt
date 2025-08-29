package org.opensearch.alerting

import org.apache.logging.log4j.LogManager
import org.json.JSONObject
import org.opensearch.ExceptionsHelper
import org.opensearch.action.DocWriteRequest
import org.opensearch.action.bulk.BackoffPolicy
import org.opensearch.action.bulk.BulkRequest
import org.opensearch.action.bulk.BulkResponse
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.support.WriteRequest
import org.opensearch.alerting.QueryLevelMonitorRunner.getConfigAndSendNotification
import org.opensearch.alerting.alerts.AlertIndices
import org.opensearch.alerting.opensearchapi.retry
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.script.PPLTriggerExecutionContext
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.commons.alerting.model.ActionRunResult
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.commons.alerting.model.AlertV2
import org.opensearch.commons.alerting.model.MonitorV2
import org.opensearch.commons.alerting.model.MonitorV2RunResult
import org.opensearch.commons.alerting.model.PPLMonitor
import org.opensearch.commons.alerting.model.PPLMonitorRunResult
import org.opensearch.commons.alerting.model.PPLTrigger
import org.opensearch.commons.alerting.model.PPLTrigger.ConditionType
import org.opensearch.commons.alerting.model.PPLTrigger.NumResultsCondition
import org.opensearch.commons.alerting.model.PPLTrigger.TriggerMode
import org.opensearch.commons.alerting.model.PPLTriggerRunResult
import org.opensearch.commons.alerting.model.ScheduledJob.Companion.SCHEDULED_JOBS_INDEX
import org.opensearch.commons.alerting.model.action.Action
import org.opensearch.commons.ppl.PPLPluginInterface
import org.opensearch.commons.ppl.action.TransportPPLQueryRequest
import org.opensearch.core.common.Strings
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.node.NodeClient
import java.time.Instant
import java.time.ZoneOffset.UTC
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

object PPLMonitorRunner : MonitorV2Runner() {
    private val logger = LogManager.getLogger(javaClass)

    private const val PPL_SQL_QUERY_FIELD = "query" // name of PPL query field when passing into PPL/SQL Execute API call

    override suspend fun runMonitorV2(
        monitorV2: MonitorV2,
        monitorCtx: MonitorRunnerExecutionContext, // MonitorV2 reads from same context as Monitor
        periodStart: Instant,
        periodEnd: Instant,
        dryRun: Boolean,
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
        val pplQueryResults = mutableMapOf<String, JSONObject>()
        val generatedAlerts = mutableListOf<AlertV2>()

        // TODO: Instant.ofEpochMilli(monitorCtx.threadPool!!.absoluteTimeInMillis()) alternative?
        // set the current execution time
        val timeOfCurrentExecution = Instant.now()

        // TODO: should alerting v1 and v2 alerts index be separate?
        // TODO: should alerting v1 and v2 alerting-config index be separate?
        try {
            // TODO: write generated V2 alerts to existing alerts v1 index for now, revisit this decision
            monitorCtx.alertIndices!!.createOrUpdateAlertIndex()
            monitorCtx.alertIndices!!.createOrUpdateInitialAlertHistoryIndex()
        } catch (e: Exception) {
            val id = if (pplMonitor.id.trim().isEmpty()) "_na_" else pplMonitor.id
            logger.error("Error loading alerts for monitorV2: $id", e)
            return PPLMonitorRunResult(pplMonitor.name, e, periodStart, periodEnd, mapOf(), mapOf())
        }

        // only query data between now and the last PPL Monitor execution
        // do this by injecting a time filtering where statement into PPL Monitor query
        val timeFilteredQuery = addTimeFilter(pplMonitor.query, periodStart, periodEnd)

        // run each trigger
        for (trigger in pplMonitor.triggers) {
            try {
                val pplTrigger = trigger as PPLTrigger

                // check for suppression and skip execution
                // before even running the trigger itself
                val suppressed = checkForSuppress(pplTrigger, timeOfCurrentExecution)
                if (suppressed) {
                    logger.info("suppressing trigger ${pplTrigger.name} from monitor ${pplMonitor.name}")
                    continue
                }
                logger.info("suppression check passed, executing trigger ${pplTrigger.name} from monitor ${pplMonitor.name}")

//                internal fun isActionActionable(action: Action, alert: Alert?): Boolean {
//                    if (alert != null && alert.state == Alert.State.AUDIT)
//                        return false
//                    if (alert == null || action.throttle == null) {
//                        return true
//                    }
//                    if (action.throttleEnabled) {
//                        val result = alert.actionExecutionResults.firstOrNull { r -> r.actionId == action.id }
//                        val lastExecutionTime: Instant? = result?.lastExecutionTime
//                        val throttledTimeBound = currentTime().minus(action.throttle!!.value.toLong(), action.throttle!!.unit)
//                        return (lastExecutionTime == null || lastExecutionTime.isBefore(throttledTimeBound))
//                    }
//                    return true
//                }

                // if trigger uses custom condition, append the custom condition to query, otherwise simply proceed
                val queryToExecute = if (pplTrigger.conditionType == ConditionType.NUMBER_OF_RESULTS) { // number of results trigger
                    timeFilteredQuery
                } else { // custom condition trigger
                    appendCustomCondition(timeFilteredQuery, pplTrigger.customCondition!!)
                }

                // TODO: does this handle pagination? does it need to?
                // execute the PPL query
                val queryResponseJson = executePplQuery(queryToExecute, nodeClient)
                logger.info("query execution results for trigger ${pplTrigger.name}: $queryResponseJson")

                // retrieve the number of results
                // for number of results triggers, this is simply the number of PPL query results
                // for custom triggers, this is the number of rows in the query response's eval result column that evaluated to true
                val numResults = if (pplTrigger.conditionType == ConditionType.NUMBER_OF_RESULTS) { // number of results trigger
                    queryResponseJson.getLong("total")
                } else { // custom condition trigger
                    evaluateCustomConditionTrigger(queryResponseJson, pplTrigger)
                }

                // determine if the trigger condition has been met
                val triggered = if (pplTrigger.conditionType == ConditionType.NUMBER_OF_RESULTS) { // number of results trigger
                    evaluateNumResultsTrigger(numResults, pplTrigger.numResultsCondition!!, pplTrigger.numResultsValue!!)
                } else { // custom condition trigger
                    numResults > 0 // if any of the query results satisfied the custom condition, the trigger counts as triggered
                }

                logger.info("PPLTrigger ${pplTrigger.name} triggered: $triggered")

                // store the trigger execution and ppl query results for
                // trigger execution response and notification message context
                triggerResults[pplTrigger.id] = PPLTriggerRunResult(pplTrigger.name, triggered, null)
                pplQueryResults[pplTrigger.id] = queryResponseJson

                if (triggered) {
                    // collect the generated alerts to be written to alerts index
                    generatedAlerts.addAll(generateAlerts(pplTrigger, pplMonitor, numResults, timeOfCurrentExecution))

                    // update the trigger's last execution time for future suppression checks
                    pplTrigger.lastTriggeredTime = timeOfCurrentExecution

                    // TODO: this is purely a result set implementation
                    // TODO: when trigger is per result, need to send for every individual result, not just the whole thing
                    // send alert notifications
                    val pplTriggerExecutionContext = PPLTriggerExecutionContext(
                        monitorV2,
                        periodStart,
                        periodEnd,
                        null,
                        pplTrigger,
                        pplQueryResults[pplTrigger.id]!!.toMap()
                    )
                    for (action in pplTrigger.actions) {
                        runAction(
                            action,
                            pplTriggerExecutionContext,
                            monitorCtx,
                            pplMonitor,
                            dryRun,
                            timeOfCurrentExecution
                        )
                    }
                }
            } catch (e: Exception) {
                logger.error("failed to run PPL Trigger for PPL Monitor ${pplMonitor.name}", e)
                continue
            }
        }

        // TODO: what if retry policy null?
        // write the alerts to the alerts index
        monitorCtx.retryPolicy?.let {
            saveAlertsV2(generatedAlerts, pplMonitor, it, nodeClient)
        }

        // TODO: collect all triggers that were throttled, and if none were throttled, skip update monitor? saves on write requests
        // for suppression checking purposes, update the PPL Monitor in the alerting-config index
        // with updated last triggered times for each of its triggers
        updateMonitorWithLastTriggeredTimes(pplMonitor, nodeClient)

        return PPLMonitorRunResult(
            pplMonitor.name,
            null,
            periodStart,
            periodEnd,
            triggerResults,
            pplQueryResults.map { it.key to it.value.toMap() }.toMap()
        )
    }

    private fun checkForSuppress(pplTrigger: PPLTrigger, timeOfCurrentExecution: Instant): Boolean {
        // the interval between throttledTimeBound and now is the suppression window
        // i.e. any PPLTrigger whose last trigger time is in this window must be suppressed
        val suppressTimeBound = pplTrigger.suppressDuration?.let {
            timeOfCurrentExecution.minus(pplTrigger.suppressDuration!!.millis, ChronoUnit.MILLIS)
        }

        // the trigger must be suppressed if...
        return pplTrigger.suppressDuration != null && // suppression is enabled on the PPLTrigger
            pplTrigger.lastTriggeredTime != null && // and it has triggered before at least once
            pplTrigger.lastTriggeredTime!!.isAfter(suppressTimeBound!!) // and it's not yet out of the suppression window
    }

    // adds monitor schedule-based time filter
    private fun addTimeFilter(query: String, periodStart: Instant, periodEnd: Instant): String {
        // inject time filter into PPL query to only query for data within the (periodStart, periodEnd) interval
        // TODO: if query contains "_time", "span", "earliest", "latest", skip adding filter
        // TODO: pending https://github.com/opensearch-project/sql/issues/3969
        // for now assume "_time" field is always present in customer data

        // PPL plugin only accepts timestamp strings in this format
        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(UTC)

        val periodStartPplTimestamp = formatter.format(periodStart)
        val periodEndPplTimeStamp = formatter.format(periodEnd)

        val timeFilterReplace = "| where _time > TIMESTAMP('$periodStartPplTimestamp') and _time < TIMESTAMP('$periodEndPplTimeStamp') |"
        val timeFilterAppend = "| where _time > TIMESTAMP('$periodStartPplTimestamp') and _time < TIMESTAMP('$periodEndPplTimeStamp')"

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

        logger.info("time filtered query: $timeFilteredQuery")

        return timeFilteredQuery
    }

    // appends user-defined custom trigger condition to PPL query, only for custom condition Triggers
    private fun appendCustomCondition(query: String, customCondition: String): String {
        return "$query | $customCondition"
    }

    // returns PPL query response as parsable JSONObject
    private suspend fun executePplQuery(query: String, client: NodeClient): JSONObject {
        // call PPL plugin to execute time filtered query
        val transportPplQueryRequest = TransportPPLQueryRequest(
            query,
            JSONObject(mapOf(PPL_SQL_QUERY_FIELD to query)), // TODO: what is the purpose of this arg?
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

    private fun evaluateNumResultsTrigger(numResults: Long, numResultsCondition: NumResultsCondition, numResultsValue: Long): Boolean {
        return when (numResultsCondition) {
            NumResultsCondition.GREATER_THAN -> numResults > numResultsValue
            NumResultsCondition.GREATER_THAN_EQUAL -> numResults >= numResultsValue
            NumResultsCondition.LESS_THAN -> numResults < numResultsValue
            NumResultsCondition.LESS_THAN_EQUAL -> numResults <= numResultsValue
            NumResultsCondition.EQUAL -> numResults == numResultsValue
            NumResultsCondition.NOT_EQUAL -> numResults != numResultsValue
        }
    }

    private fun evaluateCustomConditionTrigger(customConditionQueryResponse: JSONObject, pplTrigger: PPLTrigger): Long {
        // a PPL query with custom condition returning 0 results should imply a valid but not useful query.
        // do not trigger alert, but warn that query likely is not functioning as user intended
        if (customConditionQueryResponse.getLong("total") == 0L) {
            logger.warn(
                "During execution of PPL Trigger ${pplTrigger.name}, PPL query with custom " +
                    "condition returned no results. Proceeding without generating alert."
            )
            return 0L
        }

        // find the name of the eval result variable defined in custom condition
        val evalResultVarName = findEvalResultVar(pplTrigger.customCondition!!)

        // find the eval statement result variable in the PPL query response schema
        val schemaList = customConditionQueryResponse.getJSONArray("schema")
        var evalResultVarIdx = -1
        for (i in 0 until schemaList.length()) {
            val schemaObj = schemaList.getJSONObject(i)
            val columnName = schemaObj.getString("name")

            if (columnName == evalResultVarName) {
                if (schemaObj.getString("type") != "boolean") {
                    throw IllegalStateException(
                        "parsing results of PPL query with custom condition failed," +
                            "eval statement variable was not type boolean, but instead type: ${schemaObj.getString("type")}"
                    )
                }

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

        val dataRowList = customConditionQueryResponse.getJSONArray("datarows")
        var numTriggered = 0L // the number of query result rows that evaluated to true
        for (i in 0 until dataRowList.length()) {
            val dataRow = dataRowList.getJSONArray(i)
            val evalResult = dataRow.getBoolean(evalResultVarIdx)
            if (evalResult) {
                numTriggered++
            }
        }

        return numTriggered
    }

    // TODO: is there maybe some PPL plugin util function we can use to replace this?
    // searches a given custom condition eval statement for the name of the result
    // variable and returns it
    private fun findEvalResultVar(customCondition: String): String {
        // the PPL keyword "eval", followed by a whitespace must be present, otherwise a syntax error from PPL plugin would've
        // been thrown when executing the query (without the whitespace, the query would've had something like "evalresult",
        // which is invalid PPL
        val startOfEvalStatement = "eval "

        val startIdx = customCondition.indexOf(startOfEvalStatement) + startOfEvalStatement.length
        val endIdx = startIdx + customCondition.substring(startIdx).indexOfFirst { it == ' ' || it == '=' }
        return customCondition.substring(startIdx, endIdx)
    }

    private fun generateAlerts(
        pplTrigger: PPLTrigger,
        pplMonitor: PPLMonitor,
        numAlertsToGenerate: Long,
        timeOfCurrentExecution: Instant
    ): List<AlertV2> {
        // TODO: currently naively generates an alert and action every time
        // TODO: maintain alert state, check for COMPLETED alert and suppression condition, like query level monitor

        val expirationTime = pplTrigger.expireDuration?.millis?.let { timeOfCurrentExecution.plus(it, ChronoUnit.MILLIS) }

        val alertV2 = AlertV2(
            monitorId = pplMonitor.id,
            monitorName = pplMonitor.name,
            monitorVersion = pplMonitor.version,
            triggerId = pplTrigger.id,
            triggerName = pplTrigger.name,
            state = Alert.State.ACTIVE,
            startTime = timeOfCurrentExecution,
            expirationTime = expirationTime,
            errorHistory = listOf(),
            severity = pplTrigger.severity.value,
            actionExecutionResults = listOf(),
        )

        val alertV2s = mutableListOf<AlertV2>()
        if (pplTrigger.mode == TriggerMode.RESULT_SET) {
            alertV2s.add(alertV2)
        } else { // TriggerMode.PER_RESULT
            for (i in 0 until numAlertsToGenerate) {
                alertV2s.add(alertV2)
            }
        }

        return alertV2s.toList() // return an immutable list
    }

    private suspend fun saveAlertsV2(
        alerts: List<AlertV2>,
        pplMonitor: PPLMonitor,
        retryPolicy: BackoffPolicy,
        client: NodeClient
    ) {
        logger.info("received alerts: $alerts")

        var requestsToRetry = alerts.flatMap { alert ->
            // We don't want to set the version when saving alerts because the MonitorRunner has first priority when writing alerts.
            // In the rare event that a user acknowledges an alert between when it's read and when it's written
            // back we're ok if that acknowledgement is lost. It's easier to get the user to retry than for the runner to
            // spend time reloading the alert and writing it back.

            when (alert.state) {
                Alert.State.ACTIVE, Alert.State.ERROR -> {
                    listOf<DocWriteRequest<*>>(
                        IndexRequest(AlertIndices.ALERT_INDEX)
                            .routing(pplMonitor.id) // set routing ID to PPL Monitor ID
                            .source(alert.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
                            .id(if (alert.id != Alert.NO_ID) alert.id else null)
                    )
                }
                else -> throw IllegalStateException("trying to save non ACTIVE alert, unimplemented territory")
            }
        }

        if (requestsToRetry.isEmpty()) return
        // Retry Bulk requests if there was any 429 response
        retryPolicy.retry(logger, listOf(RestStatus.TOO_MANY_REQUESTS)) {
            val bulkRequest = BulkRequest().add(requestsToRetry).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            val bulkResponse: BulkResponse = client.suspendUntil { client.bulk(bulkRequest, it) }
            val failedResponses = (bulkResponse.items ?: arrayOf()).filter { it.isFailed }
            requestsToRetry = failedResponses.filter { it.status() == RestStatus.TOO_MANY_REQUESTS }
                .map { bulkRequest.requests()[it.itemId] as IndexRequest }

            if (requestsToRetry.isNotEmpty()) {
                val retryCause = failedResponses.first { it.status() == RestStatus.TOO_MANY_REQUESTS }.failure.cause
                throw ExceptionsHelper.convertToOpenSearchException(retryCause)
            }
        }
    }

    private suspend fun updateMonitorWithLastTriggeredTimes(pplMonitor: PPLMonitor, client: NodeClient) {
        val indexRequest = IndexRequest(SCHEDULED_JOBS_INDEX)
            .id(pplMonitor.id)
            .source(pplMonitor.toXContentWithType(XContentFactory.jsonBuilder()))
            .routing(pplMonitor.id)

        val indexResponse = client.suspendUntil { index(indexRequest, it) }

        logger.info("PPLMonitor update with last execution times index response: ${indexResponse.result}")
    }

    suspend fun runAction(
        action: Action,
        triggerCtx: PPLTriggerExecutionContext,
        monitorCtx: MonitorRunnerExecutionContext,
        pplMonitor: PPLMonitor,
        dryrun: Boolean,
        timeOfCurrentExecution: Instant
    ): ActionRunResult {
        return try {
            val actionOutput = mutableMapOf<String, String>()
            actionOutput[Action.SUBJECT] = if (action.subjectTemplate != null)
                MonitorRunnerService.compileTemplateV2(action.subjectTemplate!!, triggerCtx)
            else ""
            actionOutput[Action.MESSAGE] = MonitorRunnerService.compileTemplateV2(action.messageTemplate, triggerCtx)
            if (Strings.isNullOrEmpty(actionOutput[Action.MESSAGE])) {
                throw IllegalStateException("Message content missing in the Destination with id: ${action.destinationId}")
            }

            if (!dryrun) {
//                val client = monitorCtx.client
                actionOutput[Action.MESSAGE_ID] = getConfigAndSendNotification(
                    action,
                    monitorCtx,
                    actionOutput[Action.SUBJECT],
                    actionOutput[Action.MESSAGE]!!
                )
                // TODO: use this block when security plugin is enabled
//                client!!.threadPool().threadContext.stashContext().use {
//                    withClosableContext(
//                        InjectorContextElement(
//                            pplMonitor.id,
//                            monitorCtx.settings!!,
//                            monitorCtx.threadPool!!.threadContext,
//                            pplMonitor.user?.roles,
//                            pplMonitor.user
//                        )
//                    ) {
//                        actionOutput[Action.MESSAGE_ID] = getConfigAndSendNotification(
//                            action,
//                            monitorCtx,
//                            actionOutput[Action.SUBJECT],
//                            actionOutput[Action.MESSAGE]!!
//                        )
//                    }
//                }
            }
            ActionRunResult(action.id, action.name, actionOutput, false, timeOfCurrentExecution, null)
        } catch (e: Exception) {
            ActionRunResult(action.id, action.name, mapOf(), false, timeOfCurrentExecution, e)
        }
    }
}

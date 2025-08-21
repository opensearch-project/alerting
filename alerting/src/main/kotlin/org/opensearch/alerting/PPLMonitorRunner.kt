package org.opensearch.alerting

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import org.apache.logging.log4j.LogManager
import org.json.JSONObject
import org.opensearch.ExceptionsHelper
import org.opensearch.action.DocWriteRequest
import org.opensearch.action.bulk.BackoffPolicy
import org.opensearch.action.bulk.BulkRequest
import org.opensearch.action.bulk.BulkResponse
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.support.WriteRequest
import org.opensearch.alerting.alerts.AlertIndices
import org.opensearch.alerting.opensearchapi.retry
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.common.xcontent.XContentFactory
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
import org.opensearch.commons.ppl.PPLPluginInterface
import org.opensearch.commons.ppl.action.TransportPPLQueryRequest
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.node.NodeClient
import java.time.Instant
import java.time.ZoneOffset.UTC
import java.time.format.DateTimeFormatter

object PPLMonitorRunner : MonitorV2Runner() {
    private val logger = LogManager.getLogger(javaClass)
    private val scope: CoroutineScope = CoroutineScope(Dispatchers.IO)

    const val PPL_SQL_QUERY_FIELD = "query" // name of PPL query field when passing into PPL/SQL Execute API call

    // TODO: this is a hacky implementation, needs serious revision and additions
    // TODO: implement custom condition triggering
    override suspend fun runMonitorV2(
        monitorV2: MonitorV2,
        monitorCtx: MonitorRunnerExecutionContext, // MonitorV2 reads from same context as Monitor
        periodStart: Instant,
        periodEnd: Instant,
        dryRun: Boolean,
        executionId: String,
        transportService: TransportService,
    ): MonitorV2RunResult<*> {
        logger.debug("Running monitor: ${monitorV2.name}. Thread: ${Thread.currentThread().name}")

        if (monitorV2 !is PPLMonitor) {
            throw IllegalStateException("Unexpected monitor type: ${monitorV2.javaClass.name}")
        }

        if (periodStart == periodEnd) {
            logger.warn("Start and end time are the same: $periodStart. This monitor will probably only run once.")
        }

        var monitorV2Result = PPLMonitorRunResult(monitorV2.name, null, periodStart, periodEnd, mapOf(), mapOf())

        // TODO: should alerting v1 and v2 alerts index be separate?
        // TODO: should alerting v1 and v2 alerting-config index be separate?
//        val currentAlerts = try {
//            // TODO: write generated V2 alerts to existing alerts v1 index for now, revisit this decision
//            monitorCtx.alertIndices!!.createOrUpdateAlertIndex()
//            monitorCtx.alertIndices!!.createOrUpdateInitialAlertHistoryIndex()
//        } catch (e: Exception) {
//            // We can't save ERROR alerts to the index here as we don't know if there are existing ACTIVE alerts
//            val id = if (monitorV2.id.trim().isEmpty()) "_na_" else monitorV2.id
//            logger.error("Error loading alerts for monitorV2: $id", e)
//            return monitorV2Result.copy(error = e)
//        }

        val timeFilteredQuery = addTimeFilter(monitorV2.query, periodStart, periodEnd)

        val triggerResults = mutableMapOf<String, PPLTriggerRunResult>()
        val pplQueryResults = mutableMapOf<String, JSONObject>()
        val generatedAlerts = mutableListOf<AlertV2>()

        for (trigger in monitorV2.triggers) {
            val pplTrigger = trigger as PPLTrigger

            if (pplTrigger.conditionType == ConditionType.NUMBER_OF_RESULTS) { // number_of_results trigger
                val queryResponseJson = executePplQuery(timeFilteredQuery, monitorCtx)

                val numResults = queryResponseJson.getLong("total")

                val triggered = evaluateNumResultsTrigger(numResults, trigger.numResultsCondition!!, trigger.numResultsValue!!)

                val pplTriggerRunResult = PPLTriggerRunResult(trigger.name, triggered, null)

                triggerResults[pplTrigger.id] = pplTriggerRunResult
                pplQueryResults[pplTrigger.id] = queryResponseJson

                logger.info("trigger ${trigger.name} triggered: $triggered")
                logger.info("ppl query results: $queryResponseJson")

                if (triggered) {
                    // TODO: currently naively generates an alert and action every time
                    // TODO: maintain alert state, check for COMPLETED alert and suppression condition, like query level monitor
                    // query results will not be stored in alerts, but are instead included in notification actions
                    val alertV2 = AlertV2(
                        monitorId = monitorV2.id,
                        monitorName = monitorV2.name,
                        monitorVersion = monitorV2.version,
                        triggerId = trigger.id,
                        triggerName = trigger.name,
                        state = Alert.State.ACTIVE,
                        startTime = Instant.now(),
                        errorHistory = listOf(),
                        severity = trigger.severity.value,
                        actionExecutionResults = listOf(),
                    )

                    if (pplTrigger.mode == TriggerMode.RESULT_SET) {
                        generatedAlerts.add(alertV2)
                    } else { // TriggerMode.PER_RESULT
                        for (i in 0 until numResults) {
                            generatedAlerts.add(alertV2)
                        }
                    }
                }
            } else { // custom trigger
                val queryWithCustomCondition = addCustomCondition(timeFilteredQuery, trigger.customCondition!!)

                val queryResponseJson = executePplQuery(queryWithCustomCondition, monitorCtx)

                // a PPL query with custom condition returning 0 results should imply a valid but not useful query.
                // do not trigger alert, but warn that query likely is not functioning as user intended
                if (queryResponseJson.getLong("total") == 0L) {
                    logger.warn(
                        "During execution of monitor ${monitorV2.name}, PPL query with custom" +
                            "condition returned no results. Proceeding without triggering alert."
                    )

                    val pplTriggerRunResult = PPLTriggerRunResult(trigger.name, false, null)
                    triggerResults[pplTrigger.id] = pplTriggerRunResult

                    continue
                }

                // find the name of the eval result variable defined in custom condition
                val evalResultVarName = trigger.customCondition!!.split(" ")[1] // [0] is "eval", [1] is the var name

                // find the eval statement result variable in the PPL query response schema
                val schemaList = queryResponseJson.getJSONArray("schema")
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
                        "expected to find eval statement results variable $evalResultVarName in results" +
                            "of PPL query with custom condition, but did not."
                    )
                }

                val dataRowList = queryResponseJson.getJSONArray("datarows")
                var numTriggered = 0 // the number of query result rows that evaluated to true
                for (i in 0 until dataRowList.length()) {
                    val dataRow = dataRowList.getJSONArray(i)
                    val evalResult = dataRow.getBoolean(evalResultVarIdx)
                    if (evalResult) {
                        numTriggered++
                    }
                }

                val triggered = numTriggered > 0
                val pplTriggerRunResult = PPLTriggerRunResult(trigger.name, triggered, null)

                triggerResults[pplTrigger.id] = pplTriggerRunResult
                pplQueryResults[pplTrigger.id] = queryResponseJson

                logger.info("trigger ${trigger.name} triggered: $triggered")
                logger.info("ppl query results: $queryResponseJson")

                if (triggered) {
                    // TODO: currently naively generates an alert and action every time
                    // TODO: maintain alert state, check for COMPLETED alert and suppression condition, like query level monitor
                    val alertV2 = AlertV2(
                        monitorId = monitorV2.id,
                        monitorName = monitorV2.name,
                        monitorVersion = monitorV2.version,
                        triggerId = trigger.id,
                        triggerName = trigger.name,
                        state = Alert.State.ACTIVE,
                        startTime = Instant.now(),
                        errorHistory = listOf(),
                        severity = trigger.severity.value,
                        actionExecutionResults = listOf(),
                    )

                    if (pplTrigger.mode == TriggerMode.RESULT_SET) {
                        generatedAlerts.add(alertV2)
                    } else { // TriggerMode.PER_RESULT
                        for (i in 0 until numTriggered) {
                            generatedAlerts.add(alertV2)
                        }
                    }
                }
            }

//            if (monitorCtx.triggerService!!.isQueryLevelTriggerActionable(triggerCtx, triggerResult, workflowRunContext)) {
//                val actionCtx = triggerCtx.copy(error = monitorResult.error ?: triggerResult.error)
//                for (action in trigger.actions) {
//                    triggerResult.actionResults[action.id] = this.runAction(action, actionCtx, monitorCtx, monitor, dryrun)
//                }
//            }
        }

        // TODO: what if retry policy null?
        monitorCtx.retryPolicy?.let {
            saveAlertsV2(
                generatedAlerts,
                monitorCtx,
                it,
                monitorV2.id
            )
        }

        logger.info("trigger results: $triggerResults")
        logger.info("ppl query results: $pplQueryResults")

        return monitorV2Result.copy(triggerResults = triggerResults, pplQueryResults = pplQueryResults)
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

    // appendss user-defined custom trigger condition to PPL query, only for custom condition Triggers
    private fun addCustomCondition(query: String, customCondition: String): String {
        return "$query | $customCondition"
    }

    // returns PPL query response as parsable JSONObject
    private suspend fun executePplQuery(query: String, monitorCtx: MonitorRunnerExecutionContext): JSONObject {
        // call PPL plugin to execute time filtered query
        val transportPplQueryRequest = TransportPPLQueryRequest(
            query,
            JSONObject(mapOf(PPL_SQL_QUERY_FIELD to query)), // TODO: what is the purpose of this arg?
            null // null path falls back to a default path internal to SQL/PPL Plugin
        )

        val transportPplQueryResponse = PPLPluginInterface.suspendUntil {
            this.executeQuery(
                monitorCtx.client as NodeClient,
                transportPplQueryRequest,
                it
            )
        }

        val queryResponseJson = JSONObject(transportPplQueryResponse.result)

        return queryResponseJson
    }

    private suspend fun saveAlertsV2(
        alerts: List<AlertV2>,
        monitorCtx: MonitorRunnerExecutionContext,
        retryPolicy: BackoffPolicy,
        routingId: String // routing is mandatory and set as monitor id. for workflow chained alerts we pass workflow id as routing
    ) {
        val alertsIndex = AlertIndices.ALERT_INDEX
        val alertsHistoryIndex = AlertIndices.ALERT_HISTORY_WRITE_INDEX

        var requestsToRetry = alerts.flatMap { alert ->
            // We don't want to set the version when saving alerts because the MonitorRunner has first priority when writing alerts.
            // In the rare event that a user acknowledges an alert between when it's read and when it's written
            // back we're ok if that acknowledgement is lost. It's easier to get the user to retry than for the runner to
            // spend time reloading the alert and writing it back.
            when (alert.state) {
                Alert.State.ACTIVE, Alert.State.ERROR -> {
                    listOf<DocWriteRequest<*>>(
                        IndexRequest(alertsIndex)
                            .routing(routingId)
                            .source(alert.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
                            .id(if (alert.id != Alert.NO_ID) alert.id else null)
                    )
                }
                else -> throw IllegalStateException("trying to save non ACTIVE alert, unimplemented territory")
            }
        }

        val client = monitorCtx.client!!

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
}

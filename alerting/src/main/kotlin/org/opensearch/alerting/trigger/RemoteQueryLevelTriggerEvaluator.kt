/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.trigger

import org.apache.logging.log4j.LogManager
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.alerting.opensearchapi.convertToMap
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.util.TriggerScriptRewriter
import org.opensearch.commons.alerting.model.QueryLevelTrigger
import org.opensearch.commons.alerting.model.QueryLevelTriggerRunResult
import org.opensearch.index.query.QueryBuilders
import org.opensearch.script.Script
import org.opensearch.script.ScriptType
import org.opensearch.search.aggregations.AggregationBuilders
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.transport.client.Client

/**
 * Evaluates query-level triggers remotely on the user's cluster via filter aggregations.
 *
 * This evaluator:
 * 1. Rewrites trigger scripts from `ctx.results[0]` to `params.results_0`
 * 2. Sends a single search request with one filter agg each per trigger
 * 3. Passes the full search response from Call 1 as `params.results_0`
 * 4. Reads `doc_count > 0` per trigger agg to determine if the trigger fires
 */
object RemoteQueryLevelTriggerEvaluator {

    private val logger = LogManager.getLogger(javaClass)
    private const val TRIGGER_AGG_PREFIX = "_query_trigger_"

    /**
     * Evaluates all triggers for a query-level monitor by sending a filter-agg request
     * to the user's cluster.
     *
     * @param client The client connected to the user's cluster
     * @param indices The indices to target (same as the monitor's search input)
     * @param triggers The query-level triggers to evaluate
     * @param searchResponse The full search response from the monitor's query (Call 1)
     * @return Map of trigger ID to QueryLevelTriggerRunResult
     */
    suspend fun evaluate(
        client: Client,
        indices: List<String>,
        triggers: List<QueryLevelTrigger>,
        searchResponse: Map<String, Any>
    ): Map<String, QueryLevelTriggerRunResult> {
        val triggerData = triggers.map { TriggerData(it.id, it.name, it.condition.idOrCode) }
        val searchSource = buildEvalSearchSource(triggerData, searchResponse)

        return try {
            val evalRequest = SearchRequest(*indices.toTypedArray()).source(searchSource)
            val evalResponse: SearchResponse = client.suspendUntil { client.search(evalRequest, it) }
            val evalMap = evalResponse.convertToMap()

            @Suppress("UNCHECKED_CAST")
            val aggs = evalMap["aggregations"] as? Map<String, Map<String, Any>> ?: emptyMap()
            val triggerIds = triggerData.map { it.id }
            val parsedResults = parseEvalResponse(triggerIds, aggs)

            triggers.associate { trigger ->
                val triggered = parsedResults[trigger.id] ?: false
                trigger.id to QueryLevelTriggerRunResult(trigger.name, triggered, null)
            }
        } catch (e: Exception) {
            logger.error("Error evaluating triggers remotely", e)
            // On error, fail closed — don't trigger alerts but surface the error
            triggers.associate { it.id to QueryLevelTriggerRunResult(it.name, false, e) }
        }
    }

    /**
     * Builds the search source for the evaluation request.
     * Each trigger becomes a filter aggregation with a script query.
     */
    fun buildEvalSearchSource(
        triggers: List<Any>,
        searchResponse: Map<String, Any>
    ): SearchSourceBuilder {
        val searchSource = SearchSourceBuilder().size(0)

        for (trigger in triggers) {
            val (id, script) = when (trigger) {
                is TriggerData -> trigger.id to trigger.script
                else -> {
                    val t = trigger as? Map<*, *>
                        ?: throw IllegalArgumentException("Unsupported trigger type")
                    t["id"].toString() to t["script"].toString()
                }
            }

            val rewrittenScript = TriggerScriptRewriter.rewriteScript(script)
            val scriptObj = Script(
                ScriptType.INLINE,
                "painless",
                rewrittenScript,
                mapOf("results_0" to searchResponse)
            )
            val filterAgg = AggregationBuilders.filter(
                "$TRIGGER_AGG_PREFIX$id",
                QueryBuilders.scriptQuery(scriptObj)
            )
            searchSource.aggregation(filterAgg)
        }

        return searchSource
    }

    /**
     * Parses the evaluation response to determine which triggers fired.
     * `doc_count > 0` means the trigger condition was true.
     * Missing triggers default to triggered (fail-open for safety).
     */
    @Suppress("UNCHECKED_CAST")
    fun parseEvalResponse(
        triggerIds: List<String>,
        aggResults: Map<String, Map<String, Any>>
    ): Map<String, Boolean> {
        return triggerIds.associateWith { triggerId ->
            val aggKey = "$TRIGGER_AGG_PREFIX$triggerId"
            val aggResult = aggResults[aggKey]
            if (aggResult == null) {
                logger.warn("Missing evaluation result for trigger $triggerId, defaulting to not triggered")
                false
            } else {
                val docCount = (aggResult["doc_count"] as? Number)?.toLong() ?: 0L
                docCount > 0
            }
        }
    }

    /** Internal data class for decoupling trigger data from the full model in buildEvalSearchSource */
    data class TriggerData(val id: String, val name: String, val script: String)
}

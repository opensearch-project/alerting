/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchException
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsResponse
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.model.Finding
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.alerting.util.AlertingException
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParserUtils
import org.opensearch.index.query.QueryBuilders
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.transport.client.Client

private val log = LogManager.getLogger(WorkflowService::class.java)

/**
 * Contains util methods used in workflow execution
 */
class WorkflowService(
    val client: Client,
    val xContentRegistry: NamedXContentRegistry,
) {
    /**
     * Returns finding doc ids per index for the given workflow execution
     * Used for pre-filtering the dataset in the case of creating a workflow with chained findings
     *
     * @param chainedMonitors Monitors that have previously executed
     * @param workflowExecutionId Execution id of the current workflow
     */
    suspend fun getFindingDocIdsByExecutionId(chainedMonitors: List<Monitor>, workflowExecutionId: String):
        Pair<Map<String, List<String>>, List<String>> {
        if (chainedMonitors.isEmpty())
            return Pair(emptyMap(), listOf())
        val dataSources = chainedMonitors[0].dataSources
        try {
            val existsResponse: IndicesExistsResponse = client.admin().indices().suspendUntil {
                exists(IndicesExistsRequest(dataSources.findingsIndex).local(true), it)
            }
            if (existsResponse.isExists == false) return Pair(emptyMap(), listOf())
            // Search findings index to match id of monitors and workflow execution id
            val bqb = QueryBuilders.boolQuery()
                .filter(
                    QueryBuilders.termsQuery(
                        Finding.MONITOR_ID_FIELD,
                        chainedMonitors.map { it.id }
                    )
                )
                .filter(QueryBuilders.termQuery(Finding.EXECUTION_ID_FIELD, workflowExecutionId))
            val searchRequest = SearchRequest()
                .source(
                    SearchSourceBuilder()
                        .query(bqb)
                        .version(true)
                        .seqNoAndPrimaryTerm(true)
                )
                .indices(dataSources.findingsIndex)
            val searchResponse: SearchResponse = client.suspendUntil { client.search(searchRequest, it) }

            // Get the findings docs
            val findings = mutableListOf<Finding>()
            for (hit in searchResponse.hits) {
                val xcp = XContentType.JSON.xContent()
                    .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, hit.sourceAsString)
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
                val finding = Finding.parse(xcp)
                findings.add(finding)
            }
            // Based on the findings get the document ids
            val indexToRelatedDocIdsMap = mutableMapOf<String, MutableList<String>>()
            for (finding in findings) {
                indexToRelatedDocIdsMap.getOrPut(finding.index) { mutableListOf() }.addAll(finding.relatedDocIds)
            }
            return Pair(indexToRelatedDocIdsMap, findings.map { it.id })
        } catch (t: Exception) {
            log.error("Error getting finding doc ids: ${t.message}", t)
            throw AlertingException.wrap(t)
        }
    }

    /**
     * Returns the list of monitors for the given ids
     * Used in workflow execution in order to figure out the monitor type
     *
     * @param monitors List of monitor ids
     * @param size Expected number of monitors
     */
    suspend fun getMonitorsById(monitors: List<String>, size: Int): List<Monitor> {
        try {
            val bqb = QueryBuilders.boolQuery().filter(QueryBuilders.termsQuery("_id", monitors))

            val searchRequest = SearchRequest()
                .source(
                    SearchSourceBuilder()
                        .query(bqb)
                        .version(true)
                        .seqNoAndPrimaryTerm(true)
                        .size(size)
                )
                .indices(ScheduledJob.SCHEDULED_JOBS_INDEX)

            val searchResponse: SearchResponse = client.suspendUntil { client.search(searchRequest, it) }
            return parseMonitors(searchResponse)
        } catch (e: Exception) {
            log.error("Error getting monitors: ${e.message}", e)
            throw AlertingException.wrap(e)
        }
    }

    private fun parseMonitors(response: SearchResponse): List<Monitor> {
        if (response.isTimedOut) {
            log.error("Request for getting monitors timeout")
            throw OpenSearchException("Cannot determine that the ${ScheduledJob.SCHEDULED_JOBS_INDEX} index is healthy")
        }
        val monitors = mutableListOf<Monitor>()
        try {
            for (hit in response.hits) {
                XContentType.JSON.xContent().createParser(
                    xContentRegistry,
                    LoggingDeprecationHandler.INSTANCE, hit.sourceAsString
                ).use { hitsParser ->
                    val monitor = ScheduledJob.parse(hitsParser, hit.id, hit.version) as Monitor
                    monitors.add(monitor)
                }
            }
        } catch (e: Exception) {
            log.error("Error parsing monitors: ${e.message}", e)
            throw AlertingException.wrap(e)
        }
        return monitors
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchException
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.util.AlertingException
import org.opensearch.client.Client
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParserUtils
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.model.Finding
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.index.query.BoolQueryBuilder
import org.opensearch.index.query.MatchQueryBuilder
import org.opensearch.index.query.QueryBuilders
import org.opensearch.index.query.TermsQueryBuilder
import org.opensearch.search.builder.SearchSourceBuilder
import java.util.stream.Collectors

private val log = LogManager.getLogger(WorkflowService::class.java)

class WorkflowService(
    val client: Client,
    val xContentRegistry: NamedXContentRegistry,
) {

    suspend fun getFindingDocIdsPerMonitorExecution(chainedMonitor: Monitor, workflowExecutionId: String): Map<String, List<String>> {
        // Search findings index per monitor and workflow execution id
        val bqb = QueryBuilders.boolQuery().filter(QueryBuilders.termQuery(Finding.MONITOR_ID_FIELD, chainedMonitor.id))
            .filter(QueryBuilders.termQuery(Finding.WORKFLOW_EXECUTION_ID_FIELD, workflowExecutionId))
        val searchRequest = SearchRequest()
            .source(
                SearchSourceBuilder()
                    .query(bqb)
                    .version(true)
                    .seqNoAndPrimaryTerm(true)
            )
            .indices(chainedMonitor.dataSources.findingsIndex)
        val searchResponse: SearchResponse = client.suspendUntil { client.search(searchRequest, it) }

        // Get the findings docs
        val findings = mutableListOf<Finding>()
        for (hit in searchResponse.hits) {
            val xcp = XContentFactory.xContent(XContentType.JSON)
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
        return indexToRelatedDocIdsMap
    }

    suspend fun searchMonitors(monitors: List<String>, size: Int, owner: String?): List<Monitor> {
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
        return buildMonitors(searchResponse)
    }

    private fun buildMonitors(response: SearchResponse): List<Monitor> {
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
            log.error("Error parsing monitors: ${e.message}")
            throw AlertingException.wrap(e)
        }
        return monitors
    }

    suspend fun getDocIdsPerFindingIndex(monitorId: String, workflowExecutionId: String): Map<String, List<String>> {
        val getRequest = GetRequest(ScheduledJob.SCHEDULED_JOBS_INDEX, monitorId)

        val getResponse: GetResponse = client.suspendUntil {
            client.get(getRequest, it)
        }

        val monitor = if (!getResponse.isSourceEmpty) {
            XContentHelper.createParser(
                xContentRegistry, LoggingDeprecationHandler.INSTANCE,
                getResponse.sourceAsBytesRef, XContentType.JSON
            ).use { xcp ->
                ScheduledJob.parse(xcp, getResponse.id, getResponse.version) as Monitor
            }
        } else throw IllegalStateException("Delegate monitors don't exist $monitorId")
        // Search findings index per monitor and workflow execution id
        val bqb = QueryBuilders.boolQuery().filter(QueryBuilders.termQuery(Finding.MONITOR_ID_FIELD, monitor.id))
            .filter(QueryBuilders.termQuery(Finding.WORKFLOW_EXECUTION_ID_FIELD, workflowExecutionId))
        val searchRequest = SearchRequest()
            .source(
                SearchSourceBuilder()
                    .query(bqb)
                    .version(true)
                    .seqNoAndPrimaryTerm(true)
            )
            .indices(monitor.dataSources.findingsIndex)
        val searchResponse: SearchResponse = client.suspendUntil { client.search(searchRequest, it) }

        // Get the findings docs
        val findings = mutableListOf<Finding>()
        for (hit in searchResponse.hits) {
            val xcp = XContentFactory.xContent(XContentType.JSON)
                .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, hit.sourceAsString)
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
            val finding = Finding.parse(xcp)
            findings.add(finding)
        }

        val indexToRelatedDocIdsMap = mutableMapOf<String, MutableList<String>>()

        for (finding in findings) {
            indexToRelatedDocIdsMap.getOrPut(finding.index) { mutableListOf() }.addAll(finding.relatedDocIds)
        }

        val toTypedArray = indexToRelatedDocIdsMap.keys.stream().collect(Collectors.toList()).toTypedArray()
        val searchFindings = SearchRequest().indices(*toTypedArray)
        val queryBuilder = QueryBuilders.boolQuery()
        indexToRelatedDocIdsMap.forEach { entry ->
            queryBuilder
                .should()
                .add(
                    BoolQueryBuilder()
                        .must(MatchQueryBuilder("_index", entry.key))
                        .must(TermsQueryBuilder("_id", entry.value))
                )
        }
        searchFindings.source(SearchSourceBuilder().query(queryBuilder))
        val finalQueryResponse: SearchResponse = client.suspendUntil { client.search(searchFindings, it) }

        val indexDocIds = mutableMapOf<String, MutableList<String>>()
        for (hit in finalQueryResponse.hits) {
            indexDocIds.getOrPut(hit.index) { mutableListOf() }.add(hit.id)
        }
        return indexDocIds
    }
}

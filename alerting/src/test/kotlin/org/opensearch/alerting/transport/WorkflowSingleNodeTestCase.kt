/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope
import org.opensearch.action.support.WriteRequest
import org.opensearch.common.xcontent.json.JsonXContent
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.action.GetWorkflowRequest
import org.opensearch.commons.alerting.action.GetWorkflowResponse
import org.opensearch.commons.alerting.action.IndexWorkflowRequest
import org.opensearch.commons.alerting.action.IndexWorkflowResponse
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.alerting.model.Workflow
import org.opensearch.index.query.TermQueryBuilder
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.rest.RestRequest
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.search.fetch.subphase.FetchSourceContext

/**
 * A test that keep a singleton node started for all tests that can be used to get
 * references to Guice injectors in unit tests.
 */

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
abstract class WorkflowSingleNodeTestCase : AlertingSingleNodeTestCase() {

    protected fun searchWorkflow(id: String, indices: String = ScheduledJob.SCHEDULED_JOBS_INDEX, refresh: Boolean = true): Workflow? {
        try {
            if (refresh) refreshIndex(indices)
        } catch (e: Exception) {
            logger.warn("Could not refresh index $indices because: ${e.message}")
            return null
        }
        val ssb = SearchSourceBuilder()
        ssb.version(true)
        ssb.query(TermQueryBuilder("_id", id))
        val searchResponse = client().prepareSearch(indices).setRouting(id).setSource(ssb).get()

        return searchResponse.hits.hits.map { it ->
            val xcp = createParser(JsonXContent.jsonXContent, it.sourceRef).also { it.nextToken() }
            Workflow.parse(xcp, it.id, it.version)
        }.first()
    }

    protected fun upsertWorkflow(workflow: Workflow, id: String = Workflow.NO_ID, method: RestRequest.Method = RestRequest.Method.POST): IndexWorkflowResponse? {
        val request = IndexWorkflowRequest(
            workflowId = id,
            seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO,
            primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
            refreshPolicy = WriteRequest.RefreshPolicy.parse("true"),
            method = method,
            workflow = workflow
        )

        return client().execute(AlertingActions.INDEX_WORKFLOW_ACTION_TYPE, request).actionGet()
    }

    protected fun getWorkflowById(
        id: String,
        version: Long = 1L,
        fetchSourceContext: FetchSourceContext = FetchSourceContext.FETCH_SOURCE
    ): GetWorkflowResponse {
        return client().execute(AlertingActions.GET_WORKFLOW_ACTION_TYPE, GetWorkflowRequest(id, version, RestRequest.Method.GET, fetchSourceContext)).get()
    }
}

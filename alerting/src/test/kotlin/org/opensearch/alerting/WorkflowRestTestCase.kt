/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.apache.http.HttpEntity
import org.apache.http.HttpHeaders
import org.apache.http.entity.ContentType
import org.apache.http.entity.StringEntity
import org.apache.http.message.BasicHeader
import org.opensearch.client.RestClient
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParserUtils
import org.opensearch.common.xcontent.XContentType
import org.opensearch.common.xcontent.json.JsonXContent
import org.opensearch.commons.alerting.model.Workflow
import org.opensearch.commons.alerting.util.string
import org.opensearch.rest.RestStatus

open class WorkflowRestTestCase : AlertingRestTestCase() {

    protected fun createRandomWorkflow(refresh: Boolean = false, monitorIds: List<String>): Workflow {
        val workflow = randomWorkflow(monitorIds = monitorIds)
        return createWorkflow(workflow, refresh)
    }

    private fun createWorkflowEntityWithBackendRoles(workflow: Workflow, rbacRoles: List<String>?): HttpEntity {
        if (rbacRoles == null) {
            return workflow.toHttpEntity()
        }
        val temp = workflow.toJsonString()
        val toReplace = temp.lastIndexOf("}")
        val rbacString = rbacRoles.joinToString { "\"$it\"" }
        val jsonString = temp.substring(0, toReplace) + ", \"rbac_roles\": [$rbacString] }"
        return StringEntity(jsonString, ContentType.APPLICATION_JSON)
    }

    protected fun createWorkflowWithClient(
        client: RestClient,
        workflow: Workflow,
        rbacRoles: List<String>? = null,
        refresh: Boolean = true
    ): Workflow {
        val response = client.makeRequest(
            "POST", "$WORKFLOW_ALERTING_BASE_URI?refresh=$refresh", emptyMap(),
            createWorkflowEntityWithBackendRoles(workflow, rbacRoles)
        )
        assertEquals("Unable to create a new monitor", RestStatus.CREATED, response.restStatus())

        val workflowJson = JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE,
            response.entity.content
        ).map()
        assertUserNull(workflowJson as HashMap<String, Any>)
        return workflow.copy(id = workflowJson["_id"] as String)
    }

    protected fun createWorkflow(workflow: Workflow, refresh: Boolean = true): Workflow {
        return createWorkflowWithClient(client(), workflow, emptyList(), refresh)
    }

    protected fun Workflow.toHttpEntity(): HttpEntity {
        return StringEntity(toJsonString(), ContentType.APPLICATION_JSON)
    }

    private fun Workflow.toJsonString(): String {
        val builder = XContentFactory.jsonBuilder()
        return shuffleXContent(toXContent(builder, ToXContent.EMPTY_PARAMS)).string()
    }

    protected fun getWorkflow(workflowId: String, header: BasicHeader = BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")): Workflow {
        val response = client().makeRequest("GET", "$WORKFLOW_ALERTING_BASE_URI/$workflowId", null, header)
        assertEquals("Unable to get workflow $workflowId", RestStatus.OK, response.restStatus())

        val parser = createParser(XContentType.JSON.xContent(), response.entity.content)
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser)

        lateinit var id: String
        var version: Long = 0
        lateinit var workflow: Workflow

        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            parser.nextToken()

            when (parser.currentName()) {
                "_id" -> id = parser.text()
                "_version" -> version = parser.longValue()
                "workflow" -> workflow = Workflow.parse(parser)
            }
        }

        assertUserNull(workflow)
        return workflow.copy(id = id, version = version)
    }

    protected fun Workflow.relativeUrl() = "$WORKFLOW_ALERTING_BASE_URI/$id"
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchException
import org.opensearch.action.DocWriteRequest
import org.opensearch.action.DocWriteResponse
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.index.IndexResponse
import org.opensearch.action.support.WriteRequest
import org.opensearch.alerting.model.WorkflowMetadata
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.util.AlertingException
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentParserUtils
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.model.CompositeInput
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.alerting.model.Workflow
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.rest.RestStatus
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.UUID

object WorkflowMetadataService :
    CoroutineScope by CoroutineScope(SupervisorJob() + Dispatchers.Default + CoroutineName("WorkflowMetadataService")) {
    private val log = LogManager.getLogger(this::class.java)

    private lateinit var client: Client
    private lateinit var xContentRegistry: NamedXContentRegistry
    private lateinit var clusterService: ClusterService
    private lateinit var settings: Settings

    @Volatile private lateinit var indexTimeout: TimeValue

    fun initialize(
        client: Client,
        clusterService: ClusterService,
        xContentRegistry: NamedXContentRegistry,
        settings: Settings
    ) {
        this.clusterService = clusterService
        this.client = client
        this.xContentRegistry = xContentRegistry
        this.settings = settings
        this.indexTimeout = AlertingSettings.INDEX_TIMEOUT.get(settings)
        this.clusterService.clusterSettings.addSettingsUpdateConsumer(AlertingSettings.INDEX_TIMEOUT) { indexTimeout = it }
    }

    @Suppress("ComplexMethod", "ReturnCount")
    suspend fun upsertWorkflowMetadata(metadata: WorkflowMetadata, updating: Boolean): WorkflowMetadata {
        try {
            val indexRequest = IndexRequest(ScheduledJob.SCHEDULED_JOBS_INDEX)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .source(metadata.toXContent(XContentFactory.jsonBuilder(), ToXContent.MapParams(mapOf("with_type" to "true"))))
                .id(metadata.id)
                .routing(metadata.workflowId)
                .timeout(indexTimeout)

            if (updating) {
                indexRequest.id(metadata.id)
            } else {
                indexRequest.opType(DocWriteRequest.OpType.CREATE)
            }
            val response: IndexResponse = client.suspendUntil { index(indexRequest, it) }
            when (response.result) {
                DocWriteResponse.Result.DELETED, DocWriteResponse.Result.NOOP, DocWriteResponse.Result.NOT_FOUND, null -> {
                    val failureReason = "The upsert metadata call failed with a ${response.result?.lowercase} result"
                    log.error(failureReason)
                    throw AlertingException(failureReason, RestStatus.INTERNAL_SERVER_ERROR, IllegalStateException(failureReason))
                }
                DocWriteResponse.Result.CREATED, DocWriteResponse.Result.UPDATED -> {
                    log.debug("Successfully upserted WorkflowMetadata:${metadata.id} ")
                }
            }
            return metadata
        } catch (e: Exception) {
            // If the update is set to false and id is set conflict exception will be thrown
            if (e is OpenSearchException && e.status() == RestStatus.CONFLICT && !updating) {
                log.debug("Metadata already exist. Instead of creating new, updating existing metadata will be performed")
                return upsertWorkflowMetadata(metadata, true)
            }
            log.error("Error saving metadata", e)
            throw AlertingException.wrap(e)
        }
    }

    suspend fun getOrCreateWorkflowMetadata(
        workflow: Workflow,
        skipIndex: Boolean = false,
        executionId: String
    ): Pair<WorkflowMetadata, Boolean> {
        try {
            val created = true
            val metadata = getWorkflowMetadata(workflow)
            return if (metadata != null) {
                metadata to !created
            } else {
                val newMetadata = createNewWorkflowMetadata(workflow, executionId, skipIndex)
                if (skipIndex) {
                    newMetadata to created
                } else {
                    upsertWorkflowMetadata(newMetadata, updating = false) to created
                }
            }
        } catch (e: Exception) {
            throw AlertingException.wrap(e)
        }
    }

    private suspend fun getWorkflowMetadata(workflow: Workflow): WorkflowMetadata? {
        try {
            val metadataId = WorkflowMetadata.getId(workflow.id)
            val getRequest = GetRequest(ScheduledJob.SCHEDULED_JOBS_INDEX, metadataId).routing(workflow.id)

            val getResponse: GetResponse = client.suspendUntil { get(getRequest, it) }
            return if (getResponse.isExists) {
                val xcp = XContentHelper.createParser(
                    xContentRegistry,
                    LoggingDeprecationHandler.INSTANCE,
                    getResponse.sourceAsBytesRef,
                    XContentType.JSON
                )
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
                WorkflowMetadata.parse(xcp)
            } else {
                null
            }
        } catch (e: Exception) {
            if (e.message?.contains("no such index") == true) {
                return null
            } else {
                throw AlertingException.wrap(e)
            }
        }
    }

    private fun createNewWorkflowMetadata(workflow: Workflow, executionId: String, isTempWorkflow: Boolean): WorkflowMetadata {
        val id = if (isTempWorkflow) "${LocalDateTime.now(ZoneOffset.UTC)}${UUID.randomUUID()}" else workflow.id
        return WorkflowMetadata(
            id = WorkflowMetadata.getId(id),
            workflowId = workflow.id,
            monitorIds = (workflow.inputs[0] as CompositeInput).getMonitorIds(),
            latestRunTime = Instant.now(),
            latestExecutionId = executionId
        )
    }
}

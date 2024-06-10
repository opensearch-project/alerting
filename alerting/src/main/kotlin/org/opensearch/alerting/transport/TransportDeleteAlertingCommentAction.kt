/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.ActionRequest
import org.opensearch.action.delete.DeleteRequest
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.comments.CommentsIndices.Companion.ALL_COMMENTS_INDEX_PATTERN
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.action.DeleteCommentRequest
import org.opensearch.commons.alerting.action.DeleteCommentResponse
import org.opensearch.commons.alerting.model.Comment
import org.opensearch.commons.alerting.util.AlertingException
import org.opensearch.commons.authuser.User
import org.opensearch.commons.utils.recreateObject
import org.opensearch.core.action.ActionListener
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParserUtils
import org.opensearch.index.query.QueryBuilders
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService

private val scope: CoroutineScope = CoroutineScope(Dispatchers.IO)
private val log = LogManager.getLogger(TransportDeleteAlertingCommentAction::class.java)

class TransportDeleteAlertingCommentAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    settings: Settings,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<ActionRequest, DeleteCommentResponse>(
    AlertingActions.DELETE_COMMENT_ACTION_NAME, transportService, actionFilters, ::DeleteCommentRequest
),
    SecureTransportAction {

    @Volatile private var alertingCommentsEnabled = AlertingSettings.ALERTING_COMMENTS_ENABLED.get(settings)
    @Volatile override var filterByEnabled = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(AlertingSettings.ALERTING_COMMENTS_ENABLED) {
            alertingCommentsEnabled = it
        }
        listenFilterBySettingChange(clusterService)
    }

    override fun doExecute(task: Task, request: ActionRequest, actionListener: ActionListener<DeleteCommentResponse>) {
        // validate feature flag enabled
        if (!alertingCommentsEnabled) {
            actionListener.onFailure(
                AlertingException.wrap(
                    OpenSearchStatusException("Comments for Alerting is currently disabled", RestStatus.FORBIDDEN),
                )
            )
            return
        }

        val transformedRequest = request as? DeleteCommentRequest
            ?: recreateObject(request) { DeleteCommentRequest(it) }

        val user = readUserFromThreadContext(client)

        if (!validateUserBackendRoles(user, actionListener)) {
            return
        }
        scope.launch {
            DeleteCommentHandler(
                client,
                actionListener,
                user,
                transformedRequest.commentId
            ).resolveUserAndStart()
        }
    }

    inner class DeleteCommentHandler(
        private val client: Client,
        private val actionListener: ActionListener<DeleteCommentResponse>,
        private val user: User?,
        private val commentId: String
    ) {

        private var sourceIndex: String? = null

        suspend fun resolveUserAndStart() {
            try {
                val comment = getComment()

                if (sourceIndex == null) {
                    actionListener.onFailure(
                        AlertingException(
                            "Could not resolve the index the given Comment came from",
                            RestStatus.INTERNAL_SERVER_ERROR,
                            IllegalStateException()
                        )
                    )
                }

                // if user is null because security plugin is not installed, anyone can delete any comment
                // otherwise, only allow comment deletion if the deletion requester is the same as the comment's author
                // or if the user is Admin
                val canDelete = user == null || user.name == comment.user?.name || isAdmin(user)

                val deleteRequest = DeleteRequest(sourceIndex, commentId)

                if (canDelete) {
                    log.debug("Deleting the comment with id ${deleteRequest.id()}")
                    val deleteResponse = client.suspendUntil { delete(deleteRequest, it) }
                    actionListener.onResponse(DeleteCommentResponse(deleteResponse.id))
                } else {
                    actionListener.onFailure(
                        AlertingException("Not allowed to delete this comment!", RestStatus.FORBIDDEN, IllegalStateException())
                    )
                }
            } catch (t: Exception) {
                log.error("Failed to delete comment $commentId", t)
                actionListener.onFailure(AlertingException.wrap(t))
            }
        }

        private suspend fun getComment(): Comment {
            val queryBuilder = QueryBuilders
                .boolQuery()
                .must(QueryBuilders.termsQuery("_id", commentId))
            val searchSourceBuilder =
                SearchSourceBuilder()
                    .version(true)
                    .seqNoAndPrimaryTerm(true)
                    .query(queryBuilder)
            val searchRequest = SearchRequest()
                .source(searchSourceBuilder)
                .indices(ALL_COMMENTS_INDEX_PATTERN)

            val searchResponse: SearchResponse = client.suspendUntil { search(searchRequest, it) }
            if (searchResponse.hits.totalHits.value == 0L) {
                actionListener.onFailure(
                    AlertingException.wrap(
                        OpenSearchStatusException("Comment not found", RestStatus.NOT_FOUND)
                    )
                )
            }
            val comments = searchResponse.hits.map { hit ->
                val xcp = XContentHelper.createParser(
                    NamedXContentRegistry.EMPTY,
                    LoggingDeprecationHandler.INSTANCE,
                    hit.sourceRef,
                    XContentType.JSON
                )
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
                val comment = Comment.parse(xcp, hit.id)
                sourceIndex = hit.index
                comment
            }

            return comments[0] // we searched on Comment ID, there should only be one Comment in the List
        }
    }
}

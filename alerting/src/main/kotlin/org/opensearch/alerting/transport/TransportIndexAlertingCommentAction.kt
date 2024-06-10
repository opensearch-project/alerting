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
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.index.IndexResponse
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.action.support.WriteRequest
import org.opensearch.alerting.alerts.AlertIndices
import org.opensearch.alerting.comments.CommentsIndices
import org.opensearch.alerting.comments.CommentsIndices.Companion.COMMENTS_HISTORY_WRITE_INDEX
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.settings.AlertingSettings.Companion.ALERTING_COMMENTS_ENABLED
import org.opensearch.alerting.settings.AlertingSettings.Companion.COMMENTS_MAX_CONTENT_SIZE
import org.opensearch.alerting.settings.AlertingSettings.Companion.INDEX_TIMEOUT
import org.opensearch.alerting.settings.AlertingSettings.Companion.MAX_COMMENTS_PER_ALERT
import org.opensearch.alerting.util.CommentsUtils
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.action.IndexCommentRequest
import org.opensearch.commons.alerting.action.IndexCommentResponse
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.commons.alerting.model.Comment
import org.opensearch.commons.alerting.util.AlertingException
import org.opensearch.commons.authuser.User
import org.opensearch.commons.utils.recreateObject
import org.opensearch.core.action.ActionListener
import org.opensearch.core.common.io.stream.NamedWriteableRegistry
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParserUtils
import org.opensearch.index.query.QueryBuilders
import org.opensearch.rest.RestRequest
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import java.lang.IllegalArgumentException
import java.time.Instant

private val log = LogManager.getLogger(TransportIndexMonitorAction::class.java)
private val scope: CoroutineScope = CoroutineScope(Dispatchers.IO)

class TransportIndexAlertingCommentAction
@Inject
constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val commentsIndices: CommentsIndices,
    val clusterService: ClusterService,
    val settings: Settings,
    val xContentRegistry: NamedXContentRegistry,
    val namedWriteableRegistry: NamedWriteableRegistry,
) : HandledTransportAction<ActionRequest, IndexCommentResponse>(
    AlertingActions.INDEX_COMMENT_ACTION_NAME,
    transportService,
    actionFilters,
    ::IndexCommentRequest,
),
    SecureTransportAction {

    @Volatile private var alertingCommentsEnabled = ALERTING_COMMENTS_ENABLED.get(settings)
    @Volatile private var commentsMaxContentSize = COMMENTS_MAX_CONTENT_SIZE.get(settings)
    @Volatile private var maxCommentsPerAlert = MAX_COMMENTS_PER_ALERT.get(settings)
    @Volatile private var indexTimeout = INDEX_TIMEOUT.get(settings)

    @Volatile override var filterByEnabled = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(ALERTING_COMMENTS_ENABLED) { alertingCommentsEnabled = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(COMMENTS_MAX_CONTENT_SIZE) { commentsMaxContentSize = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(MAX_COMMENTS_PER_ALERT) { maxCommentsPerAlert = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(INDEX_TIMEOUT) { indexTimeout = it }
        listenFilterBySettingChange(clusterService)
    }

    override fun doExecute(
        task: Task,
        request: ActionRequest,
        actionListener: ActionListener<IndexCommentResponse>,
    ) {
        // validate feature flag enabled
        if (!alertingCommentsEnabled) {
            actionListener.onFailure(
                AlertingException.wrap(
                    OpenSearchStatusException("Comments for Alerting is currently disabled", RestStatus.FORBIDDEN),
                )
            )
            return
        }

        val transformedRequest =
            request as? IndexCommentRequest
                ?: recreateObject(request, namedWriteableRegistry) {
                    IndexCommentRequest(it)
                }

        // validate comment content size
        if (transformedRequest.content.length > commentsMaxContentSize) {
            actionListener.onFailure(
                AlertingException.wrap(
                    IllegalArgumentException("Comment content exceeds max length of $commentsMaxContentSize characters"),
                )
            )
            return
        }

        val user = readUserFromThreadContext(client)

        client.threadPool().threadContext.stashContext().use {
            scope.launch {
                IndexCommentHandler(client, actionListener, transformedRequest, user).start()
            }
        }
    }

    inner class IndexCommentHandler(
        private val client: Client,
        private val actionListener: ActionListener<IndexCommentResponse>,
        private val request: IndexCommentRequest,
        private val user: User?,
    ) {
        suspend fun start() {
            commentsIndices.createOrUpdateInitialCommentsHistoryIndex()
            if (request.method == RestRequest.Method.PUT) {
                updateComment()
            } else {
                indexComment()
            }
        }

        private suspend fun indexComment() {
            val alert = getAlert()
            if (alert == null) {
                actionListener.onFailure(
                    AlertingException.wrap(
                        OpenSearchStatusException("Alert not found", RestStatus.NOT_FOUND),
                    )
                )
                return
            }

            val numCommentsOnThisAlert = CommentsUtils.getCommentIDsByAlertIDs(client, listOf(alert.id)).size
            if (numCommentsOnThisAlert >= maxCommentsPerAlert) {
                actionListener.onFailure(
                    AlertingException.wrap(
                        IllegalArgumentException(
                            "This request would create more than the allowed number of Comments" +
                                "for this Alert: $maxCommentsPerAlert"
                        )
                    )
                )
                return
            }

            log.debug("checking user permissions in index comment")
            checkUserPermissionsWithResource(user, alert.monitorUser, actionListener, "monitor", alert.monitorId)

            val comment = Comment(entityId = request.entityId, content = request.content, createdTime = Instant.now(), user = user)

            val indexRequest =
                IndexRequest(COMMENTS_HISTORY_WRITE_INDEX)
                    .source(comment.toXContentWithUser(XContentFactory.jsonBuilder()))
                    .setIfSeqNo(request.seqNo)
                    .setIfPrimaryTerm(request.primaryTerm)
                    .timeout(indexTimeout)
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
//                    .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)

            log.debug("Creating new comment: ${comment.toXContentWithUser(XContentFactory.jsonBuilder())}")

            try {
                val indexResponse: IndexResponse = client.suspendUntil { client.index(indexRequest, it) }
                val failureReasons = checkShardsFailure(indexResponse)
                if (failureReasons != null) {
                    actionListener.onFailure(
                        AlertingException.wrap(OpenSearchStatusException(failureReasons.toString(), indexResponse.status())),
                    )
                    return
                }

                actionListener.onResponse(
                    IndexCommentResponse(indexResponse.id, indexResponse.seqNo, indexResponse.primaryTerm, comment)
                )
            } catch (t: Exception) {
                actionListener.onFailure(AlertingException.wrap(t))
            }
        }

        private suspend fun updateComment() {
            val currentComment = getComment()
            if (currentComment == null) {
                actionListener.onFailure(
                    AlertingException.wrap(
                        OpenSearchStatusException("Comment not found", RestStatus.NOT_FOUND),
                    ),
                )
                return
            }

            // check that the user has permissions to edit the comment. user can edit comment if
            // - user is Admin
            // - user is the author of the comment
            if (user != null && !isAdmin(user) && user.name != currentComment.user?.name) {
                actionListener.onFailure(
                    AlertingException.wrap(
                        OpenSearchStatusException(
                            "Comment can only be edited by Admin or author of comment",
                            RestStatus.FORBIDDEN,
                        ),
                    ),
                )
                return
            }

            // retains everything from the original comment except content and lastUpdatedTime
            val requestComment = currentComment.copy(content = request.content, lastUpdatedTime = Instant.now())

            val indexRequest =
                IndexRequest(COMMENTS_HISTORY_WRITE_INDEX)
                    .source(requestComment.toXContentWithUser(XContentFactory.jsonBuilder()))
                    .id(requestComment.id)
                    .setIfSeqNo(request.seqNo)
                    .setIfPrimaryTerm(request.primaryTerm)
                    .timeout(indexTimeout)
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
//                    .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)

            log.debug(
                "Updating comment, ${currentComment.id}, from: " +
                    "${currentComment.content} to: " +
                    requestComment.content,
            )

            try {
                val indexResponse: IndexResponse = client.suspendUntil { client.index(indexRequest, it) }
                val failureReasons = checkShardsFailure(indexResponse)
                if (failureReasons != null) {
                    actionListener.onFailure(
                        AlertingException.wrap(OpenSearchStatusException(failureReasons.toString(), indexResponse.status())),
                    )
                    return
                }

                actionListener.onResponse(
                    IndexCommentResponse(
                        indexResponse.id,
                        indexResponse.seqNo,
                        indexResponse.primaryTerm,
                        requestComment,
                    ),
                )
            } catch (t: Exception) {
                actionListener.onFailure(AlertingException.wrap(t))
            }
        }

        private suspend fun getAlert(): Alert? {
            // need to validate the existence of the Alert that user is trying to add Comment to.
            // Also need to check if user has permissions to add a Comment to the passed in Alert. To do this,
            // we retrieve the Alert to get its associated monitor user, and use that to
            // check if they have permissions to the Monitor that generated the Alert
            val queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.termsQuery("_id", listOf(request.entityId)))
            val searchSourceBuilder =
                SearchSourceBuilder()
                    .version(true)
                    .seqNoAndPrimaryTerm(true)
                    .query(queryBuilder)

            // search all alerts, since user might want to create a comment
            // on a completed alert
            val searchRequest =
                SearchRequest()
                    .indices(AlertIndices.ALL_ALERT_INDEX_PATTERN)
                    .source(searchSourceBuilder)

            val searchResponse: SearchResponse = client.suspendUntil { search(searchRequest, it) }
            val alerts = searchResponse.hits.map { hit ->
                val xcp = XContentHelper.createParser(
                    NamedXContentRegistry.EMPTY,
                    LoggingDeprecationHandler.INSTANCE,
                    hit.sourceRef,
                    XContentType.JSON
                )
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
                val alert = Alert.parse(xcp, hit.id, hit.version)
                alert
            }

            if (alerts.isEmpty()) return null
            if (alerts.size > 1) {
                actionListener.onFailure(
                    AlertingException.wrap(IllegalStateException("Multiple alerts were found with the same ID")),
                )
                return null
            }

            return alerts[0]
        }

        private suspend fun getComment(): Comment? {
            // need to validate the existence of the Alert that user is trying to add Comment to.
            // Also need to check if user has permissions to add a Comment to the passed in Alert. To do this,
            // we retrieve the Alert to get its associated monitor user, and use that to
            // check if they have permissions to the Monitor that generated the Alert
            val queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.termsQuery("_id", listOf(request.commentId)))
            val searchSourceBuilder =
                SearchSourceBuilder()
                    .version(true)
                    .seqNoAndPrimaryTerm(true)
                    .query(queryBuilder)

            // search all alerts, since user might want to create a comment
            // on a completed alert
            val searchRequest =
                SearchRequest()
                    .indices(CommentsIndices.ALL_COMMENTS_INDEX_PATTERN)
                    .source(searchSourceBuilder)

            val searchResponse: SearchResponse = client.suspendUntil { search(searchRequest, it) }
            val comments = searchResponse.hits.map { hit ->
                val xcp = XContentHelper.createParser(
                    NamedXContentRegistry.EMPTY,
                    LoggingDeprecationHandler.INSTANCE,
                    hit.sourceRef,
                    XContentType.JSON
                )
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
                val comment = Comment.parse(xcp, hit.id)
                comment
            }

            if (comments.isEmpty()) return null
            if (comments.size > 1) {
                actionListener.onFailure(
                    AlertingException.wrap(IllegalStateException("Multiple comments were found with the same ID")),
                )
                return null
            }

            return comments[0]
        }

        private fun checkShardsFailure(response: IndexResponse): String? {
            val failureReasons = StringBuilder()
            if (response.shardInfo.failed > 0) {
                response.shardInfo.failures.forEach { entry ->
                    failureReasons.append(entry.reason())
                }
                return failureReasons.toString()
            }
            return null
        }
    }
}

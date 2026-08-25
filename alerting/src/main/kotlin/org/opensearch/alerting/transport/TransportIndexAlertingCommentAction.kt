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
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.ResourceSharingClientAccessor
import org.opensearch.alerting.ResourceSharingUtils
import org.opensearch.alerting.alerts.AlertIndices
import org.opensearch.alerting.comments.CommentsIndices
import org.opensearch.alerting.comments.CommentsIndices.Companion.COMMENTS_HISTORY_WRITE_INDEX
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.settings.AlertingSettings.Companion.ALERTING_COMMENTS_ENABLED
import org.opensearch.alerting.settings.AlertingSettings.Companion.COMMENTS_MAX_CONTENT_SIZE
import org.opensearch.alerting.settings.AlertingSettings.Companion.INDEX_TIMEOUT
import org.opensearch.alerting.settings.AlertingSettings.Companion.MAX_COMMENTS_PER_ALERT
import org.opensearch.alerting.util.CommentsUtils
import org.opensearch.alerting.util.await
import org.opensearch.alerting.util.putDataObjectStashed
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.action.IndexCommentRequest
import org.opensearch.commons.alerting.action.IndexCommentResponse
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.commons.alerting.model.Comment
import org.opensearch.commons.alerting.util.AlertingException
import org.opensearch.commons.authuser.User
import org.opensearch.commons.utils.TenantContext
import org.opensearch.commons.utils.currentTenantId
import org.opensearch.commons.utils.recreateObject
import org.opensearch.core.action.ActionListener
import org.opensearch.core.common.io.stream.NamedWriteableRegistry
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParserUtils
import org.opensearch.index.query.QueryBuilders
import org.opensearch.remote.metadata.client.PutDataObjectRequest
import org.opensearch.remote.metadata.client.SdkClient
import org.opensearch.remote.metadata.client.SearchDataObjectRequest
import org.opensearch.rest.RestRequest
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.Client
import java.lang.IllegalArgumentException
import java.time.Instant
import kotlin.coroutines.resume
import kotlin.coroutines.suspendCoroutine

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
    val sdkClient: SdkClient,
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
    @Volatile override var filterByAccessStrategy = AlertingSettings.FILTER_BY_BACKEND_ROLES_ACCESS_STRATEGY.get(settings)

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

        // validate the request is for the correct entity type
        if (transformedRequest.entityType != "alert") {
            actionListener.onFailure(
                AlertingException.wrap(
                    IllegalArgumentException(
                        "Index comment request is for wrong entity type, expected alert, got ${transformedRequest.entityType}"
                    )
                )
            )
            return
        }

        val user = readUserFromThreadContext(client)

        val tenantId = client.threadPool().threadContext.getHeader(AlertingPlugin.TENANT_ID_HEADER)
        // Capture the caller's context before stashing. Under resource sharing the subordinate-
        // resource access check ([ResourceSharingClient.verifyAccess]) resolves the caller from the
        // authenticated-user header in ThreadContext; run under the stashed (empty) context it would
        // see no user and deny every comment.
        val storedContext = client.threadPool().threadContext.newStoredContext(false)
        // Stash the caller's transient auth so the alert-fetch and comment-write flow runs under
        // the plugin subject — comments target system indices where callers rarely have direct
        // permissions. When resource-sharing is enabled the shard-level ResourceIndexListener
        // still sees the caller via the persistent auth header (persistents survive stashContext),
        // so it can record the comment share entry with createdBy=<caller>.
        client.threadPool().threadContext.stashContext().use {
            scope.launch(TenantContext(tenantId)) {
                IndexCommentHandler(client, actionListener, transformedRequest, user, storedContext).start()
            }
        }
    }

    inner class IndexCommentHandler(
        private val client: Client,
        private val actionListener: ActionListener<IndexCommentResponse>,
        private val request: IndexCommentRequest,
        private val user: User?,
        private val storedContext: ThreadContext.StoredContext? = null,
    ) {
        suspend fun start() {
            // Comments-history index management (exists/create/put-mapping) is a system-index
            // operation and must run on the plugin subject, not the caller: under resource sharing a
            // non-admin caller has no direct privileges on the comments-history index, so running the
            // exists() check as the caller throws and the request never completes.
            client.threadPool().threadContext.stashContext().use {
                commentsIndices.createOrUpdateInitialCommentsHistoryIndex()
            }
            if (request.method == RestRequest.Method.PUT) {
                updateComment()
            } else {
                indexComment()
            }
        }

        private suspend fun indexComment() {
            val alert = getAlert() ?: return

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

            val useRsc = ResourceSharingUtils.shouldUseResourceAuthz(ResourceSharingUtils.MONITOR_RESOURCE_TYPE)
            if (useRsc) {
                // Comments are a subordinate resource: the comment request targets the comments index
                // (not the monitor), so the security plugin's ResourceAccessEvaluator does not gate it.
                // Gate the create explicitly on the caller's access to the parent monitor. The comment
                // write action is only in the read-write / full-access level groups, so a read-only
                // share (or no share) is denied.
                if (!callerCanAccessMonitor(alert.monitorId, AlertingActions.INDEX_COMMENT_ACTION_NAME)) {
                    actionListener.onFailure(
                        AlertingException.wrap(
                            OpenSearchStatusException(
                                "User does not have permission to add a comment to this alert",
                                RestStatus.FORBIDDEN,
                            )
                        )
                    )
                    return
                }
            } else {
                log.debug("checking user permissions in index comment")
                if (!checkUserPermissionsWithResource(user, alert.monitorUser, actionListener, "monitor", alert.monitorId)) {
                    return
                }
            }

            val comment = Comment(
                entityId = request.entityId,
                entityType = request.entityType,
                content = request.content,
                createdTime = Instant.now(),
                user = user
            )

            val tenantId = currentTenantId()
            val wrappedComment = ToXContentObject { builder, params ->
                comment.toXContentWithUser(builder)
            }
            val putRequest = PutDataObjectRequest.builder()
                .index(COMMENTS_HISTORY_WRITE_INDEX)
                .tenantId(tenantId)
                .dataObject(wrappedComment)
                .build()

            log.debug("Creating new comment")

            try {
                val putResponse = sdkClient.putDataObjectStashed(putRequest, client.threadPool().threadContext)
                val seqNo = putResponse.indexResponse()?.seqNo ?: 0L
                val primaryTerm = putResponse.indexResponse()?.primaryTerm ?: 0L
                actionListener.onResponse(
                    IndexCommentResponse(putResponse.id(), seqNo, primaryTerm, comment)
                )
            } catch (t: Exception) {
                log.error("Failed to create comment", t)
                actionListener.onFailure(AlertingException.wrap(t))
            }
        }

        private suspend fun updateComment() {
            val currentComment = getComment() ?: return

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

            val tenantId = currentTenantId()
            val wrappedComment = ToXContentObject { builder, params ->
                requestComment.toXContentWithUser(builder)
            }
            val putRequest = PutDataObjectRequest.builder()
                .index(COMMENTS_HISTORY_WRITE_INDEX)
                .id(requestComment.id)
                .tenantId(tenantId)
                .ifSeqNo(request.seqNo)
                .ifPrimaryTerm(request.primaryTerm)
                .overwriteIfExists(true)
                .dataObject(wrappedComment)
                .build()

            log.debug("Updating comment, ${currentComment.id}")

            try {
                val putResponse = sdkClient.putDataObjectStashed(putRequest, client.threadPool().threadContext)
                actionListener.onResponse(
                    IndexCommentResponse(
                        putResponse.id(),
                        putResponse.indexResponse()?.seqNo ?: 0L,
                        putResponse.indexResponse()?.primaryTerm ?: 0L,
                        requestComment,
                    ),
                )
            } catch (t: Exception) {
                log.error("Failed to update comment ${currentComment.id}", t)
                actionListener.onFailure(AlertingException.wrap(t))
            }
        }

        /**
         * Under resource sharing, verify the caller has [action] on the parent [monitorId] via the
         * [ResourceSharingClient]. The comment flow runs on the stashed plugin subject, so restore
         * the caller's context around the call — verifyAccess resolves the user from the
         * authenticated-user header in ThreadContext. Returns false (deny) if the client is
         * unavailable or the lookup fails.
         */
        private suspend fun callerCanAccessMonitor(monitorId: String, action: String): Boolean {
            val rsc = ResourceSharingClientAccessor.getResourceSharingClient()
                as? org.opensearch.security.spi.resources.client.ResourceSharingClient ?: return false
            return suspendCoroutine { cont ->
                client.threadPool().threadContext.stashContext().use {
                    storedContext?.restore()
                    rsc.verifyAccess(
                        monitorId,
                        ResourceSharingUtils.MONITOR_RESOURCE_TYPE,
                        action,
                        object : ActionListener<Boolean> {
                            override fun onResponse(hasAccess: Boolean) = cont.resume(hasAccess)
                            override fun onFailure(e: Exception) = cont.resume(false)
                        }
                    )
                }
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

            val tenantId = currentTenantId()
            val sdkSearchRequest = SearchDataObjectRequest.builder()
                .indices(AlertIndices.ALL_ALERT_INDEX_PATTERN)
                .tenantId(tenantId)
                .searchSourceBuilder(searchSourceBuilder)
                .build()
            val sdkResponse = sdkClient.searchDataObjectAsync(sdkSearchRequest).await()
            val searchResponse = sdkResponse.searchResponse()
            if (searchResponse == null) {
                log.error("Failed to search for alert ${request.entityId}")
                actionListener.onFailure(
                    AlertingException.wrap(OpenSearchStatusException("Alert not found", RestStatus.NOT_FOUND))
                )
                return null
            }
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

            if (alerts.isEmpty()) {
                actionListener.onFailure(
                    AlertingException.wrap(
                        OpenSearchStatusException("Alert not found", RestStatus.NOT_FOUND),
                    )
                )
                return null
            } else if (alerts.size > 1) {
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

            val tenantId = currentTenantId()
            val sdkSearchRequest = SearchDataObjectRequest.builder()
                .indices(CommentsIndices.ALL_COMMENTS_INDEX_PATTERN)
                .tenantId(tenantId)
                .searchSourceBuilder(searchSourceBuilder)
                .build()
            val sdkResponse = sdkClient.searchDataObjectAsync(sdkSearchRequest).await()
            val searchResponse = sdkResponse.searchResponse()
            if (searchResponse == null) {
                log.error("Failed to search for comment ${request.commentId}")
                actionListener.onFailure(
                    AlertingException.wrap(OpenSearchStatusException("Comment not found", RestStatus.NOT_FOUND))
                )
                return null
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
                comment
            }

            if (comments.isEmpty()) {
                actionListener.onFailure(
                    AlertingException.wrap(
                        OpenSearchStatusException("Comment not found", RestStatus.NOT_FOUND),
                    ),
                )
                return null
            } else if (comments.size > 1) {
                actionListener.onFailure(
                    AlertingException.wrap(IllegalStateException("Multiple comments were found with the same ID")),
                )
                return null
            }

            return comments[0]
        }
    }
}

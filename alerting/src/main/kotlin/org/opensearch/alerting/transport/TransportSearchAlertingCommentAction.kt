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
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.alerts.AlertIndices.Companion.ALL_ALERT_INDEX_PATTERN
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.util.use
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.action.SearchCommentRequest
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
import org.opensearch.index.query.BoolQueryBuilder
import org.opensearch.index.query.QueryBuilders
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import java.io.IOException

private val log = LogManager.getLogger(TransportSearchAlertingCommentAction::class.java)
private val scope: CoroutineScope = CoroutineScope(Dispatchers.IO)

class TransportSearchAlertingCommentAction @Inject constructor(
    transportService: TransportService,
    val settings: Settings,
    val client: Client,
    clusterService: ClusterService,
    actionFilters: ActionFilters,
    val namedWriteableRegistry: NamedWriteableRegistry
) : HandledTransportAction<ActionRequest, SearchResponse>(
    AlertingActions.SEARCH_COMMENTS_ACTION_NAME, transportService, actionFilters, ::SearchRequest
),
    SecureTransportAction {

    @Volatile private var alertingCommentsEnabled = AlertingSettings.ALERTING_COMMENTS_ENABLED.get(settings)
    @Volatile override var filterByEnabled: Boolean = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)
    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(AlertingSettings.ALERTING_COMMENTS_ENABLED) {
            alertingCommentsEnabled = it
        }
        listenFilterBySettingChange(clusterService)
    }

    override fun doExecute(task: Task, request: ActionRequest, actionListener: ActionListener<SearchResponse>) {
        // validate feature flag enabled
        if (!alertingCommentsEnabled) {
            actionListener.onFailure(
                AlertingException.wrap(
                    OpenSearchStatusException("Comments for Alerting is currently disabled", RestStatus.FORBIDDEN),
                )
            )
            return
        }

        val transformedRequest = request as? SearchCommentRequest
            ?: recreateObject(request, namedWriteableRegistry) {
                SearchCommentRequest(it)
            }

        val searchSourceBuilder = transformedRequest.searchRequest.source()
            .seqNoAndPrimaryTerm(true)
            .version(true)
        val queryBuilder = if (searchSourceBuilder.query() == null) BoolQueryBuilder()
        else QueryBuilders.boolQuery().must(searchSourceBuilder.query())

        searchSourceBuilder.query(queryBuilder)
            .seqNoAndPrimaryTerm(true)
            .version(true)

        val user = readUserFromThreadContext(client)
        client.threadPool().threadContext.stashContext().use {
            scope.launch {
                resolve(transformedRequest, actionListener, user)
            }
        }
    }

    suspend fun resolve(searchCommentRequest: SearchCommentRequest, actionListener: ActionListener<SearchResponse>, user: User?) {
        if (user == null) {
            // user is null when: 1/ security is disabled. 2/when user is super-admin.
            search(searchCommentRequest.searchRequest, actionListener)
        } else if (!doFilterForUser(user)) {
            // security is enabled and filterby is disabled.
            search(searchCommentRequest.searchRequest, actionListener)
        } else {
            // security is enabled and filterby is enabled.
            try {
                log.debug("Filtering result by: {}", user.backendRoles)

                // first retrieve all Alert IDs current User can see after filtering by backend roles
                val alertIDs = getFilteredAlertIDs(user)

                // then filter the returned Comments based on the Alert IDs they're allowed to see
                val queryBuilder = searchCommentRequest.searchRequest.source().query() as BoolQueryBuilder
                searchCommentRequest.searchRequest.source().query(
                    queryBuilder.filter(
                        QueryBuilders.termsQuery(Comment.ENTITY_ID_FIELD, alertIDs)
                    )
                )

                search(searchCommentRequest.searchRequest, actionListener)
            } catch (ex: IOException) {
                actionListener.onFailure(AlertingException.wrap(ex))
            }
        }
    }

    fun search(searchRequest: SearchRequest, actionListener: ActionListener<SearchResponse>) {
        client.search(
            searchRequest,
            object : ActionListener<SearchResponse> {
                override fun onResponse(response: SearchResponse) {
                    actionListener.onResponse(response)
                }

                override fun onFailure(t: Exception) {
                    actionListener.onFailure(AlertingException.wrap(t))
                }
            }
        )
    }

    // retrieve the IDs of all Alerts after filtering by current User's
    // backend roles
    private suspend fun getFilteredAlertIDs(user: User): List<String> {
        val queryBuilder = QueryBuilders
            .boolQuery()
            .filter(QueryBuilders.termsQuery("monitor_user.backend_roles.keyword", user.backendRoles))
        val searchSourceBuilder =
            SearchSourceBuilder()
                .version(true)
                .seqNoAndPrimaryTerm(true)
                .query(queryBuilder)
        val searchRequest = SearchRequest()
            .source(searchSourceBuilder)
            .indices(ALL_ALERT_INDEX_PATTERN)
        // .preference(Preference.PRIMARY_FIRST.type()) // expensive, be careful

        val searchResponse: SearchResponse = client.suspendUntil { search(searchRequest, it) }
        val alertIDs = searchResponse.hits.map { hit ->
            val xcp = XContentHelper.createParser(
                NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE,
                hit.sourceRef,
                XContentType.JSON
            )
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
            val alert = Alert.parse(xcp, hit.id, hit.version)
            alert.id
        }

        return alertIDs
    }
}

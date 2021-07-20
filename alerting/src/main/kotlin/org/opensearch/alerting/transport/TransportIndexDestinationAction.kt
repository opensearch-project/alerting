/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.alerting.transport

import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.ActionListener
import org.opensearch.action.admin.indices.create.CreateIndexResponse
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.index.IndexResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.alerting.action.IndexDestinationAction
import org.opensearch.alerting.action.IndexDestinationRequest
import org.opensearch.alerting.action.IndexDestinationResponse
import org.opensearch.alerting.actionconverter.DestinationActionsConverter
import org.opensearch.alerting.actionconverter.DestinationActionsConverter.Companion.convertCreateNotificationConfigResponseToIndexDestinationResponse
import org.opensearch.alerting.actionconverter.DestinationActionsConverter.Companion.convertIndexDestinationRequestToCreateNotificationConfigRequest
import org.opensearch.alerting.actionconverter.DestinationActionsConverter.Companion.convertIndexDestinationRequestToUpdateNotificationConfigRequest
import org.opensearch.alerting.actionconverter.DestinationActionsConverter.Companion.convertUpdateNotificationConfigResponseToIndexDestinationResponse
import org.opensearch.alerting.actionconverter.EmailAccountActionsConverter
import org.opensearch.alerting.core.ScheduledJobIndices
import org.opensearch.alerting.core.model.ScheduledJob
import org.opensearch.alerting.model.destination.Destination
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.settings.DestinationSettings
import org.opensearch.alerting.util.AlertingException
import org.opensearch.alerting.util.IndexUtils
import org.opensearch.alerting.util.NotificationAPIUtils
import org.opensearch.alerting.util.checkFilterByUserBackendRoles
import org.opensearch.alerting.util.checkUserFilterByPermissions
import org.opensearch.client.Client
import org.opensearch.client.node.NodeClient
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentFactory.jsonBuilder
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParserUtils
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.ConfigConstants
import org.opensearch.commons.authuser.User
import org.opensearch.commons.notifications.NotificationsPluginInterface
import org.opensearch.commons.notifications.action.BaseResponse
import org.opensearch.commons.notifications.action.CreateNotificationConfigResponse
import org.opensearch.commons.notifications.action.GetNotificationConfigRequest
import org.opensearch.commons.notifications.action.GetNotificationConfigResponse
import org.opensearch.commons.notifications.action.NotificationsActions
import org.opensearch.commons.notifications.action.UpdateNotificationConfigResponse
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestStatus
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import java.io.IOException

private val log = LogManager.getLogger(TransportIndexDestinationAction::class.java)

class TransportIndexDestinationAction @Inject constructor(
    transportService: TransportService,
    val client: NodeClient,
    actionFilters: ActionFilters,
    val scheduledJobIndices: ScheduledJobIndices,
    val clusterService: ClusterService,
    settings: Settings,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<IndexDestinationRequest, IndexDestinationResponse>(
    IndexDestinationAction.NAME, transportService, actionFilters, ::IndexDestinationRequest
) {

    @Volatile private var indexTimeout = AlertingSettings.INDEX_TIMEOUT.get(settings)
    @Volatile private var allowList = DestinationSettings.ALLOW_LIST.get(settings)
    @Volatile private var filterByEnabled = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(AlertingSettings.INDEX_TIMEOUT) { indexTimeout = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(DestinationSettings.ALLOW_LIST) { allowList = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(AlertingSettings.FILTER_BY_BACKEND_ROLES) { filterByEnabled = it }
    }

    override fun doExecute(task: Task, request: IndexDestinationRequest, actionListener: ActionListener<IndexDestinationResponse>) {
        val userStr = client.threadPool().threadContext.getTransient<String>(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT)
        log.debug("User and roles string from thread context: $userStr")
        val user: User? = User.parse(userStr)

        if (!checkFilterByUserBackendRoles(filterByEnabled, user, actionListener)) {
            return
        }

        try {
            val notificationResponse: BaseResponse
            val configId: String
            if (request.method == RestRequest.Method.PUT) {

                notificationResponse = NotificationAPIUtils.updateNotificationConfig(client, convertIndexDestinationRequestToUpdateNotificationConfigRequest(request))
                configId = notificationResponse.configId

//            NotificationsPluginInterface.updateNotificationConfig(client, convertIndexDestinationRequestToUpdateNotificationConfigRequest(request),
//                object : ActionListener<UpdateNotificationConfigResponse> {
//                    override fun onResponse(response: UpdateNotificationConfigResponse) {
//                        notificationResponse = response
//                        configId = response.configId
//                    }
//                    override fun onFailure(e: Exception) {
//                        actionListener.onFailure(AlertingException.wrap(e))
//                        return
//                    }
//                }
//            )
            } else {
                val createRequest = convertIndexDestinationRequestToCreateNotificationConfigRequest(request)
                notificationResponse = NotificationAPIUtils.createNotificationConfig(client, createRequest)
                configId = notificationResponse.configId
//            NotificationsPluginInterface.createNotificationConfig(client, convertIndexDestinationRequestToCreateNotificationConfigRequest(request),
//                object : ActionListener<CreateNotificationConfigResponse> {
//                    override fun onResponse(response: CreateNotificationConfigResponse) {
//                        notificationResponse = response
//                        configId = response.configId
//                    }
//                    override fun onFailure(e: Exception) {
//                        actionListener.onFailure(AlertingException.wrap(e))
//                        return
//                    }
//                }
//            )
            }

            val getNotificationConfigRequest = GetNotificationConfigRequest(setOf(configId!!), 0, 1, null, null, emptyMap())
            val getNotificationConfigResponse = NotificationAPIUtils.getNotificationConfig(client, getNotificationConfigRequest)
            if (request.method == RestRequest.Method.PUT) {
                actionListener.onResponse(convertUpdateNotificationConfigResponseToIndexDestinationResponse(notificationResponse as UpdateNotificationConfigResponse, getNotificationConfigResponse))
            } else {
                actionListener.onResponse(convertCreateNotificationConfigResponseToIndexDestinationResponse(notificationResponse as CreateNotificationConfigResponse, getNotificationConfigResponse))
            }
        } catch (e: Exception) {
            log.error("Failed to index destination due to", e)
            actionListener.onFailure(AlertingException.wrap(e))
        }
//        NotificationsPluginInterface.getNotificationConfig(client, getNotificationConfigRequest,
//            object : ActionListener<GetNotificationConfigResponse> {
//                override fun onResponse(response: GetNotificationConfigResponse) {
//                    if (request.method == RestRequest.Method.PUT) {
//                        actionListener.onResponse(convertUpdateNotificationConfigResponseToIndexDestinationResponse(notificationResponse!! as UpdateNotificationConfigResponse, response))
//                    } else {
//                        actionListener.onResponse(convertCreateNotificationConfigResponseToIndexDestinationResponse(notificationResponse!! as CreateNotificationConfigResponse, response))
//                    }
//                }
//                override fun onFailure(e: Exception) {
//                    actionListener.onFailure(AlertingException.wrap(e))
//                }
//            }
//        )

//        client.threadPool().threadContext.stashContext().use {
//            IndexDestinationHandler(client, actionListener, request, user).resolveUserAndStart()
//        }
    }

    inner class IndexDestinationHandler(
        private val client: Client,
        private val actionListener: ActionListener<IndexDestinationResponse>,
        private val request: IndexDestinationRequest,
        private val user: User?
    ) {

        fun resolveUserAndStart() {
            if (user == null) {
                // Security is disabled, add empty user to destination. user is null for older versions.
                request.destination = request.destination
                    .copy(user = User("", listOf(), listOf(), listOf()))
                start()
            } else {
                try {
                    request.destination = request.destination
                        .copy(user = User(user.name, user.backendRoles, user.roles, user.customAttNames))
                    start()
                } catch (ex: IOException) {
                    actionListener.onFailure(AlertingException.wrap(ex))
                }
            }
        }

        fun start() {
            if (!scheduledJobIndices.scheduledJobIndexExists()) {
                scheduledJobIndices.initScheduledJobIndex(object : ActionListener<CreateIndexResponse> {
                    override fun onResponse(response: CreateIndexResponse) {
                        onCreateMappingsResponse(response)
                    }
                    override fun onFailure(t: Exception) {
                        actionListener.onFailure(AlertingException.wrap(t))
                    }
                })
            } else if (!IndexUtils.scheduledJobIndexUpdated) {
                IndexUtils.updateIndexMapping(
                    ScheduledJob.SCHEDULED_JOBS_INDEX, ScheduledJob.SCHEDULED_JOB_TYPE,
                    ScheduledJobIndices.scheduledJobMappings(), clusterService.state(), client.admin().indices(),
                    object : ActionListener<AcknowledgedResponse> {
                        override fun onResponse(response: AcknowledgedResponse) {
                            onUpdateMappingsResponse(response)
                        }
                        override fun onFailure(t: Exception) {
                            actionListener.onFailure(AlertingException.wrap(t))
                        }
                    }
                )
            } else {
                prepareDestinationIndexing()
            }
        }

        private fun prepareDestinationIndexing() {

            // Prevent indexing if the Destination type is disallowed
            val destinationType = request.destination.type.value
            if (!allowList.contains(destinationType)) {
                actionListener.onFailure(
                    AlertingException.wrap(
                        OpenSearchStatusException(
                            "Destination type is not allowed: $destinationType",
                            RestStatus.FORBIDDEN
                        )
                    )
                )
                return
            }

            if (request.method == RestRequest.Method.PUT) updateDestination()
            else {
                val destination = request.destination.copy(schemaVersion = IndexUtils.scheduledJobIndexSchemaVersion)
                val indexRequest = IndexRequest(ScheduledJob.SCHEDULED_JOBS_INDEX)
                    .setRefreshPolicy(request.refreshPolicy)
                    .source(destination.toXContent(jsonBuilder(), ToXContent.MapParams(mapOf("with_type" to "true"))))
                    .setIfSeqNo(request.seqNo)
                    .setIfPrimaryTerm(request.primaryTerm)
                    .timeout(indexTimeout)

                client.index(
                    indexRequest,
                    object : ActionListener<IndexResponse> {
                        override fun onResponse(response: IndexResponse) {
                            val failureReasons = checkShardsFailure(response)
                            if (failureReasons != null) {
                                actionListener.onFailure(
                                    AlertingException.wrap(OpenSearchStatusException(failureReasons.toString(), response.status()))
                                )
                                return
                            }
                            actionListener.onResponse(
                                IndexDestinationResponse(
                                    response.id, response.version, response.seqNo,
                                    response.primaryTerm, RestStatus.CREATED, destination
                                )
                            )
                        }
                        override fun onFailure(t: Exception) {
                            actionListener.onFailure(AlertingException.wrap(t))
                        }
                    }
                )
            }
        }
        private fun onCreateMappingsResponse(response: CreateIndexResponse) {
            if (response.isAcknowledged) {
                log.info("Created ${ScheduledJob.SCHEDULED_JOBS_INDEX} with mappings.")
                prepareDestinationIndexing()
                IndexUtils.scheduledJobIndexUpdated()
            } else {
                log.error("Create ${ScheduledJob.SCHEDULED_JOBS_INDEX} mappings call not acknowledged.")
                actionListener.onFailure(
                    AlertingException.wrap(
                        OpenSearchStatusException(
                            "Create ${ScheduledJob.SCHEDULED_JOBS_INDEX} mappings call not acknowledged",
                            RestStatus.INTERNAL_SERVER_ERROR
                        )
                    )
                )
            }
        }

        private fun onUpdateMappingsResponse(response: AcknowledgedResponse) {
            if (response.isAcknowledged) {
                log.info("Updated  ${ScheduledJob.SCHEDULED_JOBS_INDEX} with mappings.")
                IndexUtils.scheduledJobIndexUpdated()
                prepareDestinationIndexing()
            } else {
                log.error("Update ${ScheduledJob.SCHEDULED_JOBS_INDEX} mappings call not acknowledged.")
                actionListener.onFailure(
                    AlertingException.wrap(
                        OpenSearchStatusException(
                            "Updated ${ScheduledJob.SCHEDULED_JOBS_INDEX} mappings call not acknowledged.",
                            RestStatus.INTERNAL_SERVER_ERROR
                        )
                    )
                )
            }
        }

        private fun updateDestination() {
            val getRequest = GetRequest(ScheduledJob.SCHEDULED_JOBS_INDEX, request.destinationId)
            client.get(
                getRequest,
                object : ActionListener<GetResponse> {
                    override fun onResponse(response: GetResponse) {
                        if (!response.isExists) {
                            actionListener.onFailure(
                                AlertingException.wrap(
                                    OpenSearchStatusException(
                                        "Destination with ${request.destinationId} is not found",
                                        RestStatus.NOT_FOUND
                                    )
                                )
                            )
                            return
                        }
                        val id = response.id
                        val version = response.version
                        val seqNo = response.seqNo.toInt()
                        val primaryTerm = response.primaryTerm.toInt()
                        val xcp = XContentFactory.xContent(XContentType.JSON)
                            .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, response.sourceAsString)
                        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
                        XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, xcp.nextToken(), xcp)
                        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
                        val dest = Destination.parse(xcp, id, version, seqNo, primaryTerm)
                        onGetResponse(dest)
                    }
                    override fun onFailure(t: Exception) {
                        actionListener.onFailure(AlertingException.wrap(t))
                    }
                }
            )
        }

        private fun onGetResponse(destination: Destination) {
            if (!checkUserFilterByPermissions(
                    filterByEnabled,
                    user,
                    destination.user,
                    actionListener,
                    "destination",
                    request.destinationId
                )
            ) {
                return
            }

            val indexDestination = request.destination.copy(schemaVersion = IndexUtils.scheduledJobIndexSchemaVersion)
            val indexRequest = IndexRequest(ScheduledJob.SCHEDULED_JOBS_INDEX)
                .setRefreshPolicy(request.refreshPolicy)
                .source(indexDestination.toXContent(jsonBuilder(), ToXContent.MapParams(mapOf("with_type" to "true"))))
                .id(request.destinationId)
                .setIfSeqNo(request.seqNo)
                .setIfPrimaryTerm(request.primaryTerm)
                .timeout(indexTimeout)

            client.index(
                indexRequest,
                object : ActionListener<IndexResponse> {
                    override fun onResponse(response: IndexResponse) {
                        val failureReasons = checkShardsFailure(response)
                        if (failureReasons != null) {
                            actionListener.onFailure(
                                AlertingException.wrap(OpenSearchStatusException(failureReasons.toString(), response.status()))
                            )
                            return
                        }
                        actionListener.onResponse(
                            IndexDestinationResponse(
                                response.id, response.version, response.seqNo,
                                response.primaryTerm, RestStatus.CREATED, indexDestination
                            )
                        )
                    }
                    override fun onFailure(t: Exception) {
                        actionListener.onFailure(AlertingException.wrap(t))
                    }
                }
            )
        }

        private fun checkShardsFailure(response: IndexResponse): String? {
            var failureReasons = StringBuilder()
            if (response.shardInfo.failed > 0) {
                response.shardInfo.failures.forEach {
                    entry ->
                    failureReasons.append(entry.reason())
                }
                return failureReasons.toString()
            }
            return null
        }
    }
}

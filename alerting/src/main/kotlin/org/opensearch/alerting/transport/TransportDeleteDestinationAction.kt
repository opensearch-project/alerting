/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.ActionListener
import org.opensearch.action.delete.DeleteRequest
import org.opensearch.action.delete.DeleteResponse
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.action.DeleteDestinationAction
import org.opensearch.alerting.action.DeleteDestinationRequest
import org.opensearch.alerting.core.model.ScheduledJob
import org.opensearch.alerting.model.destination.Destination
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.util.AlertingException
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParserUtils
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.authuser.User
import org.opensearch.rest.RestStatus
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import java.io.IOException

private val log = LogManager.getLogger(TransportDeleteDestinationAction::class.java)

class TransportDeleteDestinationAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    settings: Settings,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<DeleteDestinationRequest, DeleteResponse>(
    DeleteDestinationAction.NAME, transportService, actionFilters, ::DeleteDestinationRequest
),
    SecureTransportAction {

    @Volatile override var filterByEnabled = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        listenFilterBySettingChange(clusterService)
    }

    override fun doExecute(task: Task, request: DeleteDestinationRequest, actionListener: ActionListener<DeleteResponse>) {
        val user = readUserFromThreadContext(client)
        val deleteRequest = DeleteRequest(ScheduledJob.SCHEDULED_JOBS_INDEX, request.destinationId)
            .setRefreshPolicy(request.refreshPolicy)

        if (!validateUserBackendRoles(user, actionListener)) {
            return
        }
        client.threadPool().threadContext.stashContext().use {
            DeleteDestinationHandler(client, actionListener, deleteRequest, user, request.destinationId).resolveUserAndStart()
        }
    }

    inner class DeleteDestinationHandler(
        private val client: Client,
        private val actionListener: ActionListener<DeleteResponse>,
        private val deleteRequest: DeleteRequest,
        private val user: User?,
        private val destinationId: String
    ) {

        fun resolveUserAndStart() {
            if (user == null) {
                // Security is disabled, so we can delete the destination without issues
                deleteDestination()
            } else if (!doFilterForUser(user)) {
                // security is enabled and filterby is disabled.
                deleteDestination()
            } else {
                // security is enabled and filterby is enabled.
                try {
                    start()
                } catch (ex: IOException) {
                    actionListener.onFailure(AlertingException.wrap(ex))
                }
            }
        }

        fun start() {
            val getRequest = GetRequest(ScheduledJob.SCHEDULED_JOBS_INDEX, destinationId)
            client.get(
                getRequest,
                object : ActionListener<GetResponse> {
                    override fun onResponse(response: GetResponse) {
                        if (!response.isExists) {
                            actionListener.onFailure(
                                AlertingException.wrap(
                                    OpenSearchStatusException("Destination with $destinationId is not found", RestStatus.NOT_FOUND)
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
            if (!checkUserPermissionsWithResource(user, destination.user, actionListener, "destination", destinationId)) {
                return
            } else {
                deleteDestination()
            }
        }

        private fun deleteDestination() {
            client.delete(
                deleteRequest,
                object : ActionListener<DeleteResponse> {
                    override fun onResponse(response: DeleteResponse) {
                        actionListener.onResponse(response)
                    }

                    override fun onFailure(t: Exception) {
                        actionListener.onFailure(AlertingException.wrap(t))
                    }
                }
            )
        }
    }
}

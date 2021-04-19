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

/*
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.alerting.transport

import com.amazon.opendistroforelasticsearch.alerting.action.DeleteDestinationAction
import com.amazon.opendistroforelasticsearch.alerting.action.DeleteDestinationRequest
import com.amazon.opendistroforelasticsearch.alerting.core.model.ScheduledJob
import com.amazon.opendistroforelasticsearch.alerting.model.destination.Destination
import com.amazon.opendistroforelasticsearch.alerting.settings.AlertingSettings
import com.amazon.opendistroforelasticsearch.alerting.util.AlertingException
import com.amazon.opendistroforelasticsearch.alerting.util.checkFilterByUserBackendRoles
import com.amazon.opendistroforelasticsearch.alerting.util.checkUserFilterByPermissions
import com.amazon.opendistroforelasticsearch.commons.ConfigConstants
import com.amazon.opendistroforelasticsearch.commons.authuser.User
import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.ActionListener
import org.opensearch.action.delete.DeleteRequest
import org.opensearch.action.delete.DeleteResponse
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
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
    ) {

    @Volatile private var filterByEnabled = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(AlertingSettings.FILTER_BY_BACKEND_ROLES) { filterByEnabled = it }
    }

    override fun doExecute(task: Task, request: DeleteDestinationRequest, actionListener: ActionListener<DeleteResponse>) {
        val userStr = client.threadPool().threadContext.getTransient<String>(ConfigConstants.OPENDISTRO_SECURITY_USER_INFO_THREAD_CONTEXT)
        log.debug("User and roles string from thread context: $userStr")
        val user: User? = User.parse(userStr)
        val deleteRequest = DeleteRequest(ScheduledJob.SCHEDULED_JOBS_INDEX, request.destinationId)
                .setRefreshPolicy(request.refreshPolicy)

        if (!checkFilterByUserBackendRoles(filterByEnabled, user, actionListener)) {
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
            } else if (!filterByEnabled) {
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
            client.get(getRequest, object : ActionListener<GetResponse> {
                override fun onResponse(response: GetResponse) {
                    if (!response.isExists) {
                        actionListener.onFailure(AlertingException.wrap(
                            OpenSearchStatusException("Destination with $destinationId is not found", RestStatus.NOT_FOUND)))
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
            })
        }

        private fun onGetResponse(destination: Destination) {
            if (!checkUserFilterByPermissions(filterByEnabled, user, destination.user, actionListener, "destination", destinationId)) {
                return
            } else {
                deleteDestination()
            }
        }

        private fun deleteDestination() {
            client.delete(deleteRequest, object : ActionListener<DeleteResponse> {
                override fun onResponse(response: DeleteResponse) {
                    actionListener.onResponse(response)
                }

                override fun onFailure(t: Exception) {
                    actionListener.onFailure(AlertingException.wrap(t))
                }
            })
        }
    }
}

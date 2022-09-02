/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import org.opensearch.action.ActionListener
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.action.GetSuggestionsAction
import org.opensearch.alerting.action.GetSuggestionsRequest
import org.opensearch.alerting.action.GetSuggestionsResponse
import org.opensearch.alerting.rules.inputs.util.SuggestionsObjectListener
import org.opensearch.alerting.rules.util.RuleExecutor
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.util.AlertingException
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.rest.RestStatus
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService

class TransportGetSuggestionsAction @Inject constructor(
    transportService: TransportService,
    private val client: Client,
    private val clusterService: ClusterService,
    actionFilters: ActionFilters,
    val xContentRegistry: NamedXContentRegistry,
    settings: Settings
) : HandledTransportAction<GetSuggestionsRequest, GetSuggestionsResponse>(
    GetSuggestionsAction.NAME, transportService, actionFilters, ::GetSuggestionsRequest
),
    SecureTransportAction {

    @Volatile override var filterByEnabled = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        listenFilterBySettingChange(clusterService)
    }

    // TODO: checkUserPermissionsWithResource somewhere in here???
    override fun doExecute(task: Task, getSuggestionsRequest: GetSuggestionsRequest, actionListener: ActionListener<GetSuggestionsResponse>) {
        val user = readUserFromThreadContext(client)

        if (!validateUserBackendRoles(user, actionListener)) {
            return
        }

        client.threadPool().threadContext.stashContext().use {
            val getSuggestions = fun(obj: Any) {
                val suggestions = RuleExecutor.getSuggestions(obj)
                actionListener.onResponse(GetSuggestionsResponse(suggestions, RestStatus.OK))
            }

            val input = getSuggestionsRequest.input
            if (input.async) {
                input.getObject(
                    object : SuggestionsObjectListener {
                        override fun onGetResponse(obj: Any) {
                            getSuggestions(obj)
                        }

                        override fun onFailure(e: Exception) {
                            actionListener.onFailure(AlertingException.wrap(e))
                        }
                    },
                    client,
                    xContentRegistry
                )
            } else {
                val obj = input.getObject(
                    object : SuggestionsObjectListener {
                        override fun onGetResponse(obj: Any) {
                            actionListener.onFailure(
                                AlertingException.wrap(
                                    IllegalStateException("Inputs that don't use async object retrieval must not provide object here")
                                )
                            )
                        }

                        override fun onFailure(e: Exception) {
                            actionListener.onFailure(AlertingException.wrap(e))
                        }
                    }
                ) ?: actionListener.onFailure(AlertingException.wrap(IllegalStateException("objects passed inline cannot be null")))
                getSuggestions(obj)
            }
        }
    }
}

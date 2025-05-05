/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import org.opensearch.OpenSearchStatusException
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.action.SearchEmailAccountAction
import org.opensearch.alerting.settings.DestinationSettings.Companion.ALLOW_LIST
import org.opensearch.alerting.util.DestinationType
import org.opensearch.alerting.util.use
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.commons.alerting.util.AlertingException
import org.opensearch.core.action.ActionListener
import org.opensearch.core.rest.RestStatus
import org.opensearch.remote.metadata.client.SdkClient
import org.opensearch.remote.metadata.client.SearchDataObjectRequest
import org.opensearch.remote.metadata.common.SdkClientUtils
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.Client

class TransportSearchEmailAccountAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    val sdkClient: SdkClient,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    settings: Settings
) : HandledTransportAction<SearchRequest, SearchResponse>(
    SearchEmailAccountAction.NAME, transportService, actionFilters, ::SearchRequest
) {

    @Volatile private var allowList = ALLOW_LIST.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(ALLOW_LIST) { allowList = it }
    }

    override fun doExecute(task: Task, searchRequest: SearchRequest, actionListener: ActionListener<SearchResponse>) {

        if (!allowList.contains(DestinationType.EMAIL.value)) {
            actionListener.onFailure(
                AlertingException.wrap(
                    OpenSearchStatusException(
                        "This API is blocked since Destination type [${DestinationType.EMAIL}] is not allowed",
                        RestStatus.FORBIDDEN
                    )
                )
            )
            return
        }

        val searchDataObjectRequest = SearchDataObjectRequest.builder()
            .indices(*searchRequest.indices())
            .searchSourceBuilder(searchRequest.source())
            .build()

        client.threadPool().threadContext.stashContext().use {
            sdkClient.searchDataObjectAsync(searchDataObjectRequest)
                .whenComplete(
                    SdkClientUtils.wrapSearchCompletion(object : ActionListener<SearchResponse> {
                        override fun onResponse(response: SearchResponse) {
                            actionListener.onResponse(response)
                        }

                        override fun onFailure(e: Exception) {
                            actionListener.onFailure(e)
                        }
                    })
                )
        }
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.core.ppl

import org.opensearch.commons.utils.recreateObject
import org.opensearch.core.action.ActionListener
import org.opensearch.core.action.ActionResponse
import org.opensearch.sql.plugin.transport.PPLQueryAction
import org.opensearch.sql.plugin.transport.TransportPPLQueryRequest
import org.opensearch.sql.plugin.transport.TransportPPLQueryResponse
import org.opensearch.transport.client.node.NodeClient

/**
 * Transport action plugin interfaces for the SQL/PPL plugin
 */
@Suppress("UNCHECKED_CAST")
object PPLPluginInterface {
    fun executeQuery(
        client: NodeClient,
        request: TransportPPLQueryRequest,
        listener: ActionListener<TransportPPLQueryResponse>,
    ) {
        val wrappedListener = object : ActionListener<ActionResponse> {
            override fun onResponse(response: ActionResponse) {
                val recreated = recreateObject(response) { TransportPPLQueryResponse(it) }
                listener.onResponse(recreated)
            }

            override fun onFailure(exception: Exception) {
                listener.onFailure(exception)
            }
        } as ActionListener<TransportPPLQueryResponse>

        client.execute(
            PPLQueryAction.INSTANCE,
            request,
            wrappedListener
        )
    }
}

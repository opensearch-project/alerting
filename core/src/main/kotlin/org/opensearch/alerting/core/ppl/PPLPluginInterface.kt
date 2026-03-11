package org.opensearch.alerting.core.ppl

import org.opensearch.action.ActionListenerResponseHandler
import org.opensearch.cluster.node.DiscoveryNode
import org.opensearch.common.unit.TimeValue
import org.opensearch.commons.utils.recreateObject
import org.opensearch.core.action.ActionListener
import org.opensearch.core.action.ActionResponse
import org.opensearch.core.common.io.stream.Writeable
import org.opensearch.sql.plugin.transport.PPLQueryAction
import org.opensearch.sql.plugin.transport.TransportPPLQueryRequest
import org.opensearch.sql.plugin.transport.TransportPPLQueryResponse
import org.opensearch.transport.TransportException
import org.opensearch.transport.TransportRequestOptions
import org.opensearch.transport.TransportService

/**
 * Transport action plugin interfaces for the SQL/PPL plugin
 */
object PPLPluginInterface {
    fun executeQuery(
        transportService: TransportService,
        localNode: DiscoveryNode,
        request: TransportPPLQueryRequest,
        listener: ActionListener<TransportPPLQueryResponse>,
    ) {

        val responseReader = Writeable.Reader<ActionResponse> {
            TransportPPLQueryResponse(it)
        }

        val wrappedListener = object : ActionListener<ActionResponse> {
            override fun onResponse(response: ActionResponse) {
                val recreated = recreateObject(response) { TransportPPLQueryResponse(it) }
                listener.onResponse(recreated)
            }

            override fun onFailure(exception: Exception) {
                listener.onFailure(exception)
            }
        }

        transportService.sendRequest(
            localNode,
            PPLQueryAction.NAME,
            request,
            TransportRequestOptions
                .builder()
                .withTimeout(TimeValue.timeValueMinutes(1))
                .build(),
            object : ActionListenerResponseHandler<ActionResponse>(
                wrappedListener,
                responseReader
            ) {
                override fun handleResponse(response: ActionResponse) {
                    wrappedListener.onResponse(response)
                }

                override fun handleException(e: TransportException) {
                    wrappedListener.onFailure(e)
                }
            }
        )
    }
}

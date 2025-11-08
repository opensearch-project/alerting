package org.opensearch.alerting.core.ppl

import org.opensearch.client.node.NodeClient
import org.opensearch.commons.utils.recreateObject
import org.opensearch.core.action.ActionListener
import org.opensearch.core.action.ActionResponse
import org.opensearch.core.common.io.stream.Writeable
import org.opensearch.sql.plugin.transport.PPLQueryAction
import org.opensearch.sql.plugin.transport.TransportPPLQueryRequest
import org.opensearch.sql.plugin.transport.TransportPPLQueryResponse

/**
 * Transport action plugin interfaces for the SQL/PPL plugin
 */
object PPLPluginInterface {
    fun executeQuery(
        client: NodeClient,
        request: TransportPPLQueryRequest,
        listener: ActionListener<TransportPPLQueryResponse>
    ) {
        client.execute(
            PPLQueryAction.INSTANCE,
            request,
            wrapActionListener(listener) { response -> recreateObject(response) { TransportPPLQueryResponse(it) } }
        )
    }

    /**
     * Wrap action listener on concrete response class by a new created one on ActionResponse.
     * This is required because the response may be loaded by different classloader across plugins.
     * The onResponse(ActionResponse) avoids type cast exception and give a chance to recreate
     * the response object.
     */
    @Suppress("UNCHECKED_CAST")
    private fun <Response : ActionResponse> wrapActionListener(
        listener: ActionListener<Response>,
        recreate: (Writeable) -> Response
    ): ActionListener<Response> {
        return object : ActionListener<ActionResponse> {
            override fun onResponse(response: ActionResponse) {
                val recreated = recreate(response)
                listener.onResponse(recreated)
            }

            override fun onFailure(exception: java.lang.Exception) {
                listener.onFailure(exception)
            }
        } as ActionListener<Response>
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.action.GetEmailGroupAction
import org.opensearch.alerting.action.GetEmailGroupRequest
import org.opensearch.alerting.util.context
import org.opensearch.client.node.NodeClient
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.RestHandler.ReplacedRoute
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.action.RestActions
import org.opensearch.rest.action.RestToXContentListener
import org.opensearch.search.fetch.subphase.FetchSourceContext
import java.lang.IllegalArgumentException

/**
 * Rest handlers to retrieve an EmailGroup
 */
class RestGetEmailGroupAction : BaseRestHandler() {

    override fun getName(): String {
        return "get_email_group_action"
    }

    override fun routes(): List<Route> {
        return listOf()
    }

    override fun replacedRoutes(): MutableList<ReplacedRoute> {
        return mutableListOf(
            ReplacedRoute(
                RestRequest.Method.GET,
                "${AlertingPlugin.EMAIL_GROUP_BASE_URI}/{emailGroupID}",
                RestRequest.Method.GET,
                "${AlertingPlugin.LEGACY_OPENDISTRO_EMAIL_GROUP_BASE_URI}/{emailGroupID}"
            ),
            ReplacedRoute(
                RestRequest.Method.HEAD,
                "${AlertingPlugin.EMAIL_GROUP_BASE_URI}/{emailGroupID}",
                RestRequest.Method.HEAD,
                "${AlertingPlugin.LEGACY_OPENDISTRO_EMAIL_GROUP_BASE_URI}/{emailGroupID}"
            )
        )
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val emailGroupID = request.param("emailGroupID")
        if (emailGroupID == null || emailGroupID.isEmpty()) {
            throw IllegalArgumentException("Missing email group ID")
        }

        var srcContext = context(request)
        if (request.method() == RestRequest.Method.HEAD) {
            srcContext = FetchSourceContext.DO_NOT_FETCH_SOURCE
        }

        val getEmailGroupRequest = GetEmailGroupRequest(emailGroupID, RestActions.parseVersion(request), request.method(), srcContext)
        return RestChannelConsumer { channel ->
            client.execute(GetEmailGroupAction.INSTANCE, getEmailGroupRequest, RestToXContentListener(channel))
        }
    }
}

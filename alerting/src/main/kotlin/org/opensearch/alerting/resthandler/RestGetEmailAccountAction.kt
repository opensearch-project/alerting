/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.action.GetEmailAccountAction
import org.opensearch.alerting.action.GetEmailAccountRequest
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
 * Rest handler to retrieve an EmailAccount.
 */
class RestGetEmailAccountAction : BaseRestHandler() {

    override fun getName(): String {
        return "get_email_account_action"
    }

    override fun routes(): List<Route> {
        return listOf()
    }

    override fun replacedRoutes(): MutableList<ReplacedRoute> {
        return mutableListOf(
            ReplacedRoute(
                RestRequest.Method.GET,
                "${AlertingPlugin.EMAIL_ACCOUNT_BASE_URI}/{emailAccountID}",
                RestRequest.Method.GET,
                "${AlertingPlugin.LEGACY_OPENDISTRO_EMAIL_ACCOUNT_BASE_URI}/{emailAccountID}"
            ),
            ReplacedRoute(
                RestRequest.Method.HEAD,
                "${AlertingPlugin.EMAIL_ACCOUNT_BASE_URI}/{emailAccountID}",
                RestRequest.Method.HEAD,
                "${AlertingPlugin.LEGACY_OPENDISTRO_EMAIL_ACCOUNT_BASE_URI}/{emailAccountID}"
            )
        )
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val emailAccountID = request.param("emailAccountID")
        if (emailAccountID == null || emailAccountID.isEmpty()) {
            throw IllegalArgumentException("Missing email account ID")
        }

        var srcContext = context(request)
        if (request.method() == RestRequest.Method.HEAD) {
            srcContext = FetchSourceContext.DO_NOT_FETCH_SOURCE
        }

        val getEmailAccountRequest = GetEmailAccountRequest(emailAccountID, RestActions.parseVersion(request), request.method(), srcContext)
        return RestChannelConsumer { channel ->
            client.execute(GetEmailAccountAction.INSTANCE, getEmailAccountRequest, RestToXContentListener(channel))
        }
    }
}

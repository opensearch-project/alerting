/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.opensearch.action.support.WriteRequest
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.action.DeleteEmailAccountAction
import org.opensearch.alerting.action.DeleteEmailAccountRequest
import org.opensearch.alerting.util.REFRESH
import org.opensearch.client.node.NodeClient
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.RestHandler.ReplacedRoute
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.action.RestToXContentListener
import java.io.IOException

private val log: Logger = LogManager.getLogger(RestDeleteEmailAccountAction::class.java)

/**
 * Rest handler to delete EmailAccount.
 */
class RestDeleteEmailAccountAction : BaseRestHandler() {

    override fun getName(): String {
        return "delete_email_account_action"
    }

    override fun routes(): List<Route> {
        return listOf()
    }

    override fun replacedRoutes(): MutableList<ReplacedRoute> {
        return mutableListOf(
            ReplacedRoute(
                RestRequest.Method.DELETE,
                "${AlertingPlugin.EMAIL_ACCOUNT_BASE_URI}/{emailAccountID}",
                RestRequest.Method.DELETE,
                "${AlertingPlugin.LEGACY_OPENDISTRO_EMAIL_ACCOUNT_BASE_URI}/{emailAccountID}"
            )
        )
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val emailAccountID = request.param("emailAccountID")
        log.debug("${request.method()} ${AlertingPlugin.EMAIL_ACCOUNT_BASE_URI}/$emailAccountID")

        val refreshPolicy = WriteRequest.RefreshPolicy.parse(request.param(REFRESH, WriteRequest.RefreshPolicy.IMMEDIATE.value))
        val deleteEmailAccountRequest = DeleteEmailAccountRequest(emailAccountID, refreshPolicy)

        return RestChannelConsumer { channel ->
            client.execute(DeleteEmailAccountAction.INSTANCE, deleteEmailAccountRequest, RestToXContentListener(channel))
        }
    }
}

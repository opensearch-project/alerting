/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.opensearch.action.support.WriteRequest
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.action.DeleteEmailGroupAction
import org.opensearch.alerting.action.DeleteEmailGroupRequest
import org.opensearch.alerting.util.REFRESH
import org.opensearch.client.node.NodeClient
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.RestHandler.ReplacedRoute
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.action.RestToXContentListener
import java.io.IOException

private val log: Logger = LogManager.getLogger(RestDeleteEmailGroupAction::class.java)

/**
 * Rest handler to delete EmailGroup.
 */
class RestDeleteEmailGroupAction : BaseRestHandler() {

    override fun getName(): String {
        return "delete_email_group_action"
    }

    override fun routes(): List<Route> {
        return listOf()
    }

    override fun replacedRoutes(): MutableList<ReplacedRoute> {
        return mutableListOf(
            ReplacedRoute(
                RestRequest.Method.DELETE,
                "${AlertingPlugin.EMAIL_GROUP_BASE_URI}/{emailGroupID}",
                RestRequest.Method.DELETE,
                "${AlertingPlugin.LEGACY_OPENDISTRO_EMAIL_GROUP_BASE_URI}/{emailGroupID}"
            )
        )
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val emailGroupID = request.param("emailGroupID")
        log.debug("${request.method()} ${AlertingPlugin.EMAIL_GROUP_BASE_URI}/$emailGroupID")

        val refreshPolicy = WriteRequest.RefreshPolicy.parse(request.param(REFRESH, WriteRequest.RefreshPolicy.IMMEDIATE.value))
        val deleteEmailGroupRequest = DeleteEmailGroupRequest(emailGroupID, refreshPolicy)

        return RestChannelConsumer { channel ->
            client.execute(DeleteEmailGroupAction.INSTANCE, deleteEmailGroupRequest, RestToXContentListener(channel))
        }
    }
}

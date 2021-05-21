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
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package org.opensearch.alerting.resthandler

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.opensearch.action.support.WriteRequest
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.action.DeleteDestinationAction
import org.opensearch.alerting.action.DeleteDestinationRequest
import org.opensearch.alerting.util.REFRESH
import org.opensearch.client.node.NodeClient
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BaseRestHandler.RestChannelConsumer
import org.opensearch.rest.RestHandler.ReplacedRoute
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.action.RestToXContentListener
import java.io.IOException

private val log: Logger = LogManager.getLogger(RestDeleteDestinationAction::class.java)

/**
 * This class consists of the REST handler to delete destination.
 */
class RestDeleteDestinationAction : BaseRestHandler() {

    override fun getName(): String {
        return "delete_destination_action"
    }

    override fun routes(): List<Route> {
        return listOf()
    }

    override fun replacedRoutes(): MutableList<ReplacedRoute> {
        return mutableListOf(
            ReplacedRoute(
                RestRequest.Method.DELETE,
                "${AlertingPlugin.DESTINATION_BASE_URI}/{destinationID}",
                RestRequest.Method.DELETE,
                "${AlertingPlugin.LEGACY_OPENDISTRO_DESTINATION_BASE_URI}/{destinationID}"
            )
        )
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        log.debug("${request.method()} ${AlertingPlugin.DESTINATION_BASE_URI}/{destinationID}")

        val destinationId = request.param("destinationID")
        log.debug("${request.method()} ${AlertingPlugin.MONITOR_BASE_URI}/$destinationId")

        val refreshPolicy = WriteRequest.RefreshPolicy.parse(request.param(REFRESH, WriteRequest.RefreshPolicy.IMMEDIATE.value))
        val deleteDestinationRequest = DeleteDestinationRequest(destinationId, refreshPolicy)

        return RestChannelConsumer { channel ->
            client.execute(DeleteDestinationAction.INSTANCE, deleteDestinationRequest, RestToXContentListener(channel))
        }
    }
}

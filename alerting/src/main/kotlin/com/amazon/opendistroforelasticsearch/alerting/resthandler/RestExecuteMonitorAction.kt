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

package com.amazon.opendistroforelasticsearch.alerting.resthandler

import com.amazon.opendistroforelasticsearch.alerting.AlertingPlugin
import com.amazon.opendistroforelasticsearch.alerting.action.ExecuteMonitorAction
import com.amazon.opendistroforelasticsearch.alerting.action.ExecuteMonitorRequest
import com.amazon.opendistroforelasticsearch.alerting.model.Monitor
import org.apache.logging.log4j.LogManager
import org.opensearch.client.node.NodeClient
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.XContentParser.Token.START_OBJECT
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BaseRestHandler.RestChannelConsumer
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.POST
import org.opensearch.rest.action.RestToXContentListener
import java.time.Instant

private val log = LogManager.getLogger(RestExecuteMonitorAction::class.java)

class RestExecuteMonitorAction : BaseRestHandler() {

    override fun getName(): String = "execute_monitor_action"

    override fun routes(): List<Route> {
        return listOf(
                Route(POST, "${AlertingPlugin.MONITOR_BASE_URI}/{monitorID}/_execute"),
                Route(POST, "${AlertingPlugin.MONITOR_BASE_URI}/_execute")
        )
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        log.debug("${request.method()} ${AlertingPlugin.MONITOR_BASE_URI}/_execute")

        return RestChannelConsumer { channel ->
            val dryrun = request.paramAsBoolean("dryrun", false)
            val requestEnd = request.paramAsTime("period_end", TimeValue(Instant.now().toEpochMilli()))

            if (request.hasParam("monitorID")) {
                val monitorId = request.param("monitorID")
                val execMonitorRequest = ExecuteMonitorRequest(dryrun, requestEnd, monitorId, null)
                client.execute(ExecuteMonitorAction.INSTANCE, execMonitorRequest, RestToXContentListener(channel))
            } else {
                val xcp = request.contentParser()
                ensureExpectedToken(START_OBJECT, xcp.nextToken(), xcp)
                val monitor = Monitor.parse(xcp, Monitor.NO_ID, Monitor.NO_VERSION)
                val execMonitorRequest = ExecuteMonitorRequest(dryrun, requestEnd, null, monitor)
                client.execute(ExecuteMonitorAction.INSTANCE, execMonitorRequest, RestToXContentListener(channel))
            }
        }
    }

    override fun responseParams(): Set<String> {
        return setOf("dryrun", "period_end", "monitorID")
    }
}

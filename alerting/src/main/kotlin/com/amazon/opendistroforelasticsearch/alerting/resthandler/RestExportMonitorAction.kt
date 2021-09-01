/*
 *   Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import com.amazon.opendistroforelasticsearch.alerting.action.*
import org.apache.logging.log4j.LogManager
import org.elasticsearch.client.node.NodeClient
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.BaseRestHandler.RestChannelConsumer
import org.elasticsearch.rest.BytesRestResponse
import org.elasticsearch.rest.RestChannel
import org.elasticsearch.rest.RestHandler.Route
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestRequest.Method.GET
import org.elasticsearch.rest.RestResponse
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.rest.action.RestResponseListener
import java.io.IOException

private val log = LogManager.getLogger(RestExportMonitorAction::class.java)

/**
 * Rest handler to bulk export monitors.
 */
class RestExportMonitorAction : BaseRestHandler() {

    override fun getName(): String {
        return "export-monitor-action"
    }

    override fun routes(): List<Route> {
        return listOf(
            Route(GET, "${AlertingPlugin.MONITOR_BASE_URI}/export") // Export monitors
        )
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        log.debug("${request.method()} ${AlertingPlugin.MONITOR_BASE_URI}/export")

        return RestChannelConsumer { channel ->
            client.execute(ExportMonitorAction.INSTANCE, ExportMonitorRequest(), exportMonitorResponse(channel))
        }
    }

    private fun exportMonitorResponse(channel: RestChannel):
            RestResponseListener<ExportMonitorResponse> {
        return object : RestResponseListener<ExportMonitorResponse>(channel) {
            @Throws(Exception::class)
            override fun buildResponse(response: ExportMonitorResponse): RestResponse {
                val restResponse = BytesRestResponse(RestStatus.CREATED, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS))
                val location = "${AlertingPlugin.MONITOR_BASE_URI}/export"
                restResponse.addHeader("Location", location)
                return restResponse
            }
        }
    }
}

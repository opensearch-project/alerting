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

import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.action.GetEmailAccountAction
import org.opensearch.alerting.action.GetEmailAccountRequest
import org.opensearch.alerting.util.context
import org.opensearch.client.node.NodeClient
import org.opensearch.rest.BaseRestHandler
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
        return listOf(
                Route(RestRequest.Method.GET, "${AlertingPlugin.EMAIL_ACCOUNT_BASE_URI}/{emailAccountID}"),
                Route(RestRequest.Method.HEAD, "${AlertingPlugin.EMAIL_ACCOUNT_BASE_URI}/{emailAccountID}")
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

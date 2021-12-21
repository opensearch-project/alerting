/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.opensearch.client.node.NodeClient
import org.opensearch.rest.BytesRestResponse
import org.opensearch.rest.RestChannel

abstract class AsyncActionHandler(protected val client: NodeClient, protected val channel: RestChannel) {

    protected fun onFailure(e: Exception) {
        channel.sendResponse(BytesRestResponse(channel, e))
    }
}

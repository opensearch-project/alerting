/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.opensearch.rest.BytesRestResponse
import org.opensearch.rest.RestChannel
import org.opensearch.transport.client.node.NodeClient

abstract class AsyncActionHandler(protected val client: NodeClient, protected val channel: RestChannel) {

    protected fun onFailure(e: Exception) {
        channel.sendResponse(BytesRestResponse(channel, e))
    }
}

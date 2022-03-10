/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.ActionResponse
import org.opensearch.alerting.model.MonitorRunResult
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentBuilder
import java.io.IOException

class ExecuteMonitorResponse : ActionResponse, ToXContentObject {

    val monitorRunResult: MonitorRunResult<*>

    constructor(monitorRunResult: MonitorRunResult<*>) : super() {
        this.monitorRunResult = monitorRunResult
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        MonitorRunResult.readFrom(sin) // monitorRunResult
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        monitorRunResult.writeTo(out)
    }

    @Throws(IOException::class)
    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return monitorRunResult.toXContent(builder, ToXContent.EMPTY_PARAMS)
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.actionv2

import org.opensearch.alerting.modelv2.MonitorV2RunResult
import org.opensearch.core.action.ActionResponse
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentBuilder
import java.io.IOException

class ExecuteMonitorV2Response : ActionResponse, ToXContentObject {
    val monitorV2RunResult: MonitorV2RunResult<*>

    constructor(monitorV2RunResult: MonitorV2RunResult<*>) : super() {
        this.monitorV2RunResult = monitorV2RunResult
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        MonitorV2RunResult.readFrom(sin) // monitorRunResult
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        MonitorV2RunResult.writeTo(out, monitorV2RunResult)
    }

    @Throws(IOException::class)
    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return monitorV2RunResult.toXContent(builder, ToXContent.EMPTY_PARAMS)
    }
}

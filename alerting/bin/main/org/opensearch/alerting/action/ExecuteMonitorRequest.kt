/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.alerting.model.Monitor
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.unit.TimeValue
import java.io.IOException

class ExecuteMonitorRequest : ActionRequest {
    val dryrun: Boolean
    val requestEnd: TimeValue
    val monitorId: String?
    val monitor: Monitor?

    constructor(
        dryrun: Boolean,
        requestEnd: TimeValue,
        monitorId: String?,
        monitor: Monitor?
    ) : super() {
        this.dryrun = dryrun
        this.requestEnd = requestEnd
        this.monitorId = monitorId
        this.monitor = monitor
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        sin.readBoolean(), // dryrun
        sin.readTimeValue(), // requestEnd
        sin.readOptionalString(), // monitorId
        if (sin.readBoolean()) {
            Monitor.readFrom(sin) // monitor
        } else null
    )

    override fun validate(): ActionRequestValidationException? {
        return null
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeBoolean(dryrun)
        out.writeTimeValue(requestEnd)
        out.writeOptionalString(monitorId)
        if (monitor != null) {
            out.writeBoolean(true)
            monitor.writeTo(out)
        } else {
            out.writeBoolean(false)
        }
    }
}

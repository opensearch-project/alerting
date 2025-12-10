/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.common.unit.TimeValue
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import java.io.IOException

class ExecuteMonitorRequest : ActionRequest {
    val dryrun: Boolean
    val requestEnd: TimeValue
    val monitorId: String?
    val monitor: Monitor?
    val requestStart: TimeValue?

    constructor(
        dryrun: Boolean,
        requestEnd: TimeValue,
        monitorId: String?,
        monitor: Monitor?,
        requestStart: TimeValue? = null,
    ) : super() {
        this.dryrun = dryrun
        this.requestEnd = requestEnd
        this.monitorId = monitorId
        this.monitor = monitor
        this.requestStart = requestStart
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        sin.readBoolean(), // dryrun
        sin.readTimeValue(), // requestEnd
        sin.readOptionalString(), // monitorId
        if (sin.readBoolean()) {
            Monitor.readFrom(sin) // monitor
        } else {
            null
        },
        sin.readOptionalTimeValue(),
    )

    override fun validate(): ActionRequestValidationException? = null

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
        out.writeOptionalTimeValue(requestStart)
    }
}

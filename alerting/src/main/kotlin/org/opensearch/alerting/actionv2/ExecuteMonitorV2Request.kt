package org.opensearch.alerting.actionv2

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.ValidateActions
import org.opensearch.alerting.core.modelv2.MonitorV2
import org.opensearch.common.unit.TimeValue
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import java.io.IOException

class ExecuteMonitorV2Request : ActionRequest {
    val dryrun: Boolean
    val manual: Boolean
    val monitorId: String? // exactly one of monitorId or monitor must be non-null
    val monitorV2: MonitorV2?
    val requestStart: TimeValue?
    val requestEnd: TimeValue

    constructor(
        dryrun: Boolean,
        manual: Boolean, // if execute was called by user or by scheduled job
        monitorId: String?,
        monitorV2: MonitorV2?,
        requestStart: TimeValue? = null,
        requestEnd: TimeValue
    ) : super() {
        this.dryrun = dryrun
        this.manual = manual
        this.monitorId = monitorId
        this.monitorV2 = monitorV2
        this.requestStart = requestStart
        this.requestEnd = requestEnd
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        sin.readBoolean(), // dryrun
        sin.readBoolean(), // manual
        sin.readOptionalString(), // monitorId
        if (sin.readBoolean()) {
            MonitorV2.readFrom(sin) // monitor
        } else {
            null
        },
        sin.readOptionalTimeValue(),
        sin.readTimeValue() // requestEnd
    )

    override fun validate(): ActionRequestValidationException? =
        if (monitorV2 == null && monitorId == null) {
            ValidateActions.addValidationError("Neither a monitor ID nor monitor object was supplied", null)
        } else {
            null
        }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeBoolean(dryrun)
        out.writeBoolean(manual)
        out.writeOptionalString(monitorId)
        if (monitorV2 != null) {
            out.writeBoolean(true)
            monitorV2.writeTo(out)
        } else {
            out.writeBoolean(false)
        }
        out.writeOptionalTimeValue(requestStart)
        out.writeTimeValue(requestEnd)
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.actionv2

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.ValidateActions
import org.opensearch.alerting.modelv2.MonitorV2
import org.opensearch.common.unit.TimeValue
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import java.io.IOException

class ExecuteMonitorV2Request : ActionRequest {
    val dryrun: Boolean
    val manual: Boolean
    val monitorV2Id: String? // exactly one of monitorId or monitor must be non-null
    val monitorV2: MonitorV2?
    val requestEnd: TimeValue

    constructor(
        dryrun: Boolean,
        manual: Boolean, // if execute was called by user or by scheduled job
        monitorV2Id: String?,
        monitorV2: MonitorV2?,
        requestEnd: TimeValue
    ) : super() {
        this.dryrun = dryrun
        this.manual = manual
        this.monitorV2Id = monitorV2Id
        this.monitorV2 = monitorV2
        this.requestEnd = requestEnd
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        sin.readBoolean(), // dryrun
        sin.readBoolean(), // manual
        sin.readOptionalString(), // monitorV2Id
        if (sin.readBoolean()) {
            MonitorV2.readFrom(sin) // monitorV2
        } else {
            null
        },
        sin.readTimeValue() // requestEnd
    )

    override fun validate(): ActionRequestValidationException? =
        if (monitorV2 == null && monitorV2Id == null) {
            ValidateActions.addValidationError("Neither a monitor ID nor monitor object was supplied", null)
        } else {
            null
        }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeBoolean(dryrun)
        out.writeBoolean(manual)
        out.writeOptionalString(monitorV2Id)
        if (monitorV2 != null) {
            out.writeBoolean(true)
            MonitorV2.writeTo(out, monitorV2)
        } else {
            out.writeBoolean(false)
        }
        out.writeTimeValue(requestEnd)
    }
}

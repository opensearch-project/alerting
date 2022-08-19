package org.opensearch.alerting.action

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.alerting.model.Monitor
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import java.io.IOException

class GetSuggestionsRequest : ActionRequest {
    val monitorId: String?
    val monitor: Monitor?

    constructor(
        monitorId: String?,
        monitor: Monitor?
    ) : super() {
        this.monitorId = monitorId
        this.monitor = monitor
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
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
        out.writeOptionalString(monitorId)
        if (monitor != null) {
            out.writeBoolean(true)
            monitor.writeTo(out)
        } else {
            out.writeBoolean(false)
        }
    }
}

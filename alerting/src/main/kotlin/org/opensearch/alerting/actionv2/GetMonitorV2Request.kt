package org.opensearch.alerting.actionv2

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.search.fetch.subphase.FetchSourceContext
import java.io.IOException

class GetMonitorV2Request : ActionRequest {
    val monitorV2Id: String
    val version: Long
    val srcContext: FetchSourceContext?

    constructor(
        monitorV2Id: String,
        version: Long,
        srcContext: FetchSourceContext?
    ) : super() {
        this.monitorV2Id = monitorV2Id
        this.version = version
        this.srcContext = srcContext
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        sin.readString(), // monitorV2Id
        sin.readLong(), // version
        if (sin.readBoolean()) {
            FetchSourceContext(sin) // srcContext
        } else {
            null
        }
    )

    override fun validate(): ActionRequestValidationException? {
        return null
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(monitorV2Id)
        out.writeLong(version)
        out.writeBoolean(srcContext != null)
        srcContext?.writeTo(out)
    }
}

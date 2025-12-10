package org.opensearch.alerting.core.model

import org.opensearch.commons.alerting.model.Schedule
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentBuilder
import java.io.IOException
import java.time.Instant

class MockScheduledJob(
    override val id: String,
    override val version: Long,
    override val name: String,
    override val type: String,
    override val enabled: Boolean,
    override val schedule: Schedule,
    override var lastUpdateTime: Instant,
    override val enabledTime: Instant?,
) : ScheduledJob {
    override fun fromDocument(
        id: String,
        version: Long,
    ): ScheduledJob {
        TODO("not implemented")
    }

    override fun toXContent(
        builder: XContentBuilder?,
        params: ToXContent.Params?,
    ): XContentBuilder {
        TODO("not implemented")
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        TODO("not implemented")
    }
}

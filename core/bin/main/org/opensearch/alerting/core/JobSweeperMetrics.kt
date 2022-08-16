/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.core

import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentFragment
import org.opensearch.common.xcontent.XContentBuilder

data class JobSweeperMetrics(val lastFullSweepTimeMillis: Long, val fullSweepOnTime: Boolean) : ToXContentFragment, Writeable {

    constructor(si: StreamInput) : this(si.readLong(), si.readBoolean())

    override fun writeTo(out: StreamOutput) {
        out.writeLong(lastFullSweepTimeMillis)
        out.writeBoolean(fullSweepOnTime)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.field("last_full_sweep_time_millis", lastFullSweepTimeMillis)
        builder.field("full_sweep_on_time", fullSweepOnTime)
        return builder
    }
}

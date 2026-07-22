/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.core.action.ActionResponse
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentBuilder
import java.io.IOException

/**
 * Aggregate counts from [org.opensearch.alerting.transport.TransportMigrateToRscAction]:
 *
 *  - [updated]: shareable docs (monitors + workflows) that gained `resource_type` and the scratch
 *    owner fields on this run.
 *  - [noops]: docs the script inspected but left untouched — either already migrated (had
 *    `resource_type`) or non-shareable (metadata, other records).
 *  - [failures]: shard-level failures reported by the underlying update-by-query. Zero on success.
 *  - [tookMillis]: wall-clock duration of the UBQ.
 */
class MigrateToRscResponse : ActionResponse, ToXContentObject {

    val updated: Long
    val noops: Long
    val failures: Long
    val tookMillis: Long

    constructor(updated: Long, noops: Long, failures: Long, tookMillis: Long) : super() {
        this.updated = updated
        this.noops = noops
        this.failures = failures
        this.tookMillis = tookMillis
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        updated = sin.readLong(),
        noops = sin.readLong(),
        failures = sin.readLong(),
        tookMillis = sin.readLong(),
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeLong(updated)
        out.writeLong(noops)
        out.writeLong(failures)
        out.writeLong(tookMillis)
    }

    @Throws(IOException::class)
    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .field("updated", updated)
            .field("noops", noops)
            .field("failures", failures)
            .field("took_millis", tookMillis)
            .endObject()
    }
}

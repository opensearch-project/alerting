/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.alerting.core.schedule

import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentFragment
import org.opensearch.common.xcontent.XContentBuilder
import java.time.Instant

class JobSchedulerMetrics : ToXContentFragment, Writeable {
    val scheduledJobId: String
    val lastExecutionTime: Long?
    val runningOnTime: Boolean

    constructor(scheduledJobId: String, lastExecutionTime: Long?, runningOnTime: Boolean) {
        this.scheduledJobId = scheduledJobId
        this.lastExecutionTime = lastExecutionTime
        this.runningOnTime = runningOnTime
    }

    constructor(si: StreamInput) {
        scheduledJobId = si.readString()
        lastExecutionTime = si.readOptionalLong()
        runningOnTime = si.readBoolean()
    }

    override fun writeTo(out: StreamOutput) {
        out.writeString(scheduledJobId)
        out.writeOptionalLong(lastExecutionTime)
        out.writeBoolean(runningOnTime)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        if (lastExecutionTime != null)
            builder.timeField("last_execution_time", "last_execution_time_in_millis",
                    Instant.ofEpochMilli(lastExecutionTime).toEpochMilli())
        builder.field("running_on_time", runningOnTime)
        return builder
    }
}

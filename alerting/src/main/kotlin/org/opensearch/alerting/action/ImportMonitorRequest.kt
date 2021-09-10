/*
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package org.opensearch.alerting.action

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.alerting.model.Monitor
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import java.io.IOException

class ImportMonitorRequest : ActionRequest {
    var monitors: MutableList<Monitor>

    constructor(
        monitors: MutableList<Monitor>
    ): super() {
        this.monitors = monitors
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput): this(
//        monitors = Monitor.readFrom(sin) as Monitor
        monitors = sin.readList(::Monitor) as MutableList<Monitor>
    )

    override fun validate(): ActionRequestValidationException? {
        return null
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        this.monitors.forEach{ monitor ->
            monitor.writeTo(out)
        }
    }
}

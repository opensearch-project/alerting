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

import org.opensearch.action.ActionResponse
import org.opensearch.alerting.model.Monitor
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentBuilder
import java.io.IOException

class ImportMonitorResponse : ActionResponse, ToXContentObject {
//    var id: String
//    var version: Long
//    var seqNo: Long
//    var primaryTerm: Long
//    var status: RestStatus
    var monitors: MutableList<Monitor>

    constructor(
//        id: String,
//        version: Long,
//        seqNo: Long,
//        primaryTerm: Long,
//        status: RestStatus,
        monitors: MutableList<Monitor>
    ) : super() {
//        this.id = id
//        this.version = version
//        this.seqNo = seqNo
//        this.primaryTerm = primaryTerm
//        this.status = status
        this.monitors = monitors
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
//        sin.readString(), // id
//        sin.readLong(), // version
//        sin.readLong(), // seqNo
//        sin.readLong(), // primaryTerm
//        sin.readEnum(RestStatus::class.java), // status
        sin.readList(::Monitor) as MutableList<Monitor>
//        Monitor.readFrom(sin) as Monitor // monitor
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
//        out.writeString(id)
//        out.writeLong(version)
//        out.writeLong(seqNo)
//        out.writeLong(primaryTerm)
//        out.writeEnum(status)
        out.writeList(monitors) // Todo: Check to see if this works
//        monitors.writeTo(out)
    }

    @Throws(IOException::class)
    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
//            .field(_ID, id)
//            .field(_VERSION, version)
//            .field(_SEQ_NO, seqNo)
//            .field(_PRIMARY_TERM, primaryTerm)
            .field("monitors", monitors)
            .endObject()
    }
}

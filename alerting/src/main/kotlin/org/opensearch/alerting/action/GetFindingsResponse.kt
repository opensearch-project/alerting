/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.ActionResponse
import org.opensearch.alerting.model.FindingWithDocs
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.rest.RestStatus
import java.io.IOException

class GetFindingsResponse : ActionResponse, ToXContentObject {
    var status: RestStatus
    var totalFindings: Int?
    var findings: List<FindingWithDocs>

    constructor(
        status: RestStatus,
        totalFindings: Int?,
        findings: List<FindingWithDocs>
    ) : super() {
        this.status = status
        this.totalFindings = totalFindings
        this.findings = findings
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) {
        this.status = sin.readEnum(RestStatus::class.java)
        val findings = mutableListOf<FindingWithDocs>()
        this.totalFindings = sin.readOptionalInt()
        var currentSize = sin.readInt()
        for (i in 0 until currentSize) {
            findings.add(FindingWithDocs.readFrom(sin))
        }
        this.findings = findings
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeEnum(status)
        out.writeOptionalInt(totalFindings)
        out.writeInt(findings.size)
        for (finding in findings) {
            finding.writeTo(out)
        }
    }

    @Throws(IOException::class)
    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
            .field("total_findings", totalFindings)
            .field("findings", findings)

        return builder.endObject()
    }
}

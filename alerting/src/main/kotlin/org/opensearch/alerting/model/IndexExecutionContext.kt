/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model

import org.opensearch.commons.alerting.model.DocLevelQuery
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.common.io.stream.Writeable
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentBuilder
import java.io.IOException

data class IndexExecutionContext(
    val queries: List<DocLevelQuery>,
    val lastRunContext: MutableMap<String, Any>, // previous execution
    val updatedLastRunContext: MutableMap<String, Any>, // without sequence numbers
    val indexName: String,
    val concreteIndexName: String,
    val updatedIndexNames: List<String>,
    val concreteIndexNames: List<String>,
    val conflictingFields: List<String>,
    val docIds: List<String>? = emptyList(),
) : Writeable, ToXContent {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        queries = sin.readList { DocLevelQuery(sin) },
        lastRunContext = sin.readMap(),
        updatedLastRunContext = sin.readMap(),
        indexName = sin.readString(),
        concreteIndexName = sin.readString(),
        updatedIndexNames = sin.readStringList(),
        concreteIndexNames = sin.readStringList(),
        conflictingFields = sin.readStringList(),
        docIds = sin.readStringList()
    )

    override fun writeTo(out: StreamOutput?) {
        out!!.writeCollection(queries)
        out.writeMap(lastRunContext)
        out.writeMap(updatedLastRunContext)
        out.writeString(indexName)
        out.writeString(concreteIndexName)
        out.writeStringCollection(updatedIndexNames)
        out.writeStringCollection(concreteIndexNames)
        out.writeStringCollection(conflictingFields)
        out.writeOptionalStringCollection(docIds)
    }

    override fun toXContent(builder: XContentBuilder?, params: ToXContent.Params?): XContentBuilder {
        builder!!.startObject()
            .field("queries", queries)
            .field("last_run_context", lastRunContext)
            .field("updated_last_run_context", updatedLastRunContext)
            .field("index_name", indexName)
            .field("concrete_index_name", concreteIndexName)
            .field("udpated_index_names", updatedIndexNames)
            .field("concrete_index_names", concreteIndexNames)
            .field("conflicting_fields", conflictingFields)
            .field("doc_ids", docIds)
            .endObject()
        return builder
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model

import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentBuilder
import java.io.IOException

data class BucketLevelTriggerRunResult(
    override var triggerName: String,
    override var error: Exception? = null,
    var aggregationResultBuckets: Map<String, AggregationResultBucket>,
    var actionResultsMap: MutableMap<String, MutableMap<String, ActionRunResult>> = mutableMapOf()
) : TriggerRunResult(triggerName, error) {

    @Throws(IOException::class)
    @Suppress("UNCHECKED_CAST")
    constructor(sin: StreamInput) : this(
        sin.readString(),
        sin.readException() as Exception?, // error
        sin.readMap(StreamInput::readString, ::AggregationResultBucket),
        sin.readMap() as MutableMap<String, MutableMap<String, ActionRunResult>>
    )

    override fun internalXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder
            .field(AGG_RESULT_BUCKETS, aggregationResultBuckets)
            .field(ACTIONS_RESULTS, actionResultsMap as Map<String, Any>)
    }

    @Throws(IOException::class)
    @Suppress("UNCHECKED_CAST")
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeMap(aggregationResultBuckets, StreamOutput::writeString) {
                valueOut: StreamOutput, aggResultBucket: AggregationResultBucket ->
            aggResultBucket.writeTo(valueOut)
        }
        out.writeMap(actionResultsMap as Map<String, Any>)
    }

    companion object {
        const val AGG_RESULT_BUCKETS = "agg_result_buckets"
        const val ACTIONS_RESULTS = "action_results"

        @JvmStatic
        @Throws(IOException::class)
        fun readFrom(sin: StreamInput): TriggerRunResult {
            return BucketLevelTriggerRunResult(sin)
        }
    }
}

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

package org.opensearch.alerting.aggregation.bucketselectorext

import org.apache.lucene.util.BytesRef
import org.opensearch.alerting.aggregation.bucketselectorext.BucketSelectorExtAggregationBuilder.Companion.NAME
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.script.BucketAggregationSelectorScript
import org.opensearch.script.Script
import org.opensearch.search.DocValueFormat
import org.opensearch.search.aggregations.Aggregations
import org.opensearch.search.aggregations.InternalAggregation
import org.opensearch.search.aggregations.InternalAggregation.ReduceContext
import org.opensearch.search.aggregations.InternalMultiBucketAggregation
import org.opensearch.search.aggregations.bucket.SingleBucketAggregation
import org.opensearch.search.aggregations.bucket.composite.InternalComposite
import org.opensearch.search.aggregations.bucket.terms.IncludeExclude
import org.opensearch.search.aggregations.pipeline.BucketHelpers
import org.opensearch.search.aggregations.pipeline.BucketHelpers.GapPolicy
import org.opensearch.search.aggregations.pipeline.SiblingPipelineAggregator
import org.opensearch.search.aggregations.support.AggregationPath
import java.io.IOException

class BucketSelectorExtAggregator : SiblingPipelineAggregator {
    private var name: String? = null
    private var bucketsPathsMap: Map<String, String>
    private var parentBucketPath: String
    private var script: Script
    private var gapPolicy: GapPolicy
    private var bucketSelectorExtFilter: BucketSelectorExtFilter? = null

    constructor(
        name: String?,
        bucketsPathsMap: Map<String, String>,
        parentBucketPath: String,
        script: Script,
        gapPolicy: GapPolicy,
        filter: BucketSelectorExtFilter?,
        metadata: Map<String, Any>?
    ) : super(name, bucketsPathsMap.values.toTypedArray(), metadata) {
        this.bucketsPathsMap = bucketsPathsMap
        this.parentBucketPath = parentBucketPath
        this.script = script
        this.gapPolicy = gapPolicy
        this.bucketSelectorExtFilter = filter
    }

    /**
     * Read from a stream.
     */
    @Suppress("UNCHECKED_CAST")
    @Throws(IOException::class)
    constructor(sin: StreamInput) : super(sin.readString(), null, null) {
        script = Script(sin)
        gapPolicy = GapPolicy.readFrom(sin)
        bucketsPathsMap = sin.readMap() as Map<String, String>
        parentBucketPath = sin.readString()
        if (sin.readBoolean()) {
            bucketSelectorExtFilter = BucketSelectorExtFilter(sin)
        } else {
            bucketSelectorExtFilter = null
        }
    }

    @Throws(IOException::class)
    override fun doWriteTo(out: StreamOutput) {
        out.writeString(name)
        script.writeTo(out)
        gapPolicy.writeTo(out)
        out.writeGenericValue(bucketsPathsMap)
        out.writeString(parentBucketPath)
        if (bucketSelectorExtFilter != null) {
            out.writeBoolean(true)
            bucketSelectorExtFilter!!.writeTo(out)
        } else {
            out.writeBoolean(false)
        }
    }

    override fun getWriteableName(): String {
        return NAME.preferredName
    }

    override fun doReduce(aggregations: Aggregations, reduceContext: ReduceContext): InternalAggregation {
        val parentBucketPathList = AggregationPath.parse(parentBucketPath).pathElementsAsStringList
        var subAggregations: Aggregations = aggregations
        for (i in 0 until parentBucketPathList.size - 1) {
            subAggregations = subAggregations.get<SingleBucketAggregation>(parentBucketPathList[0]).aggregations
        }
        val originalAgg = subAggregations.get(parentBucketPathList.last()) as InternalMultiBucketAggregation<*, *>
        val buckets = originalAgg.buckets
        val factory = reduceContext.scriptService().compile(script, BucketAggregationSelectorScript.CONTEXT)
        val selectedBucketsIndex: MutableList<Int> = ArrayList()
        for (i in buckets.indices) {
            val bucket = buckets[i]
            if (bucketSelectorExtFilter != null) {
                var accepted = true
                if (bucketSelectorExtFilter!!.isCompositeAggregation) {
                    val compBucketKeyObj = (bucket as InternalComposite.InternalBucket).key
                    val filtersMap: HashMap<String, IncludeExclude>? = bucketSelectorExtFilter!!.filtersMap
                    for (sourceKey in compBucketKeyObj.keys) {
                        if (filtersMap != null) {
                            if (filtersMap.containsKey(sourceKey)) {
                                val obj = compBucketKeyObj[sourceKey]
                                accepted = isAccepted(obj!!, filtersMap[sourceKey])
                                if (!accepted) break
                            } else {
                                accepted = false
                                break
                            }
                        }
                    }
                } else {
                    accepted = isAccepted(bucket.key, bucketSelectorExtFilter!!.filters)
                }
                if (!accepted) continue
            }

            val vars: MutableMap<String, Any> = HashMap()
            if (script.params != null) {
                vars.putAll(script.params)
            }
            for ((varName, bucketsPath) in bucketsPathsMap) {
                val value = BucketHelpers.resolveBucketValue(originalAgg, bucket, bucketsPath, gapPolicy)
                vars[varName] = value
            }
            val executableScript = factory.newInstance(vars)
            // TODO: can we use one instance of the script for all buckets? it should be stateless?
            if (executableScript.execute()) {
                selectedBucketsIndex.add(i)
            }
        }

        return BucketSelectorIndices(
            name(), parentBucketPath, selectedBucketsIndex, originalAgg.metadata
        )
    }

    private fun isAccepted(obj: Any, filter: IncludeExclude?): Boolean {
        return when (obj.javaClass) {
            String::class.java -> {
                val stringFilter = filter!!.convertToStringFilter(DocValueFormat.RAW)
                stringFilter.accept(BytesRef(obj as String))
            }
            java.lang.Long::class.java, Long::class.java -> {
                val longFilter = filter!!.convertToLongFilter(DocValueFormat.RAW)
                longFilter.accept(obj as Long)
            }
            java.lang.Double::class.java, Double::class.java -> {
                val doubleFilter = filter!!.convertToDoubleFilter()
                doubleFilter.accept(obj as Long)
            }
            else -> {
                throw IllegalStateException("Object is not comparable. Please use one of String, Long or Double type.")
            }
        }
    }
}

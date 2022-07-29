/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model

import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentBuilder

data class ClusterMetricsDataPoint(
    var metric: MetricType,
    var timestamp: String,
    var value: String,
    var minimum: String? = null,
    var maximum: String? = null
) : ToXContent {

    companion object {
        val TIMESTAMP_FIELD = "timestamp"
        val VALUE_FIELD = "value"
        val MINIMUM_FIELD = "minimum"
        val MAXIMUM_FIELD = "maximum"
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        val output = builder.startObject()
            .startObject(metric.metricName)
            .field(TIMESTAMP_FIELD, timestamp)
            .field(VALUE_FIELD, value)
        if (metric === MetricType.JVM_PRESSURE || metric === MetricType.CPU_USAGE) {
            builder.field(MINIMUM_FIELD, minimum)
            builder.field(MAXIMUM_FIELD, maximum)
        }
        output
            .endObject()
            .endObject()
        return output
    }

    enum class MetricType(
        val metricName: String
    ) {
        CLUSTER_STATUS(
            "cluster_status"
        ),
        JVM_PRESSURE(
            "jvm_pressure"
        ),
        CPU_USAGE(
            "cpu_usage"
        ),
        UNASSIGNED_SHARDS(
            "unassigned_shards"
        ),
        NUMBER_OF_PENDING_TASKS(
            "number_of_pending_tasks"
        ),
        ACTIVE_SHARDS(
            "active_shards"
        ),
        RELOCATING_SHARDS(
            "relocating_shards"
        )
    }
}

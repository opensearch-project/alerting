package org.opensearch.alerting.model

import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentBuilder

data class ClusterMetricsDataPoint(
    var metric: MetricType,
    var timestamp: String,
    var value: String
) : ToXContent {

    companion object {
        val TIMESTAMP_FIELD = "timestamp"
        val VALUE_FIELD = "value"
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .startObject(metric.metricName)
            .field(TIMESTAMP_FIELD, timestamp)
            .field(VALUE_FIELD, value)
            .endObject()
            .endObject()
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
        )
    }
}

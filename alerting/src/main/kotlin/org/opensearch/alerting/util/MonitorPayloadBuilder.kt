/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.core.xcontent.ToXContent
import java.io.ByteArrayOutputStream
import java.util.Base64
import java.util.zip.GZIPOutputStream

/**
 * Builds the SQS message payload (EB Target.Input) per LLD section 5.2.
 *
 * Top-level fields for routing, nested monitorConfig (gzip+base64) with full monitor definition.
 * job_start_time uses EB's <aws.scheduler.scheduled-time> context attribute.
 */
object MonitorPayloadBuilder {

    private const val GZIP_THRESHOLD = 4096 // bytes — only compress if config exceeds this

    fun buildTargetInput(
        monitor: Monitor,
        appId: String,
        tenantId: String,
        workspaceId: String,
        collectionEndpoint: String
    ): String {
        val monitorConfigJson = serializeMonitorConfig(monitor)
        val encodedConfig = if (monitorConfigJson.length > GZIP_THRESHOLD) {
            gzipAndBase64(monitorConfigJson)
        } else {
            monitorConfigJson
        }

        val builder = XContentFactory.jsonBuilder()
        builder.startObject()
        builder.field("job_start_time", "<aws.scheduler.scheduled-time>")
        builder.field("appId", appId)
        builder.field("tenantId", tenantId)
        builder.field("monitorId", monitor.id)
        builder.field("workspaceId", workspaceId)
        builder.field("collectionEndpoint", collectionEndpoint)
        builder.field("monitorConfig", encodedConfig)
        builder.field("compressed", monitorConfigJson.length > GZIP_THRESHOLD)
        builder.endObject()
        return builder.toString()
    }

    private fun serializeMonitorConfig(monitor: Monitor): String {
        val builder = XContentFactory.jsonBuilder()
        monitor.toXContent(builder, ToXContent.EMPTY_PARAMS)
        return builder.toString()
    }

    private fun gzipAndBase64(input: String): String {
        val bos = ByteArrayOutputStream()
        GZIPOutputStream(bos).use { it.write(input.toByteArray(Charsets.UTF_8)) }
        return Base64.getEncoder().encodeToString(bos.toByteArray())
    }
}

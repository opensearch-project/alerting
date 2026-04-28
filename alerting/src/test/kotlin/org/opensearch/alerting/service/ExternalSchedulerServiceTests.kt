/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.service

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.commons.alerting.model.IntervalSchedule
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.ScheduleJobPayload
import org.opensearch.commons.alerting.model.SearchInput
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.index.query.QueryBuilders
import org.opensearch.search.builder.SearchSourceBuilder
import java.time.Instant
import java.time.temporal.ChronoUnit

class ExternalSchedulerServiceTests {

    private fun testMonitor(
        enabled: Boolean = true,
        interval: Int = 5,
        unit: ChronoUnit = ChronoUnit.MINUTES
    ): Monitor {
        return Monitor(
            name = "test-monitor",
            monitorType = Monitor.MonitorType.QUERY_LEVEL_MONITOR.value,
            enabled = enabled,
            inputs = listOf(SearchInput(emptyList(), SearchSourceBuilder().query(QueryBuilders.matchAllQuery()))),
            schedule = IntervalSchedule(interval, unit),
            triggers = emptyList(),
            enabledTime = if (enabled) Instant.now().truncatedTo(ChronoUnit.MILLIS) else null,
            lastUpdateTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            user = null,
            uiMetadata = mapOf()
        )
    }

    private fun buildPayloadJson(monitor: Monitor): String {
        val configBuilder = XContentFactory.jsonBuilder()
        monitor.toXContent(configBuilder, ToXContent.EMPTY_PARAMS)
        val payload = ScheduleJobPayload(
            monitorId = monitor.id,
            jobStartTime = ExternalSchedulerService.EB_SCHEDULED_TIME_PLACEHOLDER,
            monitorConfig = configBuilder.toString()
        )
        val builder = XContentFactory.jsonBuilder()
        payload.toXContent(builder, ToXContent.EMPTY_PARAMS)
        return builder.toString()
    }

    @Test
    fun `scheduleName formats correctly`() {
        assertEquals("monitor-abc123", ExternalSchedulerService.scheduleName("abc123"))
    }

    @Test
    fun `payload json contains all ScheduleJobPayload fields`() {
        val monitor = testMonitor()
        val json = buildPayloadJson(monitor)
        assertTrue(json.contains("\"${ScheduleJobPayload.MONITOR_ID_FIELD}\""))
        assertTrue(json.contains("\"${ScheduleJobPayload.JOB_START_TIME_FIELD}\""))
        assertTrue(json.contains("\"${ScheduleJobPayload.MONITOR_CONFIG_FIELD}\""))
        assertTrue(json.contains(ExternalSchedulerService.EB_SCHEDULED_TIME_PLACEHOLDER))
    }

    @Test
    fun `createSchedule throws when credentialsCache not set`() {
        ExternalSchedulerService.credentialsCache = null
        ExternalSchedulerService.initialize(
            org.opensearch.common.settings.Settings.builder()
                .put("plugins.alerting.remote_metadata_region", "us-west-2").build()
        )
        val monitor = testMonitor()
        val routing = SchedulerRoutingResolver.Routing("111111111111", "queue", "arn:aws:iam::111:role/test")
        try {
            ExternalSchedulerService.createSchedule(monitor, routing, buildPayloadJson(monitor))
            throw AssertionError("Expected IllegalArgumentException")
        } catch (e: IllegalArgumentException) {
            assertTrue(e.message!!.contains("credentialsCache"))
        }
    }

    @Test
    fun `deleteSchedule throws when credentialsCache not set`() {
        ExternalSchedulerService.credentialsCache = null
        ExternalSchedulerService.initialize(
            org.opensearch.common.settings.Settings.builder()
                .put("plugins.alerting.remote_metadata_region", "us-west-2").build()
        )
        val routing = SchedulerRoutingResolver.Routing("333333333333", "queue", "arn:aws:iam::333:role/test")
        try {
            ExternalSchedulerService.deleteSchedule("mon-3", routing)
            throw AssertionError("Expected IllegalArgumentException")
        } catch (e: IllegalArgumentException) {
            assertTrue(e.message!!.contains("credentialsCache"))
        }
    }
}

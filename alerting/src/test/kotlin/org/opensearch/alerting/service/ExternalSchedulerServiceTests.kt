/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.service

import org.junit.Assert.assertEquals
import org.junit.Test
import org.opensearch.alerting.service.ExternalSchedulerService.ScheduleRequest
import org.opensearch.alerting.util.ScheduleTranslator
import org.opensearch.commons.alerting.model.IntervalSchedule
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.SearchInput
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

    @Test
    fun `scheduleName formats correctly`() {
        assertEquals("monitor-abc123", ExternalSchedulerService.scheduleName("abc123"))
    }

    @Test
    fun `createSchedule does not throw`() {
        // Verifies no exception — SDK calls are TODO stubs for OSSA-608
        ExternalSchedulerService.createSchedule(
            testMonitor(), "111111111111", "us-east-1",
            "arn:aws:sqs:us-east-1:111:queue", "arn:aws:iam::111:role/test", "{}"
        )
    }

    @Test
    fun `updateSchedule does not throw`() {
        ExternalSchedulerService.updateSchedule(
            testMonitor(), "222222222222", "us-west-2",
            "arn:aws:sqs:us-west-2:222:queue", "arn:aws:iam::222:role/test", "{}"
        )
    }

    @Test
    fun `deleteSchedule accepts all required params`() {
        val monitorId = "mon-3"
        val account = "333333333333"
        val region = "eu-west-1"
        val roleArn = "arn:aws:iam::333:role/eb-role"
        // Verifies no exception — SDK calls are TODO stubs for OSSA-608
        ExternalSchedulerService.deleteSchedule(monitorId, account, region, roleArn)
    }

    @Test
    fun `ScheduleRequest captures all fields`() {
        val monitor = testMonitor(interval = 10)
        val (expr, tz) = ScheduleTranslator.toEventBridgeExpression(monitor.schedule)
        val req = ScheduleRequest(
            name = ExternalSchedulerService.scheduleName("mon-1"),
            scheduleExpression = expr,
            timezone = tz,
            targetArn = "arn:aws:sqs:us-west-2:222:AlertingJobQueue",
            targetRoleArn = "arn:aws:iam::222:role/eb-role",
            targetInput = "{\"monitorConfig\":\"test\"}",
            enabled = true,
            ebCellAccountId = "222222222222",
            ebCellRegion = "us-west-2"
        )
        assertEquals("monitor-mon-1", req.name)
        assertEquals("rate(10 minutes)", req.scheduleExpression)
        assertEquals("222222222222", req.ebCellAccountId)
        assertEquals("us-west-2", req.ebCellRegion)
        assertEquals("arn:aws:iam::222:role/eb-role", req.targetRoleArn)
        assertEquals("arn:aws:sqs:us-west-2:222:AlertingJobQueue", req.targetArn)
        assertEquals("{\"monitorConfig\":\"test\"}", req.targetInput)
        assertEquals(true, req.enabled)
    }
}

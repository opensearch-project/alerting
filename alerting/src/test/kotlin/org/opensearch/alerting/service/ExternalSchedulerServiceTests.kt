/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.service

import org.junit.Assert.assertEquals
import org.junit.Test
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
        val routing = SchedulerRoutingResolver.Routing("111111111111", "arn:aws:sqs:us-east-1:111:queue", "arn:aws:iam::111:role/test")
        ExternalSchedulerService.createSchedule(testMonitor(), routing, "{}")
    }

    @Test
    fun `updateSchedule does not throw`() {
        val routing = SchedulerRoutingResolver.Routing("222222222222", "arn:aws:sqs:us-west-2:222:queue", "arn:aws:iam::222:role/test")
        ExternalSchedulerService.updateSchedule(testMonitor(), routing, "{}")
    }

    @Test
    fun `deleteSchedule accepts all required params`() {
        val routing = SchedulerRoutingResolver.Routing("333333333333", "", "arn:aws:iam::333:role/eb-role")
        ExternalSchedulerService.deleteSchedule("mon-3", routing)
    }
}

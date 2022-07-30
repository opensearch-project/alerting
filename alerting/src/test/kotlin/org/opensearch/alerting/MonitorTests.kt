/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.opensearch.alerting.model.Monitor
import org.opensearch.alerting.model.Trigger
import org.opensearch.alerting.util._ID
import org.opensearch.alerting.util._VERSION
import org.opensearch.test.OpenSearchTestCase
import java.time.Instant

class MonitorTests : OpenSearchTestCase() {

    fun `test enabled time`() {
        val monitor = randomQueryLevelMonitor()
        val enabledMonitor = monitor.copy(enabled = true, enabledTime = Instant.now())
        try {
            enabledMonitor.copy(enabled = false)
            fail("Disabling monitor with enabled time set should fail.")
        } catch (e: IllegalArgumentException) {
        }

        val disabledMonitor = monitor.copy(enabled = false, enabledTime = null)

        try {
            disabledMonitor.copy(enabled = true)
            fail("Enabling monitor without enabled time should fail")
        } catch (e: IllegalArgumentException) {
        }
    }

    fun `test max triggers`() {
        val monitor = randomQueryLevelMonitor()

        val tooManyTriggers = mutableListOf<Trigger>()
        for (i in 0..10) {
            tooManyTriggers.add(randomQueryLevelTrigger())
        }

        try {
            monitor.copy(triggers = tooManyTriggers)
            fail("Monitor with too many triggers should be rejected.")
        } catch (e: IllegalArgumentException) {
        }
    }

    fun `test monitor as template args`() {
        // GIVEN
        val monitor = randomMonitor()

        // WHEN
        val templateArgs = monitor.asTemplateArg()

        // THEN
        assertEquals("Template args id does not match", templateArgs[_ID], monitor.id)
        assertEquals("Template args version does not match", templateArgs[_VERSION], monitor.version)
        assertEquals("Template args name does not match", templateArgs[Monitor.NAME_FIELD], monitor.name)
        assertEquals("Template args monitor type does not match\nhere's templateArgs: ", templateArgs[Monitor.MONITOR_TYPE_FIELD], monitor.monitorType.toString())
        assertEquals("Template args enabled field does not match", templateArgs[Monitor.ENABLED_FIELD], monitor.enabled)
        assertEquals("Template args enabled time does not match", templateArgs[Monitor.ENABLED_TIME_FIELD], monitor.enabledTime.toString())
        assertEquals("Template args last update time does not match", templateArgs[Monitor.LAST_UPDATE_TIME_FIELD], monitor.lastUpdateTime.toString())
        assertEquals("Template args schedule does not match", templateArgs[Monitor.SCHEDULE_FIELD], monitor.schedule.asTemplateArg())
        assertEquals(
            "Template args inputs does not contain the expected number of inputs",
            monitor.inputs.size,
            (templateArgs[Monitor.INPUTS_FIELD] as List<*>).size
        )
        monitor.inputs.forEach {
            assertTrue(
                "Template args 'queries' field does not match:",
                (templateArgs[Monitor.INPUTS_FIELD] as List<*>).contains(it.asTemplateArg())
            )
        }
    }
}

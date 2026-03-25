/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.opensearch.commons.alerting.model.Trigger
import org.opensearch.test.OpenSearchTestCase
import java.lang.IllegalArgumentException
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
        // Monitor no longer validates trigger count at construction time.
        // Trigger count validation is now handled at the transport action level
        // via the configurable MAX_TRIGGERS_PER_MONITOR setting.
        val monitor = randomQueryLevelMonitor()

        val manyTriggers = mutableListOf<Trigger>()
        for (i in 0..10) {
            manyTriggers.add(randomQueryLevelTrigger())
        }

        // Should not throw — validation moved to transport layer
        val monitorWithManyTriggers = monitor.copy(triggers = manyTriggers)
        assertEquals(11, monitorWithManyTriggers.triggers.size)
    }
}

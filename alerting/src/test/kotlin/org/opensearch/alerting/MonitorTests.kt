/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

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
}

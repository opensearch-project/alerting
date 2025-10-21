/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.opensearch.alerting.modelv2.PPLTrigger
import org.opensearch.test.OpenSearchTestCase
import java.lang.IllegalArgumentException
import java.time.Instant

class MonitorV2Tests : OpenSearchTestCase() {
    fun `test enabled time`() {
        val pplMonitor = randomPPLMonitor(enabled = true, enabledTime = Instant.now())
        try {
            pplMonitor.makeCopy(enabled = false)
            fail("Disabling monitor with enabled time set should fail.")
        } catch (_: IllegalArgumentException) {}

        val disabledMonitor = pplMonitor.copy(enabled = false, enabledTime = null)

        try {
            disabledMonitor.makeCopy(enabled = true)
            fail("Enabling monitor without enabled time should fail")
        } catch (_: IllegalArgumentException) {}
    }

    fun `test max triggers`() {
        val tooManyTriggers = mutableListOf<PPLTrigger>()
        for (i in 0..10) { // 11 times
            tooManyTriggers.add(randomPPLTrigger())
        }

        try {
            randomPPLMonitor(triggers = tooManyTriggers)
            fail("Monitor with too many triggers should be rejected.")
        } catch (_: IllegalArgumentException) {}
    }

    fun `test min throttle duration`() {
        try {
            randomPPLTrigger(throttleDuration = 0)
            fail("Trigger with throttle duration less than 1 should be rejected")
        } catch (_: IllegalArgumentException) {}
    }

    fun `test min expire duration`() {
        try {
            randomPPLTrigger(expireDuration = 0)
            fail("Trigger with expire duration less than 1 should be rejected")
        } catch (_: IllegalArgumentException) {}
    }
}

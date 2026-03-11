/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.modelv2

import org.opensearch.alerting.assertPplMonitorsEqual
import org.opensearch.alerting.modelv2.MonitorV2.Companion.ALERTING_V2_MAX_NAME_LENGTH
import org.opensearch.alerting.modelv2.MonitorV2.Companion.ENABLED_FIELD
import org.opensearch.alerting.modelv2.MonitorV2.Companion.ENABLED_TIME_FIELD
import org.opensearch.alerting.modelv2.MonitorV2.Companion.LAST_UPDATE_TIME_FIELD
import org.opensearch.alerting.modelv2.MonitorV2.Companion.LOOK_BACK_WINDOW_FIELD
import org.opensearch.alerting.modelv2.MonitorV2.Companion.MONITOR_V2_MIN_LOOK_BACK_WINDOW
import org.opensearch.alerting.modelv2.MonitorV2.Companion.NAME_FIELD
import org.opensearch.alerting.modelv2.MonitorV2.Companion.SCHEDULE_FIELD
import org.opensearch.alerting.modelv2.PPLSQLMonitor.Companion.QUERY_FIELD
import org.opensearch.alerting.randomPPLMonitor
import org.opensearch.alerting.randomPPLTrigger
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.commons.alerting.util.IndexUtils.Companion._ID
import org.opensearch.commons.alerting.util.IndexUtils.Companion._VERSION
import org.opensearch.core.common.io.stream.StreamInput
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
        val tooManyTriggers = mutableListOf<PPLSQLTrigger>()
        for (i in 0..10) { // 11 times
            tooManyTriggers.add(randomPPLTrigger())
        }

        try {
            randomPPLMonitor(triggers = tooManyTriggers)
            fail("Monitor with too many triggers should be rejected.")
        } catch (_: IllegalArgumentException) {}
    }

    fun `test monitor name too long`() {
        var monitorName = ""
        for (i in 0 until ALERTING_V2_MAX_NAME_LENGTH + 1) {
            monitorName += "a"
        }

        try {
            randomPPLMonitor(name = monitorName)
            fail("Monitor with too long a name should be rejected.")
        } catch (_: IllegalArgumentException) {}
    }

    fun `test monitor min look back window`() {
        try {
            randomPPLMonitor(
                lookBackWindow = MONITOR_V2_MIN_LOOK_BACK_WINDOW - 1
            )
            fail("Monitor with too long a name should be rejected.")
        } catch (_: IllegalArgumentException) {}
    }

    fun `test monitor no triggers`() {
        try {
            randomPPLMonitor(
                triggers = listOf()
            )
            fail("Monitor without triggers be rejected.")
        } catch (_: IllegalArgumentException) {}
    }

    fun `test monitor with look back window without timestamp field`() {
        try {
            randomPPLMonitor(
                lookBackWindow = randomLongBetween(1, 10),
                timestampField = null
            )
            fail("Monitor with look back window but without timestamp field be rejected.")
        } catch (_: IllegalArgumentException) {}
    }

    fun `test monitor without look back window with timestamp field`() {
        try {
            randomPPLMonitor(
                lookBackWindow = null,
                timestampField = "some_timestamp_field"
            )
            fail("Monitor without look back window but with timestamp field be rejected.")
        } catch (_: IllegalArgumentException) {}
    }

    fun `test monitor v2 as stream`() {
        val pplMonitor = randomPPLMonitor()
        val monitorV2 = pplMonitor as MonitorV2
        val out = BytesStreamOutput()
        MonitorV2.writeTo(out, monitorV2)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newMonitorV2 = MonitorV2.readFrom(sin)
        val newPplMonitor = newMonitorV2 as PPLSQLMonitor
        assertPplMonitorsEqual(pplMonitor, newPplMonitor)
    }

    fun `test ppl monitor as stream`() {
        val pplMonitor = randomPPLMonitor()
        val out = BytesStreamOutput()
        pplMonitor.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newPplMonitor = PPLSQLMonitor(sin)
        assertPplMonitorsEqual(pplMonitor, newPplMonitor)
    }

    fun `test ppl monitor asTemplateArgs`() {
        val pplMonitor = randomPPLMonitor()
        val templateArgs = pplMonitor.asTemplateArg()

        assertEquals(
            "Template args field $_ID doesn't match",
            pplMonitor.id,
            templateArgs[_ID]
        )
        assertEquals(
            "Template args field $_VERSION doesn't match",
            pplMonitor.version,
            templateArgs[_VERSION]
        )
        assertEquals(
            "Template args field $NAME_FIELD doesn't match",
            pplMonitor.name,
            templateArgs[NAME_FIELD]
        )
        assertEquals(
            "Template args field $ENABLED_FIELD doesn't match",
            pplMonitor.enabled,
            templateArgs[ENABLED_FIELD]
        )
        assertNotNull(templateArgs[SCHEDULE_FIELD])
        assertEquals(
            "Template args field $LOOK_BACK_WINDOW_FIELD doesn't match",
            pplMonitor.lookBackWindow,
            templateArgs[LOOK_BACK_WINDOW_FIELD]
        )
        assertEquals(
            "Template args field $LAST_UPDATE_TIME_FIELD doesn't match",
            pplMonitor.lastUpdateTime.toEpochMilli(),
            templateArgs[LAST_UPDATE_TIME_FIELD]
        )
        assertEquals(
            "Template args field $ENABLED_TIME_FIELD doesn't match",
            pplMonitor.enabledTime?.toEpochMilli(),
            templateArgs[ENABLED_TIME_FIELD]
        )
        assertEquals(
            "Template args field $QUERY_FIELD doesn't match",
            pplMonitor.query,
            templateArgs[QUERY_FIELD]
        )
    }
}

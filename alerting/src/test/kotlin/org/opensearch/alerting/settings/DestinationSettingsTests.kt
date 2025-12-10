/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.settings

import org.junit.Before
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.common.settings.Settings
import org.opensearch.test.OpenSearchTestCase
import kotlin.test.Test

class DestinationSettingsTests : OpenSearchTestCase() {
    private lateinit var plugin: AlertingPlugin

    @Before
    fun setup() {
        plugin = AlertingPlugin()
    }

    @Test
    fun `test all opendistro destination settings returned`() {
        val settings = plugin.settings
        assertTrue(
            "Legacy Settings are not returned",
            settings.containsAll(
                listOf<Any>(
                    LegacyOpenDistroDestinationSettings.ALLOW_LIST,
                    LegacyOpenDistroDestinationSettings.HOST_DENY_LIST,
                ),
            ),
        )
    }

    @Test
    fun `test all opensearch destination settings returned`() {
        val settings = plugin.settings
        assertTrue(
            "Opensearch settings not returned",
            settings.containsAll(
                listOf<Any>(
                    DestinationSettings.ALLOW_LIST,
                    DestinationSettings.HOST_DENY_LIST,
                ),
            ),
        )
    }

    @Test
    fun `test opendistro settings fallback`() {
        assertEquals(
            DestinationSettings.ALLOW_LIST.get(Settings.EMPTY),
            LegacyOpenDistroDestinationSettings.ALLOW_LIST.get(Settings.EMPTY),
        )
        assertEquals(
            DestinationSettings.HOST_DENY_LIST.get(Settings.EMPTY),
            LegacyOpenDistroDestinationSettings.HOST_DENY_LIST.get(Settings.EMPTY),
        )
    }

    @Test
    fun `test settings get Value with legacy fallback`() {
        val settings =
            Settings
                .builder()
                .putList("opendistro.alerting.destination.allow_list", listOf<String>("1"))
                .putList("opendistro.destination.host.deny_list", emptyList<String>())
                .build()

        assertEquals(DestinationSettings.ALLOW_LIST.get(settings), listOf<String>("1"))
        assertEquals(DestinationSettings.HOST_DENY_LIST.get(settings), emptyList<String>())

        assertSettingDeprecationsAndWarnings(
            arrayOf(
                LegacyOpenDistroDestinationSettings.ALLOW_LIST,
                LegacyOpenDistroDestinationSettings.HOST_DENY_LIST,
            ),
        )
    }
}

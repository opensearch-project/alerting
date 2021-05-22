package org.opensearch.alerting.settings

import org.junit.Before
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.common.settings.Settings
import org.opensearch.test.OpenSearchTestCase

class DestinationSettingsTests : OpenSearchTestCase() {
    private lateinit var plugin: AlertingPlugin

    @Before
    fun setup() {
        plugin = AlertingPlugin()
    }

    fun `test all opendistro destination settings returned`() {
        val settings = plugin.settings
        assertTrue(
            "Legacy Settings are not returned",
            settings.containsAll(
                listOf<Any>(
                    LegacyOpenDistroDestinationSettings.ALLOW_LIST,
                    LegacyOpenDistroDestinationSettings.HOST_DENY_LIST
                )
            )
        )
    }

    fun `test all opensearch destination settings returned`() {
        val settings = plugin.settings
        assertTrue(
            "Opensearch settings not returned",
            settings.containsAll(
                listOf<Any>(
                    DestinationSettings.ALLOW_LIST,
                    DestinationSettings.HOST_DENY_LIST
                )
            )
        )
    }

    fun `test opendistro settings fallback`() {
        assertEquals(
            DestinationSettings.ALLOW_LIST.get(Settings.EMPTY),
            LegacyOpenDistroDestinationSettings.ALLOW_LIST.get(Settings.EMPTY)
        )
    }

    fun `test settings get Value`() {
        val settings = Settings.builder().put("plugins.alerting.destination.allow_list", "1").build()
        assertNotNull(DestinationSettings.ALLOW_LIST.get(settings))
    }
}

package org.opensearch.alerting

import org.opensearch.alerting.settings.AlertingSettings

class AlertingSettingsIT : AlertingRestTestCase() {

    fun `test updating setting of overall max actions with less actions than the current maximum value`() {
        try {
            client().updateSettings(AlertingSettings.TOTAL_MAX_ACTIONS_ACROSS_TRIGGERS.key, -10)
        } catch (e: Exception) {
            assertTrue(e is IllegalArgumentException)
        }
    }

    fun `test updating setting of max actions per trigger with more actions than the maximum allowed actions across triggers`() {
        try {
            client().updateSettings(AlertingSettings.TOTAL_MAX_ACTIONS_PER_TRIGGER.key, 10)
        } catch (e: Exception) {
            assertTrue(e is IllegalArgumentException)
        }
    }
}

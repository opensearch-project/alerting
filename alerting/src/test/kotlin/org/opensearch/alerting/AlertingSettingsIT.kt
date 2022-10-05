/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.opensearch.alerting.settings.AlertingSettings

class AlertingSettingsIT : AlertingRestTestCase() {

    fun `test updating setting of overall max actions with less actions than the current maximum value`() {
        client().updateSettings(AlertingSettings.TOTAL_MAX_ACTIONS_ACROSS_TRIGGERS.key, -10)

        assert(MonitorRunnerService.monitorCtx.totalMaxActionsAcrossTriggers != -10)
    }

    fun `test updating setting of max actions per trigger with more actions than the maximum allowed actions across triggers`() {
        client().updateSettings(AlertingSettings.TOTAL_MAX_ACTIONS_PER_TRIGGER.key, 10)

        assert(MonitorRunnerService.monitorCtx.totalMaxActionsAcrossTriggers != 10)
    }
}

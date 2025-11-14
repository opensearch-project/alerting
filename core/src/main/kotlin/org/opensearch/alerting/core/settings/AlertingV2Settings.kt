/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.core.settings

import org.opensearch.common.settings.Setting

/**
 *  This class exclusively houses the Alerting V2 enabled setting, so that both Monitor V2 Stats
 *  and the rest of the CRUD APIs can read it
 */
class AlertingV2Settings {
    companion object {
        val ALERTING_V2_ENABLED = Setting.boolSetting(
            "plugins.alerting.v2.enabled",
            true,
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )
    }
}

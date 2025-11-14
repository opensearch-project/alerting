/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.modelv2

import org.opensearch.alerting.modelv2.PPLSQLTrigger.Companion.PPL_SQL_TRIGGER_FIELD
import org.opensearch.commons.alerting.model.action.Action
import org.opensearch.commons.notifications.model.BaseModel
import java.time.Instant

/**
 * Trigger V2 interface. All triggers of different v2 monitor
 * types must implement this interface
 *
 * @opensearch.experimental
 */
interface TriggerV2 : BaseModel {

    val id: String
    val name: String
    val severity: Severity
    val throttleDuration: Long?
    val expireDuration: Long
    var lastTriggeredTime: Instant?
    val actions: List<Action>

    enum class TriggerV2Type(val value: String) {
        PPL_TRIGGER(PPL_SQL_TRIGGER_FIELD);

        override fun toString(): String {
            return value
        }
    }

    enum class Severity(val value: String) {
        INFO("info"),
        ERROR("error"),
        LOW("low"),
        MEDIUM("medium"),
        HIGH("high"),
        CRITICAL("critical");

        companion object {
            fun enumFromString(value: String): Severity? {
                return values().find { it.value == value }
            }
        }
    }

    companion object {
        // field names
        const val ID_FIELD = "id"
        const val NAME_FIELD = "name"
        const val SEVERITY_FIELD = "severity"
        const val THROTTLE_FIELD = "throttle_minutes"
        const val LAST_TRIGGERED_FIELD = "last_triggered_time"
        const val EXPIRE_FIELD = "expires_minutes"
        const val ACTIONS_FIELD = "actions"

        // hard, nonadjustable limits
        const val MONITOR_V2_MIN_THROTTLE_DURATION_MINUTES = 1L // one minute min duration to match scheduled job interval granularity
        const val MONITOR_V2_MIN_EXPIRE_DURATION_MINUTES = 1L // one minute min duration to match scheduled job interval granularity
        const val NOTIFICATIONS_ID_MAX_LENGTH = 512 // length limit for notifications channel custom ID at channel creation time

        // default fallback values of fields if none are passed in
        const val DEFAULT_EXPIRE_DURATION = (7 * 24 * 60).toLong() // 7 days in minutes
    }
}

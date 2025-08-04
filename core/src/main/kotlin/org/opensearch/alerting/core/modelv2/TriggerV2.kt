package org.opensearch.alerting.core.modelv2

import org.opensearch.alerting.core.modelv2.PPLTrigger.Companion.PPL_TRIGGER_FIELD
import org.opensearch.common.unit.TimeValue
import org.opensearch.commons.alerting.model.action.Action
import org.opensearch.commons.notifications.model.BaseModel
import java.time.Instant

interface TriggerV2 : BaseModel {

    val id: String
    val name: String
    val severity: Severity
    val suppressDuration: TimeValue?
    val expireDuration: TimeValue?
    var lastTriggeredTime: Instant?
    val actions: List<Action>

    enum class TriggerV2Type(val value: String) {
        PPL_TRIGGER(PPL_TRIGGER_FIELD);

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
                return entries.find { it.value == value }
            }
        }
    }

    companion object {
        const val ID_FIELD = "id"
        const val NAME_FIELD = "name"
        const val SEVERITY_FIELD = "severity"
        const val SUPPRESS_FIELD = "suppress"
        const val LAST_TRIGGERED_FIELD = "last_triggered_time"
        const val EXPIRE_FIELD = "expires"
        const val ACTIONS_FIELD = "actions"
    }
}

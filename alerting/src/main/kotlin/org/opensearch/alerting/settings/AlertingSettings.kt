/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.settings

import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.action.GetMonitorAction
import org.opensearch.alerting.action.GetMonitorRequest
import org.opensearch.alerting.action.GetMonitorResponse
import org.opensearch.alerting.model.Trigger
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.client.Client
import org.opensearch.common.settings.Setting
import org.opensearch.common.unit.TimeValue
import org.opensearch.rest.RestRequest
import java.util.concurrent.TimeUnit

/**
 * settings specific to [AlertingPlugin]. These settings include things like history index max age, request timeout, etc...
 */
class AlertingSettings(val client: Client) {

    init {
        internalClient = client
    }

    companion object {
        internal var internalClient: Client? = null

        const val MONITOR_MAX_INPUTS = 1
        const val MONITOR_MAX_TRIGGERS = 10
        const val DEFAULT_MAX_ACTIONABLE_ALERT_COUNT = 50L
        const val UNBOUNDED_ACTIONS_ACROSS_TRIGGERS = -1
        const val DEFAULT_MAX_ACTIONS_ACROSS_TRIGGERS = UNBOUNDED_ACTIONS_ACROSS_TRIGGERS
        const val DEFAULT_TOTAL_MAX_ACTIONS_ACROSS_TRIGGERS = UNBOUNDED_ACTIONS_ACROSS_TRIGGERS

        val ALERTING_MAX_MONITORS = Setting.intSetting(
            "plugins.alerting.monitor.max_monitors",
            LegacyOpenDistroAlertingSettings.ALERTING_MAX_MONITORS,
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        val INPUT_TIMEOUT = Setting.positiveTimeSetting(
            "plugins.alerting.input_timeout",
            LegacyOpenDistroAlertingSettings.INPUT_TIMEOUT,
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        val INDEX_TIMEOUT = Setting.positiveTimeSetting(
            "plugins.alerting.index_timeout",
            LegacyOpenDistroAlertingSettings.INDEX_TIMEOUT,
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        val BULK_TIMEOUT = Setting.positiveTimeSetting(
            "plugins.alerting.bulk_timeout",
            LegacyOpenDistroAlertingSettings.BULK_TIMEOUT,
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        val ALERT_BACKOFF_MILLIS = Setting.positiveTimeSetting(
            "plugins.alerting.alert_backoff_millis",
            LegacyOpenDistroAlertingSettings.ALERT_BACKOFF_MILLIS,
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        val ALERT_BACKOFF_COUNT = Setting.intSetting(
            "plugins.alerting.alert_backoff_count",
            LegacyOpenDistroAlertingSettings.ALERT_BACKOFF_COUNT,
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        val MOVE_ALERTS_BACKOFF_MILLIS = Setting.positiveTimeSetting(
            "plugins.alerting.move_alerts_backoff_millis",
            LegacyOpenDistroAlertingSettings.MOVE_ALERTS_BACKOFF_MILLIS,
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        val MOVE_ALERTS_BACKOFF_COUNT = Setting.intSetting(
            "plugins.alerting.move_alerts_backoff_count",
            LegacyOpenDistroAlertingSettings.MOVE_ALERTS_BACKOFF_COUNT,
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        val ALERT_HISTORY_ENABLED = Setting.boolSetting(
            "plugins.alerting.alert_history_enabled",
            LegacyOpenDistroAlertingSettings.ALERT_HISTORY_ENABLED,
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        // TODO: Do we want to let users to disable this? If so, we need to fix the rollover logic
        //  such that the main index is findings and rolls over to the finding history index
        val FINDING_HISTORY_ENABLED = Setting.boolSetting(
            "plugins.alerting.alert_finding_enabled",
            true,
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        val ALERT_HISTORY_ROLLOVER_PERIOD = Setting.positiveTimeSetting(
            "plugins.alerting.alert_history_rollover_period",
            LegacyOpenDistroAlertingSettings.ALERT_HISTORY_ROLLOVER_PERIOD,
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        val FINDING_HISTORY_ROLLOVER_PERIOD = Setting.positiveTimeSetting(
            "plugins.alerting.alert_finding_rollover_period",
            TimeValue.timeValueHours(12),
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        val ALERT_HISTORY_INDEX_MAX_AGE = Setting.positiveTimeSetting(
            "plugins.alerting.alert_history_max_age",
            LegacyOpenDistroAlertingSettings.ALERT_HISTORY_INDEX_MAX_AGE,
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        val FINDING_HISTORY_INDEX_MAX_AGE = Setting.positiveTimeSetting(
            "plugins.alerting.finding_history_max_age",
            TimeValue(30, TimeUnit.DAYS),
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        val ALERT_HISTORY_MAX_DOCS = Setting.longSetting(
            "plugins.alerting.alert_history_max_docs",
            LegacyOpenDistroAlertingSettings.ALERT_HISTORY_MAX_DOCS,
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        val FINDING_HISTORY_MAX_DOCS = Setting.longSetting(
            "plugins.alerting.alert_finding_max_docs",
            1000L,
            0L,
            Setting.Property.NodeScope, Setting.Property.Dynamic, Setting.Property.Deprecated
        )

        val ALERT_HISTORY_RETENTION_PERIOD = Setting.positiveTimeSetting(
            "plugins.alerting.alert_history_retention_period",
            LegacyOpenDistroAlertingSettings.ALERT_HISTORY_RETENTION_PERIOD,
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        val FINDING_HISTORY_RETENTION_PERIOD = Setting.positiveTimeSetting(
            "plugins.alerting.finding_history_retention_period",
            TimeValue(60, TimeUnit.DAYS),
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        val REQUEST_TIMEOUT = Setting.positiveTimeSetting(
            "plugins.alerting.request_timeout",
            LegacyOpenDistroAlertingSettings.REQUEST_TIMEOUT,
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        val MAX_ACTION_THROTTLE_VALUE = Setting.positiveTimeSetting(
            "plugins.alerting.action_throttle_max_value",
            LegacyOpenDistroAlertingSettings.MAX_ACTION_THROTTLE_VALUE,
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        val FILTER_BY_BACKEND_ROLES = Setting.boolSetting(
            "plugins.alerting.filter_by_backend_roles",
            LegacyOpenDistroAlertingSettings.FILTER_BY_BACKEND_ROLES,
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        val MAX_ACTIONABLE_ALERT_COUNT = Setting.longSetting(
            "plugins.alerting.max_actionable_alert_count",
            DEFAULT_MAX_ACTIONABLE_ALERT_COUNT,
            -1L,
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        val MAX_ACTIONS_ACROSS_TRIGGERS = Setting.intSetting(
            "plugins.alerting.max_actions_across_triggers",
            DEFAULT_MAX_ACTIONS_ACROSS_TRIGGERS,
            -1, MaxActionsTriggersValidator(internalClient),
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )
        val TOTAL_MAX_ACTIONS_ACROSS_TRIGGERS = Setting.intSetting(
            "plugins.alerting.total_max_actions_across_triggers",
            DEFAULT_TOTAL_MAX_ACTIONS_ACROSS_TRIGGERS,
            -1, TotalMaxActionsTriggersValidator(internalClient),
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        internal class TotalMaxActionsTriggersValidator(val client: Client?) : Setting.Validator<Int> {
            override fun validate(value: Int) {}

            override fun validate(value: Int, settings: Map<Setting<*>, Any>) {
                val maxActions = settings[MAX_ACTIONS_ACROSS_TRIGGERS] as Int
                validateActionsTrigger(maxActions, value, client)
            }

            override fun settings(): MutableIterator<Setting<*>> {
                val settings = mutableListOf<Setting<*>>(
                    MAX_ACTIONS_ACROSS_TRIGGERS
                )
                return settings.iterator()
            }
        }

        internal class MaxActionsTriggersValidator(val client: Client?) : Setting.Validator<Int> {
            override fun validate(value: Int) {}

            override fun validate(value: Int, settings: Map<Setting<*>, Any>) {
                val totalMaxActions = settings[TOTAL_MAX_ACTIONS_ACROSS_TRIGGERS] as Int
                validateActionsTrigger(value, totalMaxActions, client)
            }

            override fun settings(): MutableIterator<Setting<*>> {
                val settings = mutableListOf<Setting<*>>(
                    TOTAL_MAX_ACTIONS_ACROSS_TRIGGERS
                )
                return settings.iterator()
            }
        }

        private fun validateActionsTrigger(maxActions: Int, totalMaxActions: Int, client: Client?) {
            if (maxActions > totalMaxActions) {
                throw IllegalArgumentException(
                    "The limit number of actions for a single trigger, $maxActions, " +
                        "should not be greater than that of the overall max actions across all triggers of the monitor, $totalMaxActions"
                )
            }
            client?.let {
                GlobalScope.launch {
                    val triggers = getTriggers(it)

                    var currentAmountOfActions = 0

                    currentAmountOfActions += maxActions
                    currentAmountOfActions += triggers.sumOf { trigger ->
                        trigger.actions.size
                    }

                    if (currentAmountOfActions > totalMaxActions)
                        throw IllegalArgumentException(
                            "The amount of actions that the client wants to update plus the amount of actions that " +
                                "already exist, $currentAmountOfActions should not be greater than  that of the " +
                                "overall max actions across all triggers of the monitor, $totalMaxActions"
                        )
                }
            }
        }

        suspend fun getTriggers(client: Client): List<Trigger> {
            val getMonitorRequest = GetMonitorRequest("", 1L, RestRequest.Method.GET, null)

            val getMonitorResponse: GetMonitorResponse = client.suspendUntil {
                client.execute(GetMonitorAction.INSTANCE, getMonitorRequest, it)
            }
            return getMonitorResponse.monitor?.triggers ?: emptyList()
        }
        fun getCurrentAmountOfActions(triggers: List<Trigger>): Int {
            var currentAmountOfActions = 0

            currentAmountOfActions += triggers.sumOf { trigger ->
                trigger.actions.size
            }
            return currentAmountOfActions
        }
    }
}

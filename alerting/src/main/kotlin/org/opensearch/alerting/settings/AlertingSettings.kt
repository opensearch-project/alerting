/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.settings

import org.opensearch.alerting.AlertingPlugin
import org.opensearch.client.Client
import org.opensearch.common.settings.Setting
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import java.util.concurrent.TimeUnit

/**
 * settings specific to [AlertingPlugin]. These settings include things like history index max age, request timeout, etc...
 */
private val log = org.apache.logging.log4j.LogManager.getLogger(AlertingSettings::class.java)

class AlertingSettings(val client: Client, val settings: Settings) {

    init {
        internalClient = client
        internalSettings = settings
    }
    companion object {
        internal var internalClient: Client? = null
        internal var internalSettings: Settings? = null

        const val MONITOR_MAX_INPUTS = 1
        const val MONITOR_MAX_TRIGGERS = 10
        const val DEFAULT_MAX_ACTIONABLE_ALERT_COUNT = 50L
        val METRICS_EXECUTION_FREQUENCY_DEFAULT = Setting.positiveTimeSetting(
            "plugins.alerting.cluster_metrics.execution_frequency",
            TimeValue(15, TimeUnit.MINUTES),
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )
        val METRICS_STORE_TIME_DEFAULT = Setting.positiveTimeSetting(
            "plugins.alerting.cluster_metrics.metrics_history_max_age",
            TimeValue(7, TimeUnit.DAYS),
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )
        val MINIMUM_TIME_VALUE = TimeValue(1, TimeUnit.SECONDS)

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

        val METRICS_STORE_TIME = Setting.timeSetting(
            METRICS_STORE_TIME_DEFAULT.key,
            METRICS_STORE_TIME_DEFAULT,
            TimeValueValidator(internalClient, internalSettings),
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        val METRICS_EXECUTION_FREQUENCY = Setting.timeSetting(
            METRICS_EXECUTION_FREQUENCY_DEFAULT.key,
            METRICS_EXECUTION_FREQUENCY_DEFAULT,
            TimeValueValidator(internalClient, internalSettings),
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )
        internal class TimeValueValidator(val client: Client?, val clusterSettings: Settings?) : Setting.Validator<TimeValue> {
            override fun validate(value: TimeValue) {}

            override fun validate(value: TimeValue, settings: Map<Setting<*>, Any>) {
                log.info("THIS IS SETTING $settings")
                log.info("THIS IS VALUE $value")
                log.info("THIS IS CLUSTERSETTINGS $clusterSettings")
            }

            override fun settings(): MutableIterator<Setting<*>> {
                val settings = mutableListOf<Setting<*>>(
                    METRICS_EXECUTION_FREQUENCY
                )
                return settings.iterator()
            }
            private fun validateExecutionFrequency(executionFrequency: TimeValue, storageTime: TimeValue) {
                log.info("THIS IS VALIDATEEXECUTIONFREQUENCY PARAMS $executionFrequency, $storageTime")
                log.info("THIS IS MINIMUM_TIME_VALUE $MINIMUM_TIME_VALUE")

                if (executionFrequency < MINIMUM_TIME_VALUE || storageTime < MINIMUM_TIME_VALUE) {
                    throw IllegalArgumentException(
                        "Execution frequency or storage time cannot be less than $MINIMUM_TIME_VALUE."
                    )
                }
                if (executionFrequency < storageTime) return
                if (executionFrequency > storageTime) {
                    throw IllegalArgumentException(
                        "Execution frequency cannot be greater than the storage time."
                    )
                }
            }
        }
    }
}

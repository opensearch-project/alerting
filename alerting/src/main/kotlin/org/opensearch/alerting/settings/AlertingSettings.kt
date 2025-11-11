/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.settings

import org.opensearch.alerting.AlertingPlugin
import org.opensearch.common.settings.Setting
import org.opensearch.common.unit.TimeValue
import java.util.concurrent.TimeUnit

/**
 * settings specific to [AlertingPlugin]. These settings include things like history index max age, request timeout, etc...
 */
class AlertingSettings {

    companion object {
        const val DEFAULT_MAX_ACTIONABLE_ALERT_COUNT = 50L
        const val DEFAULT_FINDINGS_INDEXING_BATCH_SIZE = 1000
        const val DEFAULT_PERCOLATE_QUERY_NUM_DOCS_IN_MEMORY = 50000
        const val DEFAULT_PERCOLATE_QUERY_DOCS_SIZE_MEMORY_PERCENTAGE_LIMIT = 10
        const val DEFAULT_DOC_LEVEL_MONITOR_SHARD_FETCH_SIZE = 10000
        const val DEFAULT_MAX_DOC_LEVEL_MONITOR_FANOUT_MAX_DURATION_MINUTES = 3L
        const val DEFAULT_MAX_DOC_LEVEL_MONITOR_EXECUTION_MAX_DURATION_MINUTES = 4L
        const val DEFAULT_FAN_OUT_NODES = 1000

        val ALERTING_MAX_MONITORS = Setting.intSetting(
            "plugins.alerting.monitor.max_monitors",
            LegacyOpenDistroAlertingSettings.ALERTING_MAX_MONITORS,
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        /** Defines the threshold percentage of heap size in bytes till which we accumulate docs in memory before we query against percolate query
         * index in document level monitor execution.
         */
        val PERCOLATE_QUERY_DOCS_SIZE_MEMORY_PERCENTAGE_LIMIT = Setting.intSetting(
            "plugins.alerting.monitor.percolate_query_docs_size_memory_percentage_limit",
            10,
            0,
            100,
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        /** Purely a setting used to verify seq_no calculation
         */
        val DOC_LEVEL_MONITOR_SHARD_FETCH_SIZE = Setting.intSetting(
            "plugins.alerting.monitor.doc_level_monitor_shard_fetch_size",
            DEFAULT_DOC_LEVEL_MONITOR_SHARD_FETCH_SIZE,
            1,
            10000,
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        /** Setting to help timebox doc level monitor fanout
         */
        val DOC_LEVEL_MONITOR_FANOUT_MAX_DURATION = Setting.positiveTimeSetting(
            "plugins.alerting.monitor.doc_level_monitor_fanout_max_duration",
            TimeValue.timeValueMinutes(DEFAULT_MAX_DOC_LEVEL_MONITOR_FANOUT_MAX_DURATION_MINUTES),
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        /** Setting to help timebox doc level monitor execution
         */
        val DOC_LEVEL_MONITOR_EXECUTION_MAX_DURATION = Setting.positiveTimeSetting(
            "plugins.alerting.monitor.doc_level_monitor_execution_max_duration",
            TimeValue.timeValueMinutes(DEFAULT_MAX_DOC_LEVEL_MONITOR_EXECUTION_MAX_DURATION_MINUTES),
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        /** Defines the threshold of the maximum number of docs accumulated in memory to query against percolate query index in document
         * level monitor execution. The docs are being collected from searching on shards of indices mentioned in the
         * monitor input indices field. When the number of in-memory docs reaches or exceeds threshold we immediately perform percolate
         * query with the current set of docs and clear the cache and repeat the process till we have queried all indices in current
         * execution
         */
        val PERCOLATE_QUERY_MAX_NUM_DOCS_IN_MEMORY = Setting.intSetting(
            "plugins.alerting.monitor.percolate_query_max_num_docs_in_memory",
            DEFAULT_PERCOLATE_QUERY_NUM_DOCS_IN_MEMORY, 1000,
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        /**
         * Boolean setting to enable/disable optimizing doc level monitors by fetchign only fields mentioned in queries.
         * Enabled by default. If disabled, will fetch entire source of documents while fetch data from shards.
         */
        val DOC_LEVEL_MONITOR_FETCH_ONLY_QUERY_FIELDS_ENABLED = Setting.boolSetting(
            "plugins.alerting.monitor.doc_level_monitor_query_field_names_enabled",
            true,
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
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val CROSS_CLUSTER_MONITORING_ENABLED = Setting.boolSetting(
            "plugins.alerting.cross_cluster_monitoring_enabled",
            true,
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        val FINDINGS_INDEXING_BATCH_SIZE = Setting.intSetting(
            "plugins.alerting.alert_findings_indexing_batch_size",
            DEFAULT_FINDINGS_INDEXING_BATCH_SIZE,
            1,
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        val DOC_LEVEL_MONITOR_FAN_OUT_NODES = Setting.intSetting(
            "plugins.alerting.monitor.doc_level_monitor_fan_out_nodes",
            DEFAULT_FAN_OUT_NODES,
            1,
            Int.MAX_VALUE,
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        val ALERTING_COMMENTS_ENABLED = Setting.boolSetting(
            "plugins.alerting.comments_enabled",
            true,
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        val COMMENTS_HISTORY_MAX_DOCS = Setting.longSetting(
            "plugins.alerting.comments_history_max_docs",
            1000L,
            0L,
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        val COMMENTS_HISTORY_INDEX_MAX_AGE = Setting.positiveTimeSetting(
            "plugins.alerting.comments_history_max_age",
            TimeValue(30, TimeUnit.DAYS),
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        val COMMENTS_HISTORY_ROLLOVER_PERIOD = Setting.positiveTimeSetting(
            "plugins.alerting.comments_history_rollover_period",
            TimeValue(12, TimeUnit.HOURS),
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        val COMMENTS_HISTORY_RETENTION_PERIOD = Setting.positiveTimeSetting(
            "plugins.alerting.comments_history_retention_period",
            TimeValue(60, TimeUnit.DAYS),
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        val COMMENTS_MAX_CONTENT_SIZE = Setting.longSetting(
            "plugins.alerting.max_comment_character_length",
            2000L,
            0L,
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        val MAX_COMMENTS_PER_ALERT = Setting.longSetting(
            "plugins.alerting.max_comments_per_alert",
            500L,
            0L,
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        val MAX_COMMENTS_PER_NOTIFICATION = Setting.intSetting(
            "plugins.alerting.max_comments_per_notification",
            3,
            0,
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        val ALERT_V2_HISTORY_ENABLED = Setting.boolSetting(
            "plugins.alerting.v2.alert_history_enabled",
            true,
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        val ALERT_V2_HISTORY_ROLLOVER_PERIOD = Setting.positiveTimeSetting(
            "plugins.alerting.v2.alert_history_rollover_period",
            TimeValue(12, TimeUnit.HOURS),
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        val ALERT_V2_HISTORY_INDEX_MAX_AGE = Setting.positiveTimeSetting(
            "plugins.alerting.v2.alert_history_max_age",
            TimeValue(30, TimeUnit.DAYS),
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        val ALERT_V2_HISTORY_MAX_DOCS = Setting.longSetting(
            "plugins.alerting.v2.alert_history_max_docs",
            1000L, 0L,
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        val ALERT_V2_HISTORY_RETENTION_PERIOD = Setting.positiveTimeSetting(
            "plugins.alerting.v2.alert_history_retention_period",
            TimeValue(60, TimeUnit.DAYS),
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        val ALERT_V2_MONITOR_EXECUTION_MAX_DURATION = Setting.positiveTimeSetting(
            "plugins.alerting.v2.alert_monitor_execution_max_duration",
            TimeValue(4, TimeUnit.MINUTES),
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        val ALERTING_V2_MAX_MONITORS = Setting.intSetting(
            "plugins.alerting.v2.monitor.max_monitors",
            1000,
            1,
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        val ALERTING_V2_MAX_THROTTLE_DURATION = Setting.longSetting(
            "plugins.alerting.v2.monitor.max_throttle_duration",
            7200L, // 5 days, 7200 minutes
            2L,
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        val ALERTING_V2_MAX_EXPIRE_DURATION = Setting.longSetting(
            "plugins.alerting.v2.monitor.max_expire_duration",
            43200L, // 30 days, 43200 minutes
            2L,
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        val ALERTING_V2_MAX_LOOK_BACK_WINDOW = Setting.longSetting(
            "plugins.alerting.v2.monitor.max_look_back_window",
            10080L, // 7 days, 10080 minutes
            2L,
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        val ALERTING_V2_MAX_QUERY_LENGTH = Setting.longSetting(
            "plugins.alerting.v2.monitor.max_query_length",
            2000L,
            0L,
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        // max data rows to retrieve when executing PPL query against
        // SQL/PPL plugin during monitor execution
        val ALERTING_V2_QUERY_RESULTS_MAX_DATAROWS = Setting.longSetting(
            "plugins.alerting.v2.query_results_max_datarows",
            10000L,
            1L,
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        // max size of query results to store in alerts and notifications
        val ALERT_V2_QUERY_RESULTS_MAX_SIZE = Setting.longSetting(
            "plugins.alerting.v2.query_results_max_size",
            3000L,
            0L,
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        val ALERT_V2_PER_RESULT_TRIGGER_MAX_ALERTS = Setting.intSetting(
            "plugins.alerting.v2.per_result_trigger_max_alerts",
            10,
            1,
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        val NOTIFICATION_SUBJECT_SOURCE_MAX_LENGTH = Setting.intSetting(
            "plugins.alerting.v2.notification_subject_source_max_length",
            1000,
            100,
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        val NOTIFICATION_MESSAGE_SOURCE_MAX_LENGTH = Setting.intSetting(
            "plugins.alerting.v2.notification_message_source_max_length",
            3000,
            1000,
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.settings

import org.opensearch.alerting.AlertingPlugin
import org.opensearch.common.settings.Setting
import org.opensearch.common.unit.TimeValue
import org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_ENDPOINT_KEY
import org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_REGION_KEY
import org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_SERVICE_NAME_KEY
import org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_TYPE_KEY
import java.util.concurrent.TimeUnit
import java.util.function.Function

/**
 * settings specific to [AlertingPlugin]. These settings include things like history index max age, request timeout, etc...
 */
class AlertingSettings {

    companion object {
        const val DEFAULT_MAX_ACTIONABLE_ALERT_COUNT = 50L
        const val DEFAULT_FINDINGS_INDEXING_BATCH_SIZE = 1000
        private const val REMOTE_METADATA_KEY_PREFIX = "plugins.alerting"
        const val DEFAULT_PERCOLATE_QUERY_NUM_DOCS_IN_MEMORY = 50000
        const val DEFAULT_PERCOLATE_QUERY_DOCS_SIZE_MEMORY_PERCENTAGE_LIMIT = 10
        const val DEFAULT_DOC_LEVEL_MONITOR_SHARD_FETCH_SIZE = 10000
        const val DEFAULT_MAX_DOC_LEVEL_MONITOR_FANOUT_MAX_DURATION_MINUTES = 3L
        const val DEFAULT_MAX_DOC_LEVEL_MONITOR_EXECUTION_MAX_DURATION_MINUTES = 4L
        const val DEFAULT_FAN_OUT_NODES = 1000
        const val DEFAULT_MAX_TRIGGERS_PER_MONITOR = 10

        val ALERTING_MAX_MONITORS = Setting.intSetting(
            "plugins.alerting.monitor.max_monitors",
            LegacyOpenDistroAlertingSettings.ALERTING_MAX_MONITORS,
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        val MAX_TRIGGERS_PER_MONITOR = Setting.intSetting(
            "plugins.alerting.monitor.max_triggers",
            DEFAULT_MAX_TRIGGERS_PER_MONITOR,
            0,
            50,
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

        val NOTIFICATION_CONTEXT_RESULTS_ALLOWED_ROLES: Setting<List<String>> = Setting.listSetting(
            "plugins.alerting.notification_context_results_allowed_roles",
            listOf(),
            Function.identity(),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val MULTI_TENANCY_ENABLED: Setting<Boolean> = Setting.boolSetting(
            "$REMOTE_METADATA_KEY_PREFIX.multi_tenancy_enabled",
            false,
            Setting.Property.NodeScope,
            Setting.Property.Final
        )

        val REMOTE_METADATA_STORE_TYPE: Setting<String?> = Setting.simpleString(
            "$REMOTE_METADATA_KEY_PREFIX.$REMOTE_METADATA_TYPE_KEY",
            Setting.Property.NodeScope,
            Setting.Property.Final
        )

        val REMOTE_METADATA_ENDPOINT: Setting<String?> = Setting.simpleString(
            "$REMOTE_METADATA_KEY_PREFIX.$REMOTE_METADATA_ENDPOINT_KEY",
            Setting.Property.NodeScope,
            Setting.Property.Final
        )

        val REMOTE_METADATA_REGION: Setting<String?> = Setting.simpleString(
            "$REMOTE_METADATA_KEY_PREFIX.$REMOTE_METADATA_REGION_KEY",
            Setting.Property.NodeScope,
            Setting.Property.Final
        )

        val REMOTE_METADATA_SERVICE_NAME: Setting<String?> = Setting.simpleString(
            "$REMOTE_METADATA_KEY_PREFIX.$REMOTE_METADATA_SERVICE_NAME_KEY",
            Setting.Property.NodeScope,
            Setting.Property.Final
        )

        val MULTI_TENANT_TRIGGER_EVAL_ENABLED = Setting.boolSetting(
            "plugins.alerting.multi_tenant_trigger_eval_enabled",
            false,
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        /**
         * Enables managing monitor schedules on an external EventBridge Scheduler instead of
         * the in-cluster job scheduler. When disabled (default), monitor create/update/delete
         * flows skip the external schedule hooks entirely.
         */
        val EXTERNAL_SCHEDULER_ENABLED = Setting.boolSetting(
            "plugins.alerting.external_scheduler.enabled",
            false,
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        /**
         * Default AWS account that owns the EventBridge schedules. May be overridden per-request
         * by a transient ThreadContext value under [ExternalSchedulerService.SCHEDULER_ACCOUNT_ID_KEY].
         */
        val EXTERNAL_SCHEDULER_ACCOUNT_ID = Setting.simpleString(
            "plugins.alerting.external_scheduler.account_id",
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        /** SQS queue ARN that EventBridge schedules dispatch monitor executions to. */
        val EXTERNAL_SCHEDULER_QUEUE_ARN = Setting.simpleString(
            "plugins.alerting.external_scheduler.queue_arn",
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        /** IAM role ARN that EventBridge assumes to send messages to the target SQS queue. */
        val EXTERNAL_SCHEDULER_ROLE_ARN = Setting.simpleString(
            "plugins.alerting.external_scheduler.role_arn",
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        /** AWS account ID that hosts the job queues available for polling. */
        val JOB_QUEUE_ACCOUNT_ID = Setting.simpleString(
            "plugins.alerting.job_queue_account_id",
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        /** Provider type used to resolve job queue account IDs (e.g. "plugin_setting"). */
        val JOB_QUEUE_ACCOUNT_PROVIDER_TYPE = Setting.simpleString(
            "plugins.alerting.job_queue_account_provider_type",
            "plugin_setting",
            Setting.Property.NodeScope, Setting.Property.Final
        )
    }
}

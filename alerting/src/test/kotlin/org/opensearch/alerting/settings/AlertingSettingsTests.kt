package org.opensearch.alerting.settings

import org.junit.Before
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.core.settings.LegacyOpenDistroScheduledJobSettings
import org.opensearch.alerting.core.settings.ScheduledJobSettings
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.test.OpenSearchTestCase
import java.util.concurrent.TimeUnit

class AlertingSettingsTests : OpenSearchTestCase() {

    private lateinit var plugin: AlertingPlugin

    @Before
    fun setup() {
        plugin = AlertingPlugin()
    }

    fun `test all opendistro settings returned`() {
        val settings = plugin.settings
        assertTrue(
            "Legacy Settings are not returned",
            settings.containsAll(
                listOf<Any>(
                    LegacyOpenDistroAlertingSettings.ALERTING_MAX_MONITORS,
                    LegacyOpenDistroAlertingSettings.INPUT_TIMEOUT,
                    LegacyOpenDistroAlertingSettings.INDEX_TIMEOUT,
                    LegacyOpenDistroAlertingSettings.BULK_TIMEOUT,
                    LegacyOpenDistroAlertingSettings.ALERT_BACKOFF_MILLIS,
                    LegacyOpenDistroAlertingSettings.ALERT_BACKOFF_COUNT,
                    LegacyOpenDistroAlertingSettings.MOVE_ALERTS_BACKOFF_MILLIS,
                    LegacyOpenDistroAlertingSettings.MOVE_ALERTS_BACKOFF_COUNT,
                    LegacyOpenDistroAlertingSettings.ALERT_HISTORY_ENABLED,
                    LegacyOpenDistroAlertingSettings.ALERT_HISTORY_ROLLOVER_PERIOD,
                    LegacyOpenDistroAlertingSettings.ALERT_HISTORY_INDEX_MAX_AGE,
                    LegacyOpenDistroAlertingSettings.ALERT_HISTORY_MAX_DOCS,
                    LegacyOpenDistroAlertingSettings.ALERT_HISTORY_RETENTION_PERIOD,
                    LegacyOpenDistroAlertingSettings.REQUEST_TIMEOUT,
                    LegacyOpenDistroAlertingSettings.MAX_ACTION_THROTTLE_VALUE,
                    LegacyOpenDistroAlertingSettings.FILTER_BY_BACKEND_ROLES,
                    LegacyOpenDistroScheduledJobSettings.SWEEP_PERIOD,
                    LegacyOpenDistroScheduledJobSettings.SWEEP_PAGE_SIZE,
                    LegacyOpenDistroScheduledJobSettings.SWEEP_BACKOFF_RETRY_COUNT,
                    LegacyOpenDistroScheduledJobSettings.SWEEP_BACKOFF_MILLIS,
                    LegacyOpenDistroScheduledJobSettings.SWEEPER_ENABLED,
                    LegacyOpenDistroScheduledJobSettings.REQUEST_TIMEOUT
                )
            )
        )
    }

    fun `test all opensearch settings returned`() {
        val settings = plugin.settings
        assertTrue(
            "Opensearch settings not returned",
            settings.containsAll(
                listOf<Any>(
                    DestinationSettings.ALLOW_LIST,
                    DestinationSettings.HOST_DENY_LIST,
                    AlertingSettings.ALERTING_MAX_MONITORS,
                    AlertingSettings.INPUT_TIMEOUT,
                    AlertingSettings.INDEX_TIMEOUT,
                    AlertingSettings.BULK_TIMEOUT,
                    AlertingSettings.ALERT_BACKOFF_MILLIS,
                    AlertingSettings.ALERT_BACKOFF_COUNT,
                    AlertingSettings.MOVE_ALERTS_BACKOFF_MILLIS,
                    AlertingSettings.MOVE_ALERTS_BACKOFF_COUNT,
                    AlertingSettings.ALERT_HISTORY_ENABLED,
                    AlertingSettings.ALERT_HISTORY_ROLLOVER_PERIOD,
                    AlertingSettings.ALERT_HISTORY_INDEX_MAX_AGE,
                    AlertingSettings.ALERT_HISTORY_MAX_DOCS,
                    AlertingSettings.ALERT_HISTORY_RETENTION_PERIOD,
                    AlertingSettings.REQUEST_TIMEOUT,
                    AlertingSettings.MAX_ACTION_THROTTLE_VALUE,
                    AlertingSettings.FILTER_BY_BACKEND_ROLES,
                    ScheduledJobSettings.SWEEP_PERIOD,
                    ScheduledJobSettings.SWEEP_PAGE_SIZE,
                    ScheduledJobSettings.SWEEP_BACKOFF_RETRY_COUNT,
                    ScheduledJobSettings.SWEEP_BACKOFF_MILLIS,
                    ScheduledJobSettings.SWEEPER_ENABLED,
                    ScheduledJobSettings.REQUEST_TIMEOUT
                )
            )
        )
    }

    fun `test opendistro settings fallback`() {
        assertEquals(
            AlertingSettings.MOVE_ALERTS_BACKOFF_COUNT.get(Settings.EMPTY),
            LegacyOpenDistroAlertingSettings.MOVE_ALERTS_BACKOFF_COUNT.get(Settings.EMPTY)
        )
        assertEquals(
            ScheduledJobSettings.REQUEST_TIMEOUT.get(Settings.EMPTY),
            LegacyOpenDistroScheduledJobSettings.REQUEST_TIMEOUT.get(Settings.EMPTY)
        )
    }

    fun `test settings get Value`() {
        val settings = Settings.builder().put("plugins.alerting.move_alerts_backoff_count", 1).build()
        assertEquals(AlertingSettings.MOVE_ALERTS_BACKOFF_COUNT.get(settings), 1)
        assertEquals(LegacyOpenDistroAlertingSettings.MOVE_ALERTS_BACKOFF_COUNT.get(settings), 3)
        val scheduledJobSettings = Settings.builder().put("plugins.scheduled_jobs.enabled", false).build()
        assertEquals(ScheduledJobSettings.SWEEPER_ENABLED.get(scheduledJobSettings), false)
        assertEquals(LegacyOpenDistroScheduledJobSettings.SWEEPER_ENABLED.get(scheduledJobSettings), true)
    }

    fun `test settings get value with legacy Fallback`() {
        val settings = Settings.builder()
            .put("opendistro.alerting.monitor.max_monitors", 1000)
            .put("opendistro.alerting.input_timeout", TimeValue.timeValueSeconds(30))
            .put("opendistro.alerting.index_timeout", TimeValue.timeValueSeconds(60))
            .put("opendistro.alerting.bulk_timeout", TimeValue.timeValueSeconds(120))
            .put("opendistro.alerting.alert_backoff_millis", TimeValue.timeValueMillis(50))
            .put("opendistro.alerting.alert_backoff_count", 2)
            .put("opendistro.alerting.move_alerts_backoff_millis", TimeValue.timeValueMillis(250))
            .put("opendistro.alerting.move_alerts_backoff_count", 3)
            .put("opendistro.alerting.alert_history_enabled", true)
            .put("opendistro.alerting.alert_history_rollover_period", TimeValue.timeValueHours(12))
            .put("opendistro.alerting.alert_history_max_age", TimeValue(30, TimeUnit.DAYS))
            .put("opendistro.alerting.alert_history_max_docs", 1000L)
            .put("opendistro.alerting.alert_history_retention_period", TimeValue(60, TimeUnit.DAYS))
            .put("opendistro.alerting.request_timeout", TimeValue.timeValueSeconds(10))
            .put("opendistro.alerting.action_throttle_max_value", TimeValue.timeValueHours(24))
            .put("opendistro.alerting.filter_by_backend_roles", false)
            .put("opendistro.scheduled_jobs.enabled", false)
            .put("opendistro.scheduled_jobs.request_timeout", TimeValue.timeValueSeconds(10))
            .put("opendistro.scheduled_jobs.sweeper.backoff_millis", TimeValue.timeValueMillis(50))
            .put("opendistro.scheduled_jobs.retry_count", 3)
            .put("opendistro.scheduled_jobs.sweeper.period", TimeValue.timeValueMinutes(5))
            .put("opendistro.scheduled_jobs.sweeper.page_size", 100).build()

        assertEquals(AlertingSettings.ALERTING_MAX_MONITORS.get(settings), 1000)
        assertEquals(AlertingSettings.INPUT_TIMEOUT.get(settings), TimeValue.timeValueSeconds(30))
        assertEquals(AlertingSettings.INDEX_TIMEOUT.get(settings), TimeValue.timeValueSeconds(60))
        assertEquals(AlertingSettings.BULK_TIMEOUT.get(settings), TimeValue.timeValueSeconds(120))
        assertEquals(AlertingSettings.ALERT_BACKOFF_MILLIS.get(settings), TimeValue.timeValueMillis(50))
        assertEquals(AlertingSettings.ALERT_BACKOFF_COUNT.get(settings), 2)
        assertEquals(AlertingSettings.MOVE_ALERTS_BACKOFF_MILLIS.get(settings), TimeValue.timeValueMillis(250))
        assertEquals(AlertingSettings.MOVE_ALERTS_BACKOFF_COUNT.get(settings), 3)
        assertEquals(AlertingSettings.ALERT_HISTORY_ENABLED.get(settings), true)
        assertEquals(AlertingSettings.ALERT_HISTORY_ROLLOVER_PERIOD.get(settings), TimeValue.timeValueHours(12))
        assertEquals(AlertingSettings.ALERT_HISTORY_INDEX_MAX_AGE.get(settings), TimeValue(30, TimeUnit.DAYS))
        assertEquals(AlertingSettings.ALERT_HISTORY_MAX_DOCS.get(settings), 1000L)
        assertEquals(AlertingSettings.ALERT_HISTORY_RETENTION_PERIOD.get(settings), TimeValue(60, TimeUnit.DAYS))
        assertEquals(AlertingSettings.REQUEST_TIMEOUT.get(settings), TimeValue.timeValueSeconds(10))
        assertEquals(AlertingSettings.MAX_ACTION_THROTTLE_VALUE.get(settings), TimeValue.timeValueHours(24))
        assertEquals(AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings), false)
        assertEquals(ScheduledJobSettings.SWEEPER_ENABLED.get(settings), false)
        assertEquals(ScheduledJobSettings.REQUEST_TIMEOUT.get(settings), TimeValue.timeValueSeconds(10))
        assertEquals(ScheduledJobSettings.SWEEP_BACKOFF_MILLIS.get(settings), TimeValue.timeValueMillis(50))
        assertEquals(ScheduledJobSettings.SWEEP_BACKOFF_RETRY_COUNT.get(settings), 3)
        assertEquals(ScheduledJobSettings.SWEEP_PERIOD.get(settings), TimeValue.timeValueMinutes(5))
        assertEquals(ScheduledJobSettings.SWEEP_PAGE_SIZE.get(settings), 100)

        assertSettingDeprecationsAndWarnings(
            arrayOf(
                LegacyOpenDistroAlertingSettings.ALERTING_MAX_MONITORS,
                LegacyOpenDistroAlertingSettings.INPUT_TIMEOUT,
                LegacyOpenDistroAlertingSettings.INDEX_TIMEOUT,
                LegacyOpenDistroAlertingSettings.BULK_TIMEOUT,
                LegacyOpenDistroAlertingSettings.ALERT_BACKOFF_MILLIS,
                LegacyOpenDistroAlertingSettings.ALERT_BACKOFF_COUNT,
                LegacyOpenDistroAlertingSettings.MOVE_ALERTS_BACKOFF_MILLIS,
                LegacyOpenDistroAlertingSettings.MOVE_ALERTS_BACKOFF_COUNT,
                LegacyOpenDistroAlertingSettings.ALERT_HISTORY_ENABLED,
                LegacyOpenDistroAlertingSettings.ALERT_HISTORY_ROLLOVER_PERIOD,
                LegacyOpenDistroAlertingSettings.ALERT_HISTORY_INDEX_MAX_AGE,
                LegacyOpenDistroAlertingSettings.ALERT_HISTORY_MAX_DOCS,
                LegacyOpenDistroAlertingSettings.ALERT_HISTORY_RETENTION_PERIOD,
                LegacyOpenDistroAlertingSettings.REQUEST_TIMEOUT,
                LegacyOpenDistroAlertingSettings.MAX_ACTION_THROTTLE_VALUE,
                LegacyOpenDistroAlertingSettings.FILTER_BY_BACKEND_ROLES,
                LegacyOpenDistroScheduledJobSettings.SWEEPER_ENABLED,
                LegacyOpenDistroScheduledJobSettings.REQUEST_TIMEOUT,
                LegacyOpenDistroScheduledJobSettings.SWEEP_BACKOFF_MILLIS,
                LegacyOpenDistroScheduledJobSettings.SWEEP_BACKOFF_RETRY_COUNT,
                LegacyOpenDistroScheduledJobSettings.SWEEP_PAGE_SIZE,
                LegacyOpenDistroScheduledJobSettings.SWEEP_PERIOD
            )
        )
    }
}

package org.opensearch.alerting.settings

import org.junit.Before
import org.opensearch.alerting.AlertingPlugin
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
                    LegacyOpenDistroDestinationSettings.ALLOW_LIST,
                    LegacyOpenDistroDestinationSettings.HOST_DENY_LIST,
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
                    LegacyOpenDistroAlertingSettings.FILTER_BY_BACKEND_ROLES
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
                    AlertingSettings.FILTER_BY_BACKEND_ROLES
                )
            )
        )
    }

    fun `test opendistro settings fallback`() {
        assertEquals(
            AlertingSettings.MOVE_ALERTS_BACKOFF_COUNT.get(Settings.EMPTY),
            LegacyOpenDistroAlertingSettings.MOVE_ALERTS_BACKOFF_COUNT.get(Settings.EMPTY)
        )
    }

    fun `test settings get Value`() {
        val settings = Settings.builder().put("plugins.alerting.move_alerts_backoff_count", 1).build()
        assertEquals(AlertingSettings.MOVE_ALERTS_BACKOFF_COUNT.get(settings), 1)
        assertEquals(LegacyOpenDistroAlertingSettings.MOVE_ALERTS_BACKOFF_COUNT.get(settings), 3)
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
            .put("opendistro.alerting.filter_by_backend_roles", false).build()

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
                LegacyOpenDistroAlertingSettings.FILTER_BY_BACKEND_ROLES
            )
        )
    }
}

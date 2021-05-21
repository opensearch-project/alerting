package org.opensearch.alerting.core.settings

import org.opensearch.common.settings.Setting
import org.opensearch.common.unit.TimeValue

/**
 * Legacy Opendistro settings used for [ScheduledJob]'s. These include back off settings, retry counts, timeouts etc...
 */

class LegacyOpenDistroScheduledJobSettings {

    companion object {
        val SWEEPER_ENABLED = Setting.boolSetting(
            "opendistro.scheduled_jobs.enabled",
            true,
            Setting.Property.NodeScope, Setting.Property.Dynamic, Setting.Property.Deprecated
        )

        val REQUEST_TIMEOUT = Setting.positiveTimeSetting(
            "opendistro.scheduled_jobs.request_timeout",
            TimeValue.timeValueSeconds(10),
            Setting.Property.NodeScope, Setting.Property.Dynamic, Setting.Property.Deprecated
        )

        val SWEEP_BACKOFF_MILLIS = Setting.positiveTimeSetting(
            "opendistro.scheduled_jobs.sweeper.backoff_millis",
            TimeValue.timeValueMillis(50),
            Setting.Property.NodeScope, Setting.Property.Dynamic, Setting.Property.Deprecated
        )

        val SWEEP_BACKOFF_RETRY_COUNT = Setting.intSetting(
            "opendistro.scheduled_jobs.retry_count",
            3,
            Setting.Property.NodeScope, Setting.Property.Dynamic, Setting.Property.Deprecated
        )

        val SWEEP_PERIOD = Setting.positiveTimeSetting(
            "opendistro.scheduled_jobs.sweeper.period",
            TimeValue.timeValueMinutes(5),
            Setting.Property.NodeScope, Setting.Property.Dynamic, Setting.Property.Deprecated
        )

        val SWEEP_PAGE_SIZE = Setting.intSetting(
            "opendistro.scheduled_jobs.sweeper.page_size",
            100,
            Setting.Property.NodeScope, Setting.Property.Dynamic, Setting.Property.Deprecated
        )
    }
}

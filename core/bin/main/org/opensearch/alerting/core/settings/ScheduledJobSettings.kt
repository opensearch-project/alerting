/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.core.settings

import org.opensearch.alerting.core.model.ScheduledJob
import org.opensearch.common.settings.Setting

/**
 * settings used for [ScheduledJob]'s. These include back off settings, retry counts, timeouts etc...
 */
class ScheduledJobSettings {

    companion object {
        val SWEEPER_ENABLED = Setting.boolSetting(
            "plugins.scheduled_jobs.enabled",
            LegacyOpenDistroScheduledJobSettings.SWEEPER_ENABLED,
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )
        val REQUEST_TIMEOUT = Setting.positiveTimeSetting(
            "plugins.scheduled_jobs.request_timeout",
            LegacyOpenDistroScheduledJobSettings.REQUEST_TIMEOUT,
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        val SWEEP_BACKOFF_MILLIS = Setting.positiveTimeSetting(
            "plugins.scheduled_jobs.sweeper.backoff_millis",
            LegacyOpenDistroScheduledJobSettings.SWEEP_BACKOFF_MILLIS,
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        val SWEEP_BACKOFF_RETRY_COUNT = Setting.intSetting(
            "plugins.scheduled_jobs.retry_count",
            LegacyOpenDistroScheduledJobSettings.SWEEP_BACKOFF_RETRY_COUNT,
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        val SWEEP_PERIOD = Setting.positiveTimeSetting(
            "plugins.scheduled_jobs.sweeper.period",
            LegacyOpenDistroScheduledJobSettings.SWEEP_PERIOD,
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )

        val SWEEP_PAGE_SIZE = Setting.intSetting(
            "plugins.scheduled_jobs.sweeper.page_size",
            LegacyOpenDistroScheduledJobSettings.SWEEP_PAGE_SIZE,
            Setting.Property.NodeScope, Setting.Property.Dynamic
        )
    }
}

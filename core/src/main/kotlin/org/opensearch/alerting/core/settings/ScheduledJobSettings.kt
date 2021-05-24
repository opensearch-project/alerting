/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
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

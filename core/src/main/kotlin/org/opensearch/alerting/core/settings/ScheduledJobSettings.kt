/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.core.settings

import org.opensearch.common.settings.Setting
import org.opensearch.node.remotestore.RemoteStoreNodeAttribute

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

        /**
         * When enabled, the full sweep deschedules in-memory jobs whose backing document no longer exists in the
         * scheduled jobs index, confirmed with a realtime GET against the primary shard.
         *
         * This repairs jobs orphaned because the delete of their document was never observed on this node. On a
         * remote-store enabled scheduled jobs index the replica copy does not execute delete operations, so
         * [JobSweeper.postDelete] never fires on a node that owns a job but holds the replica copy, and the job keeps
         * running from memory with no document behind it. The sweep only ever visited documents that still exist, so
         * it could not repair this.
         *
         * Unless set explicitly, the default follows the node's remote store configuration: enabled on nodes carrying
         * a remote segment or translog repository attribute, where the listener gap exists, and disabled elsewhere.
         * An explicit value, including a dynamic cluster settings update, always takes precedence.
         */
        val SWEEP_ORPHAN_RECONCILIATION_ENABLED = Setting.boolSetting(
            "plugins.scheduled_jobs.sweeper.orphan_reconciliation.enabled",
            { settings -> RemoteStoreNodeAttribute.isRemoteDataAttributePresent(settings).toString() },
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

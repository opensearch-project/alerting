/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.alerts

import org.opensearch.Version
import org.opensearch.alerting.alerts.AlertIndices.Companion.ALERT_HISTORY_WRITE_INDEX
import org.opensearch.alerting.alerts.AlertIndices.Companion.FINDING_HISTORY_WRITE_INDEX
import org.opensearch.cluster.metadata.AliasMetadata
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.service.ClusterManagerThrottlingException
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.snapshots.SnapshotInProgressException
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.transport.RemoteTransportException
import java.util.concurrent.TimeUnit

class AlertIndicesHistoryRetentionTests : OpenSearchTestCase() {

    private val now = System.currentTimeMillis()
    private val retention30d = TimeUnit.DAYS.toMillis(30)

    fun `test rolled over index older than retention is selected`() {
        val rolledOver = index(".opendistro-alerting-alert-history-2026.01.01-000001", 31, null)

        assertEquals(
            listOf(rolledOver.index.name),
            AlertIndices.getIndicesToDelete(listOf(rolledOver), now, retention30d, ALERT_HISTORY_WRITE_INDEX, true)
        )
    }

    fun `test index younger than retention is kept`() {
        val young = index(".opendistro-alerting-alert-history-2026.01.01-000002", 29, null)

        assertTrue(AlertIndices.getIndicesToDelete(listOf(young), now, retention30d, ALERT_HISTORY_WRITE_INDEX, true).isEmpty())
    }

    fun `test current write index is kept when history is enabled`() {
        val writeIndex = index(".opendistro-alerting-alert-history-2026.01.01-000003", 45, ALERT_HISTORY_WRITE_INDEX)

        assertTrue(AlertIndices.getIndicesToDelete(listOf(writeIndex), now, retention30d, ALERT_HISTORY_WRITE_INDEX, true).isEmpty())
    }

    fun `test current write index is selected when history is disabled`() {
        val writeIndex = index(".opendistro-alerting-alert-history-2026.01.01-000003", 45, ALERT_HISTORY_WRITE_INDEX)

        assertEquals(
            listOf(writeIndex.index.name),
            AlertIndices.getIndicesToDelete(listOf(writeIndex), now, retention30d, ALERT_HISTORY_WRITE_INDEX, false)
        )
    }

    /**
     * Each history type is evaluated against its own write alias only: the alert history write index must not be
     * selected when the findings retention is applied, even if it is older than the findings retention.
     */
    fun `test alert history write index is not selected by another history type`() {
        val alertWriteIndex = index(".opendistro-alerting-alert-history-2026.01.01-000004", 61, ALERT_HISTORY_WRITE_INDEX)
        val findingsRetention60d = TimeUnit.DAYS.toMillis(60)

        assertTrue(
            AlertIndices.getIndicesToDelete(listOf(alertWriteIndex), now, findingsRetention60d, ALERT_HISTORY_WRITE_INDEX, true).isEmpty()
        )
    }

    fun `test only expired non write indices are selected`() {
        val expired = index(".opensearch-alerting-finding-history-2026.01.01-000001", 40, null)
        val expiredWriteIndex = index(".opensearch-alerting-finding-history-2026.01.01-000002", 35, FINDING_HISTORY_WRITE_INDEX)
        val young = index(".opensearch-alerting-finding-history-2026.01.01-000003", 5, null)

        assertEquals(
            listOf(expired.index.name),
            AlertIndices.getIndicesToDelete(listOf(expired, expiredWriteIndex, young), now, retention30d, FINDING_HISTORY_WRITE_INDEX, true)
        )
    }

    fun `test snapshot and throttling failures are retryable`() {
        assertTrue(AlertIndices.isRetryableHistoryDeleteFailure(SnapshotInProgressException("Cannot delete indices that are being snapshotted")))
        assertTrue(
            AlertIndices.isRetryableHistoryDeleteFailure(
                RemoteTransportException("wrapped", SnapshotInProgressException("being snapshotted"))
            )
        )
        assertTrue(AlertIndices.isRetryableHistoryDeleteFailure(ClusterManagerThrottlingException("throttled")))
    }

    fun `test other failures are not retryable`() {
        assertFalse(AlertIndices.isRetryableHistoryDeleteFailure(IllegalArgumentException("bad request")))
    }

    fun `test retry delay backs off exponentially`() {
        assertEquals(3, AlertIndices.MAX_HISTORY_DELETE_RETRIES)
        assertEquals(TimeValue.timeValueMinutes(5), AlertIndices.historyDeleteRetryDelay(0))
        assertEquals(TimeValue.timeValueMinutes(10), AlertIndices.historyDeleteRetryDelay(1))
        assertEquals(TimeValue.timeValueMinutes(20), AlertIndices.historyDeleteRetryDelay(2))
    }

    private fun index(name: String, ageDays: Int, alias: String?): IndexMetadata {
        val builder = IndexMetadata.builder(name)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .creationDate(now - TimeUnit.DAYS.toMillis(ageDays.toLong()))
            .numberOfShards(1)
            .numberOfReplicas(0)
        if (alias != null) {
            builder.putAlias(AliasMetadata.builder(alias).build())
        }
        return builder.build()
    }
}

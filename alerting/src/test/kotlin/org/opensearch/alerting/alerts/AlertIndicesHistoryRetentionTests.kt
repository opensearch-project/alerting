/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.alerts

import org.mockito.Mockito
import org.mockito.Mockito.any
import org.mockito.Mockito.anyString
import org.mockito.Mockito.doAnswer
import org.opensearch.Version
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.alerting.alertingPluginSettings
import org.opensearch.alerting.alerts.AlertIndices.Companion.ALERT_HISTORY_WRITE_INDEX
import org.opensearch.alerting.alerts.AlertIndices.Companion.FINDING_HISTORY_WRITE_INDEX
import org.opensearch.cluster.metadata.AliasMetadata
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.node.DiscoveryNode
import org.opensearch.cluster.service.ClusterManagerThrottlingException
import org.opensearch.common.settings.ClusterSettings
import org.opensearch.common.settings.Setting
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.core.action.ActionListener
import org.opensearch.index.IndexNotFoundException
import org.opensearch.snapshots.SnapshotInProgressException
import org.opensearch.test.ClusterServiceUtils
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.RemoteTransportException
import org.opensearch.transport.client.AdminClient
import org.opensearch.transport.client.Client
import org.opensearch.transport.client.IndicesAdminClient
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
        assertTrue(AlertIndices.isRetryableHistoryDeleteFailure(SnapshotInProgressException("being snapshotted")))
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

    fun `test retry path sends one index per delete request`() {
        val harness = DeleteHarness()

        harness.alertIndices.deleteOldHistoryIndex(listOf("history-1", "history-2"))

        assertEquals(listOf(listOf("history-1"), listOf("history-2")), harness.requests.map { it.indices().toList() })
    }

    fun `test snapshot failure is retried with exponential backoff until retries are exhausted`() {
        val harness = DeleteHarness()
        harness.alertIndices.deleteOldHistoryIndex(listOf("history-1"))

        // The initial attempt and every retry are blocked by a snapshot; each scheduled retry is run immediately
        repeat(AlertIndices.MAX_HISTORY_DELETE_RETRIES + 1) { attempt ->
            harness.listeners[attempt].onFailure(SnapshotInProgressException("Cannot delete indices that are being snapshotted"))
            harness.retries.getOrNull(attempt)?.run()
        }

        assertEquals(
            List(AlertIndices.MAX_HISTORY_DELETE_RETRIES + 1) { listOf("history-1") },
            harness.requests.map { it.indices().toList() }
        )
        // The failure after the last retry is not rescheduled
        assertEquals(
            listOf(TimeValue.timeValueMinutes(5), TimeValue.timeValueMinutes(10), TimeValue.timeValueMinutes(20)),
            harness.retryDelays
        )
    }

    fun `test non retryable failure is not retried`() {
        val harness = DeleteHarness()
        harness.alertIndices.deleteOldHistoryIndex(listOf("history-1"))

        harness.listeners.single().onFailure(IllegalArgumentException("bad request"))

        assertTrue(harness.retries.isEmpty())
    }

    fun `test index already deleted is not retried`() {
        val harness = DeleteHarness()
        harness.alertIndices.deleteOldHistoryIndex(listOf("history-1"))

        harness.listeners.single().onFailure(IndexNotFoundException("history-1"))

        assertTrue(harness.retries.isEmpty())
    }

    /** An [AlertIndices] whose delete requests and scheduled retries are recorded instead of executed. */
    private inner class DeleteHarness {
        val requests = mutableListOf<DeleteIndexRequest>()
        val listeners = mutableListOf<ActionListener<AcknowledgedResponse>>()
        val retries = mutableListOf<Runnable>()
        val retryDelays = mutableListOf<TimeValue>()
        val alertIndices: AlertIndices

        init {
            val indicesClient = Mockito.mock(IndicesAdminClient::class.java)
            val adminClient = Mockito.mock(AdminClient::class.java)
            val client = Mockito.mock(Client::class.java)
            Mockito.`when`(client.admin()).thenReturn(adminClient)
            Mockito.`when`(adminClient.indices()).thenReturn(indicesClient)
            doAnswer {
                requests.add(it.getArgument(0))
                listeners.add(it.getArgument(1))
                null
            }.`when`(indicesClient).delete(any(DeleteIndexRequest::class.java), any())

            val threadPool = Mockito.mock(ThreadPool::class.java)
            doAnswer {
                assertEquals(ThreadPool.Names.MANAGEMENT, it.getArgument<String>(2))
                retries.add(it.getArgument(0))
                retryDelays.add(it.getArgument(1))
                null
            }.`when`(threadPool).schedule(any(Runnable::class.java), any(TimeValue::class.java), anyString())

            val settingSet = hashSetOf<Setting<*>>()
            settingSet.addAll(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
            settingSet.addAll(alertingPluginSettings())
            val clusterService = ClusterServiceUtils.createClusterService(
                threadPool,
                DiscoveryNode("node", buildNewFakeTransportAddress(), Version.CURRENT),
                ClusterSettings(Settings.EMPTY, settingSet)
            )
            alertIndices = AlertIndices(Settings.EMPTY, client, threadPool, clusterService)
        }
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

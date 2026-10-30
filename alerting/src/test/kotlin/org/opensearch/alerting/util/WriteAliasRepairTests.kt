/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import kotlinx.coroutines.runBlocking
import org.apache.logging.log4j.Level
import org.apache.logging.log4j.LogManager
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.doAnswer
import org.mockito.Mockito.mock
import org.mockito.Mockito.never
import org.mockito.Mockito.verify
import org.opensearch.Version
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.alerting.alerts.AlertIndices.Companion.ALERT_HISTORY_WRITE_INDEX
import org.opensearch.alerting.util.WriteAliasRepair.AliasedIndex
import org.opensearch.cluster.ClusterName
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.AliasMetadata
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.core.action.ActionListener
import org.opensearch.core.index.Index
import org.opensearch.snapshots.RestoreContext
import org.opensearch.snapshots.Snapshot
import org.opensearch.snapshots.SnapshotId
import org.opensearch.test.MockLogAppender
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.transport.client.AdminClient
import org.opensearch.transport.client.Client
import org.opensearch.transport.client.IndicesAdminClient
import org.mockito.Mockito.`when` as whenever

class WriteAliasRepairTests : OpenSearchTestCase() {

    private val own = AliasedIndex("h-2026.10.07-1", 200, null)
    private val restored = AliasedIndex("h-2026.10.07-000002", 100, null)

    fun `test alias on one index or none is left alone`() {
        assertEquals(emptyList<String>(), WriteAliasRepair.indicesToDetach(listOf(own)))
        assertEquals(emptyList<String>(), WriteAliasRepair.indicesToDetach(emptyList()))
    }

    fun `test alias with an index whose is_write_index is true is left alone`() {
        assertEquals(emptyList<String>(), WriteAliasRepair.indicesToDetach(listOf(own.copy(isWriteIndex = true), restored)))
        assertEquals(emptyList<String>(), WriteAliasRepair.indicesToDetach(listOf(own, restored.copy(isWriteIndex = true))))
    }

    fun `test an index with is_write_index false never keeps the alias`() {
        // The restored index has the higher counter, but it is marked as not to be written to, so the other index keeps the alias
        assertEquals(listOf(restored.index), WriteAliasRepair.indicesToDetach(listOf(own, restored.copy(isWriteIndex = false))))
        assertEquals(listOf(own.index), WriteAliasRepair.indicesToDetach(listOf(own.copy(isWriteIndex = false), restored)))
    }

    fun `test alias whose indices all have is_write_index false is left alone and reported`() {
        val bothFalse = listOf(own.copy(isWriteIndex = false), restored.copy(isWriteIndex = false))

        assertEquals(emptyList<String>(), WriteAliasRepair.indicesToDetach(bothFalse))
        assertTrue(WriteAliasRepair.allMarkedNotWrite(bothFalse))
        assertFalse(WriteAliasRepair.allMarkedNotWrite(listOf(own, restored.copy(isWriteIndex = false))))
    }

    fun `test highest rollover counter keeps the alias even when it is older`() {
        assertEquals(listOf(own.index), WriteAliasRepair.indicesToDetach(listOf(own, restored)))
        assertEquals(listOf(own.index), WriteAliasRepair.indicesToDetach(listOf(restored, own)))
    }

    fun `test equal counters go to the newest creation date`() {
        val older = AliasedIndex("h-2026.10.06-000002", 100, null)
        val newer = AliasedIndex("h-2026.10.07-000002", 200, null)

        assertEquals(listOf(older.index), WriteAliasRepair.indicesToDetach(listOf(older, newer)))
        assertEquals(listOf(older.index), WriteAliasRepair.indicesToDetach(listOf(newer, older)))
    }

    fun `test equal counters and creation dates go to the name that sorts last`() {
        val a = AliasedIndex("h-a-000002", 100, null)
        val b = AliasedIndex("h-b-000002", 100, null)

        assertEquals(listOf(a.index), WriteAliasRepair.indicesToDetach(listOf(a, b)))
        assertEquals(listOf(a.index), WriteAliasRepair.indicesToDetach(listOf(b, a)))
    }

    fun `test a name without a numeric suffix loses to any counter`() {
        val noCounter = AliasedIndex("h-custom", 300, null)

        assertEquals(listOf(noCounter.index), WriteAliasRepair.indicesToDetach(listOf(noCounter, own)))
    }

    fun `test every index but the kept one is detached`() {
        val third = AliasedIndex("h-2026.10.06-000003", 50, null)

        assertEquals(listOf(restored.index, own.index), WriteAliasRepair.indicesToDetach(listOf(own, restored, third)))
    }

    fun `test restore repair removes the alias from the extra index and logs the repair`() {
        val fake = FakeCluster(acknowledged = true, before = listOf(own, restored))

        fake.expectLog(Level.INFO, "Repaired write alias*It now names only [${restored.index}]*") {
            runBlocking { fake.repair.repair(restoreOf(restored.index)) }
        }

        assertRemoveOnly(fake.sentRequest(), own.index)
    }

    fun `test restore repair sends nothing when the restore did not bring back one of the alias's indices`() {
        val fake = FakeCluster(acknowledged = true, before = listOf(own, restored))

        runBlocking { fake.repair.repair(restoreOf("some-other-index")) }

        verify(fake.requests, never()).aliases(any(), any())
    }

    fun `test alias repair without a restore sends the request`() {
        val fake = FakeCluster(acknowledged = true, before = listOf(own, restored))

        runBlocking { fake.repair.repair(ALERT_HISTORY_WRITE_INDEX) }

        assertRemoveOnly(fake.sentRequest(), own.index)
    }

    fun `test not acknowledged request is logged as repaired when the alias now names one index`() {
        val fake = FakeCluster(acknowledged = false, before = listOf(own, restored), after = listOf(restored))

        fake.expectLog(Level.INFO, "Repaired write alias*It now names only [${restored.index}]*") {
            runBlocking { fake.repair.repair(ALERT_HISTORY_WRITE_INDEX) }
        }
    }

    fun `test not acknowledged request is logged as a warning when the alias still names two indices`() {
        val fake = FakeCluster(acknowledged = false, before = listOf(own, restored), after = listOf(own, restored))

        fake.expectLog(Level.WARN, "Could not confirm the repair of write alias*") {
            runBlocking { fake.repair.repair(ALERT_HISTORY_WRITE_INDEX) }
        }
    }

    fun `test alias on one index sends nothing, even when that index has is_write_index false`() {
        listOf(own, own.copy(isWriteIndex = false)).forEach { only ->
            val fake = FakeCluster(acknowledged = true, before = listOf(only))

            runBlocking { fake.repair.repair(ALERT_HISTORY_WRITE_INDEX) }

            verify(fake.requests, never()).aliases(any(), any())
        }
    }

    fun `test alias repair throws for an alias it does not cover and sends nothing`() {
        val fake = FakeCluster(acknowledged = true, before = listOf(own, restored))

        expectThrows(IllegalArgumentException::class.java) { runBlocking { fake.repair.repair("some-other-alias") } }

        verify(fake.requests, never()).aliases(any(), any())
    }

    fun `test alias whose indices all have is_write_index false sends nothing and logs an error`() {
        val fake = FakeCluster(acknowledged = true, before = listOf(own.copy(isWriteIndex = false), restored.copy(isWriteIndex = false)))

        fake.expectLog(Level.ERROR, "Write alias*each has is_write_index set to false*") {
            runBlocking { fake.repair.repair(ALERT_HISTORY_WRITE_INDEX) }
        }

        verify(fake.requests, never()).aliases(any(), any())
    }

    fun `test a failed alias request is logged, not thrown`() {
        val fake = FakeCluster(acknowledged = true, before = listOf(own, restored), failure = IllegalStateException("refused"))

        fake.expectLog(Level.ERROR, "Failed to repair write alias [$ALERT_HISTORY_WRITE_INDEX]") {
            runBlocking { fake.repair.repair(ALERT_HISTORY_WRITE_INDEX) }
        }
    }

    private fun restoreOf(index: String) =
        RestoreContext(Snapshot("repo", SnapshotId("snap", "snap-uuid")), "restore-uuid", listOf(Index(index, "uuid")), false)

    /**
     * A cluster whose alert history alias names [before] until the alias request is sent and [after] from then on. The alias request
     * answers [acknowledged], or fails with [failure] when it is set.
     */
    private class FakeCluster(
        acknowledged: Boolean,
        before: List<AliasedIndex>,
        after: List<AliasedIndex> = before,
        failure: Exception? = null
    ) {
        val requests: IndicesAdminClient = mock(IndicesAdminClient::class.java)
        val repair: WriteAliasRepair
        private var state = stateWithAlias(before)

        init {
            doAnswer {
                @Suppress("UNCHECKED_CAST")
                val listener = it.arguments[1] as ActionListener<AcknowledgedResponse>
                state = stateWithAlias(after)
                if (failure != null) listener.onFailure(failure) else listener.onResponse(AcknowledgedResponse(acknowledged))
            }.`when`(requests).aliases(any(), any())
            val admin = mock(AdminClient::class.java)
            whenever(admin.indices()).thenReturn(requests)
            val client = mock(Client::class.java)
            whenever(client.admin()).thenReturn(admin)
            val clusterService = mock(ClusterService::class.java)
            whenever(clusterService.state()).thenAnswer { state }
            repair = WriteAliasRepair(client, clusterService)
        }

        fun sentRequest(): IndicesAliasesRequest {
            val captor: ArgumentCaptor<IndicesAliasesRequest> = ArgumentCaptor.forClass(IndicesAliasesRequest::class.java)
            verify(requests).aliases(captor.capture(), any())
            return captor.value
        }

        /** Runs [block] and checks that it logs [message] (a simple wildcard pattern) at [level]. */
        fun expectLog(level: Level, message: String, block: () -> Unit) {
            MockLogAppender.createForLoggers(LogManager.getLogger(WriteAliasRepair::class.java)).use { appender ->
                val logger = WriteAliasRepair::class.java.name
                appender.addExpectation(MockLogAppender.SeenEventExpectation("repair log", logger, level, message))
                block()
                appender.assertAllExpectationsMatched()
            }
        }

        private fun stateWithAlias(indices: List<AliasedIndex>): ClusterState {
            val metadata = Metadata.builder()
            indices.forEach {
                metadata.put(
                    IndexMetadata.builder(it.index)
                        .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                        .numberOfShards(1)
                        .numberOfReplicas(0)
                        .creationDate(it.creationDate)
                        .putAlias(AliasMetadata.builder(ALERT_HISTORY_WRITE_INDEX).writeIndex(it.isWriteIndex))
                        .build(),
                    false
                )
            }
            return ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build()
        }
    }

    private fun assertRemoveOnly(request: IndicesAliasesRequest, index: String) {
        val action = request.aliasActions.single()
        assertEquals(AliasActions.Type.REMOVE, action.actionType())
        assertArrayEquals(arrayOf(index), action.indices())
        assertArrayEquals(arrayOf(ALERT_HISTORY_WRITE_INDEX), action.aliases())
    }
}

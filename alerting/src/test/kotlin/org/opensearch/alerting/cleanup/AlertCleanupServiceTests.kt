/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.cleanup

import kotlinx.coroutines.runBlocking
import org.apache.lucene.search.TotalHits
import org.junit.Before
import org.mockito.Mockito
import org.mockito.Mockito.never
import org.mockito.Mockito.verify
import org.opensearch.OpenSearchStatusException
import org.opensearch.Version
import org.opensearch.action.DocWriteRequest
import org.opensearch.action.DocWriteResponse
import org.opensearch.action.bulk.BackoffPolicy
import org.opensearch.action.bulk.BulkItemResponse
import org.opensearch.action.bulk.BulkRequest
import org.opensearch.action.bulk.BulkResponse
import org.opensearch.action.delete.DeleteRequest
import org.opensearch.action.delete.DeleteResponse
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.index.IndexResponse
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.update.UpdateRequest
import org.opensearch.action.update.UpdateResponse
import org.opensearch.alerting.alerts.AlertMover
import org.opensearch.alerting.core.lock.LockModel
import org.opensearch.alerting.core.lock.LockService
import org.opensearch.alerting.randomAlert
import org.opensearch.alerting.randomQueryLevelMonitor
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.node.DiscoveryNode
import org.opensearch.cluster.node.DiscoveryNodeRole
import org.opensearch.cluster.node.DiscoveryNodes
import org.opensearch.cluster.routing.IndexRoutingTable
import org.opensearch.cluster.routing.RoutingTable
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.core.action.ActionListener
import org.opensearch.core.common.bytes.BytesReference
import org.opensearch.core.index.Index
import org.opensearch.core.index.shard.ShardId
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.index.engine.VersionConflictEngineException
import org.opensearch.search.SearchHit
import org.opensearch.search.SearchHits
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.transport.client.Client
import java.time.Instant
import org.mockito.Mockito.`when` as whenever

/**
 * Unit coverage for the cleanup task lifecycle: what gets recorded, what a work unit does with the lock it won, and what
 * happens when the task turns out not to apply or another node has already moved its cursor on.
 *
 * The lock is a real [LockService] driven through the mocked client rather than a test double, because the properties
 * being asserted -- that a node which loses the race does nothing, and that a work unit hands the lock back -- are
 * properties of that service's documents, not of this service's calls into it.
 */
class AlertCleanupServiceTests : OpenSearchTestCase() {

    private lateinit var client: Client
    private lateinit var clusterService: ClusterService
    private lateinit var lockService: LockService
    private lateinit var monitor: Monitor

    private val searchRequests = mutableListOf<SearchRequest>()
    private val getRequests = mutableListOf<GetRequest>()
    private val indexRequests = mutableListOf<IndexRequest>()
    private val updateRequests = mutableListOf<UpdateRequest>()
    private val deleteRequests = mutableListOf<DeleteRequest>()
    private val searchResponses = ArrayDeque<SearchResponse>()
    private val bulkResponses = ArrayDeque<BulkResponse>()

    /**
     * Stands in for the indices this service reads, keyed by index name, and written through by the index and delete
     * stubs so that a second work unit sees what the first one persisted.
     */
    private val documents = mutableMapOf<String, String>()

    /** Fails writes to the cleanup task index when set, standing in for a cursor another node has already advanced. */
    private var cursorWriteFailure: Exception? = null

    /** The lock index's routing, or null while it does not exist. Set to stand in for shards that are still recovering. */
    private var lockIndexRouting: IndexRoutingTable? = null

    /** Served in place of the next reads of the lock index, one per entry, for a lock index that cannot yet be read. */
    private val lockReadFailures = ArrayDeque<Exception>()

    /** Cursor writes only; the lock's own documents go through the same client. */
    private val cursorWrites get() = indexRequests.filter { it.index() == AlertCleanupService.CLEANUP_TASK_INDEX }

    /** One per attempt to acquire the cleanup lock, won or lost. */
    private val lockAttempts get() = getRequests.filter { it.index() == LockService.LOCK_INDEX_NAME }.map { it.id() }

    /** One per lock handed back; a release is an update that sets `released`. */
    private val lockReleases get() = updateRequests.filter { it.index() == LockService.LOCK_INDEX_NAME }

    @Before
    fun setup() {
        client = Mockito.mock(Client::class.java)
        clusterService = Mockito.mock(ClusterService::class.java)
        monitor = randomQueryLevelMonitor().copy(id = JOB_ID)

        searchRequests.clear()
        getRequests.clear()
        indexRequests.clear()
        updateRequests.clear()
        deleteRequests.clear()
        searchResponses.clear()
        bulkResponses.clear()
        documents.clear()
        cursorWriteFailure = null
        lockIndexRouting = null
        lockReadFailures.clear()

        val routingTable = Mockito.mock(RoutingTable::class.java)
        whenever(routingTable.hasIndex(Mockito.anyString())).thenReturn(true)
        // Null means the lock index has not been created yet, which LockService handles itself.
        whenever(routingTable.index(Mockito.anyString())).thenAnswer { lockIndexRouting }
        val clusterState = Mockito.mock(ClusterState::class.java)
        whenever(clusterState.routingTable()).thenReturn(routingTable)
        whenever(clusterState.nodes).thenReturn(nodes())
        whenever(clusterService.state()).thenReturn(clusterState)
        whenever(clusterService.localNode()).thenReturn(dataNode)

        whenever(client.search(Mockito.any(SearchRequest::class.java), Mockito.any())).thenAnswer { invocation ->
            searchRequests.add(invocation.arguments[0] as SearchRequest)
            @Suppress("UNCHECKED_CAST")
            val listener = invocation.arguments[1] as ActionListener<SearchResponse>
            if (searchResponses.isEmpty()) {
                listener.onFailure(OpenSearchStatusException("no page queued", RestStatus.INTERNAL_SERVER_ERROR))
            } else {
                listener.onResponse(searchResponses.removeFirst())
            }
        }
        whenever(client.bulk(Mockito.any(BulkRequest::class.java), Mockito.any())).thenAnswer { invocation ->
            @Suppress("UNCHECKED_CAST")
            (invocation.arguments[1] as ActionListener<BulkResponse>).onResponse(bulkResponses.removeFirst())
        }
        whenever(client.get(Mockito.any(GetRequest::class.java), Mockito.any())).thenAnswer { invocation ->
            val request = invocation.arguments[0] as GetRequest
            getRequests.add(request)
            val index = request.index() ?: ""
            @Suppress("UNCHECKED_CAST")
            val listener = invocation.arguments[1] as ActionListener<GetResponse>
            if (index == LockService.LOCK_INDEX_NAME && lockReadFailures.isNotEmpty()) {
                listener.onFailure(lockReadFailures.removeFirst())
            } else {
                listener.onResponse(getResponse(documents[index]))
            }
        }
        whenever(client.index(Mockito.any(IndexRequest::class.java), Mockito.any())).thenAnswer { invocation ->
            val request = invocation.arguments[0] as IndexRequest
            indexRequests.add(request)
            @Suppress("UNCHECKED_CAST")
            val listener = invocation.arguments[1] as ActionListener<IndexResponse>
            val failure = cursorWriteFailure
            if (failure != null && request.index() == AlertCleanupService.CLEANUP_TASK_INDEX) {
                listener.onFailure(failure)
            } else {
                val index: String = request.index()
                documents[index] = request.source().utf8ToString()
                listener.onResponse(IndexResponse(SHARD_ID, request.id(), SEQ_NO, PRIMARY_TERM, 1L, true))
            }
        }
        whenever(client.update(Mockito.any(UpdateRequest::class.java), Mockito.any())).thenAnswer { invocation ->
            val request = invocation.arguments[0] as UpdateRequest
            updateRequests.add(request)
            @Suppress("UNCHECKED_CAST")
            val listener = invocation.arguments[1] as ActionListener<UpdateResponse>
            listener.onResponse(UpdateResponse(SHARD_ID, request.id(), SEQ_NO, PRIMARY_TERM, 2L, DocWriteResponse.Result.UPDATED))
        }
        whenever(client.delete(Mockito.any(DeleteRequest::class.java), Mockito.any())).thenAnswer { invocation ->
            val request = invocation.arguments[0] as DeleteRequest
            deleteRequests.add(request)
            val index: String = request.index()
            documents.remove(index)
            @Suppress("UNCHECKED_CAST")
            val listener = invocation.arguments[1] as ActionListener<DeleteResponse>
            listener.onResponse(DeleteResponse(SHARD_ID, request.id(), SEQ_NO, PRIMARY_TERM, 2L, true))
        }

        lockService = LockService(client, clusterService)
        AlertCleanupService.initialize(client, clusterService, lockService, NamedXContentRegistry.EMPTY)
        // The service is a singleton, so its settings-driven state has to be reset for each test.
        AlertCleanupService.multiTenancyEnabled = false
        // A retried work unit would otherwise sleep for real between attempts.
        AlertCleanupService.retryPolicy = BackoffPolicy.noBackoff()
    }

    fun `test nothing is recorded for a monitor with no alerts to move`() {
        // Most deletes have no alerts to move at all; recording a task for each would churn the task index, and the lock
        // index behind it, with documents created only to be drained empty and removed again.
        searchResponses.add(countResponse(0L))

        val task = runBlocking {
            AlertCleanupService.recordMonitorCleanupTask(monitor, survivingTriggerIds = emptyList(), jobDeleted = true)
        }

        assertNull(task)
        assertTrue(cursorWrites.isEmpty())
    }

    fun `test a recorded task carries the monitor's own alert indices and the reason for the cleanup`() {
        searchResponses.add(countResponse(250L))

        val task = runBlocking {
            AlertCleanupService.recordMonitorCleanupTask(monitor, survivingTriggerIds = listOf("kept"), jobDeleted = false)
        }

        assertNotNull(task)
        val request = cursorWrites.single()
        assertEquals(AlertCleanupTask.taskId(monitor.id, CleanupScope.MONITOR), request.id())
        val source = request.source().utf8ToString()
        // The post-delete hook is handed an id and nothing else, so a monitor with custom alert indices can only be
        // cleaned up if the indices are captured here, while the monitor object is still in hand.
        assertTrue(source, source.contains("\"alert_index\":\"${monitor.dataSources.alertsIndex}\""))
        assertTrue(source, source.contains("\"job_deleted\":false"))
        assertTrue(source, source.contains("\"surviving_trigger_ids\":[\"kept\"]"))
    }

    fun `test nothing is recorded when multi-tenancy is enabled`() {
        AlertCleanupService.multiTenancyEnabled = true

        val task = runBlocking {
            AlertCleanupService.recordMonitorCleanupTask(monitor, survivingTriggerIds = emptyList(), jobDeleted = true)
        }

        assertNull(task)
        verify(client, never()).search(Mockito.any(SearchRequest::class.java), Mockito.any())
        verify(client, never()).index(Mockito.any(IndexRequest::class.java), Mockito.any())
    }

    fun `test a node that loses the race for the lock does nothing`() {
        // This is the normal outcome for every recipient of an announcement but one.
        documents[LockService.LOCK_INDEX_NAME] = heldLockJson()
        documents[AlertCleanupService.CLEANUP_TASK_INDEX] = taskJson(jobDeleted = true)

        runBlocking { AlertCleanupService.runTask(TASK_ID) }

        assertEquals(listOf(LockModel.generateLockId(TASK_ID)), lockAttempts)
        assertTrue("A node that lost the race must not drain", searchRequests.isEmpty())
        verify(client, never()).bulk(Mockito.any(BulkRequest::class.java), Mockito.any())
        assertTrue("A lock that was never won must not be released", lockReleases.isEmpty())
    }

    fun `test an id that owes no cleanup never touches the lock index`() {
        // A delete notification carries ids that owe nothing: a monitor's metadata document is deleted from the same
        // config index, both scopes are offered for every delete, and a job whose alerts were all closed never had a
        // task recorded. Asking for a lock for each of those would fill the lock index with documents for no work.
        runBlocking { AlertCleanupService.runTask(AlertCleanupTask.taskId("$JOB_ID-metadata", CleanupScope.MONITOR)) }

        assertTrue("No task exists, so no lock may be asked for", lockAttempts.isEmpty())
        assertTrue(indexRequests.isEmpty())
        assertTrue(searchRequests.isEmpty())
    }

    fun `test a lock index whose shards are still recovering is not asked for a lock`() {
        // The lock index is created by whichever node first needs a lock, so its peers routinely arrive while the shards
        // are recovering. A read against a recovering shard fails with a 503 that looks exactly like a lost race, so the
        // cleanup would be abandoned by every node that did not create the index.
        val recovering = Mockito.mock(IndexRoutingTable::class.java)
        whenever(recovering.allPrimaryShardsActive()).thenReturn(false)
        lockIndexRouting = recovering
        documents[AlertCleanupService.CLEANUP_TASK_INDEX] = taskJson(jobDeleted = true)

        runBlocking { AlertCleanupService.runTask(TASK_ID) }

        assertTrue("The lock must not be read while its shards cannot serve a read", lockAttempts.isEmpty())
        assertTrue("Nothing may be drained without the lock", searchRequests.isEmpty())
    }

    fun `test a task whose job still exists is discarded without touching any alert`() {
        // The task is written before the job document is removed, so a failure in between leaves a task naming a job
        // that is still live. Draining it would move a live monitor's alerts into history.
        documents[AlertCleanupService.CLEANUP_TASK_INDEX] = taskJson(jobDeleted = true)
        documents[ScheduledJob.SCHEDULED_JOBS_INDEX] = """{"monitor":{}}"""

        runBlocking { AlertCleanupService.runTask(TASK_ID) }

        assertEquals(listOf(AlertCleanupService.CLEANUP_TASK_INDEX), deleteRequests.map { it.index() })
        verify(client, never()).search(Mockito.any(SearchRequest::class.java), Mockito.any())
        verify(client, never()).bulk(Mockito.any(BulkRequest::class.java), Mockito.any())
    }

    fun `test a completed task is removed`() {
        documents[AlertCleanupService.CLEANUP_TASK_INDEX] = taskJson(jobDeleted = true)
        searchResponses.add(emptyPage())

        runBlocking { AlertCleanupService.runTask(TASK_ID) }

        assertEquals(listOf(TASK_ID), deleteRequests.map { it.id() })
        assertEquals("The lock must be handed back, not left to expire", 1, lockReleases.size)
        verify(client, never()).bulk(Mockito.any(BulkRequest::class.java), Mockito.any())
    }

    fun `test a lock that cannot be read yet is retried rather than treated as a lost race`() {
        // A 503 from a lock index whose shards are still recovering is what every peer of the node that created that
        // index gets. It is indistinguishable from a held lock at the call site, so treating it as a lost race would
        // leave a task that no node is draining until the next resume pass came round.
        AlertCleanupService.retryPolicy = BackoffPolicy.constantBackoff(TimeValue.ZERO, 1)
        lockReadFailures.add(OpenSearchStatusException("shard is recovering", RestStatus.SERVICE_UNAVAILABLE))
        documents[AlertCleanupService.CLEANUP_TASK_INDEX] = taskJson(jobDeleted = true)
        searchResponses.add(emptyPage())

        runBlocking { AlertCleanupService.runTask(TASK_ID) }

        assertEquals("The lock must be asked for again after a retriable failure", 2, lockAttempts.size)
        assertEquals("The task must be drained by the node that retried", listOf(TASK_ID), deleteRequests.map { it.id() })
    }

    fun `test the advanced cursor is written back guarded on the version the task was read at`() {
        documents[AlertCleanupService.CLEANUP_TASK_INDEX] = taskJson(jobDeleted = true)
        // Exactly enough pages for one work unit, so the drain stops with the range beyond them still to do.
        repeat(AlertMover.MAX_PAGES_PER_WORK_UNIT) { page ->
            searchResponses.add(pageOf(seqNo = page.toLong()))
            bulkResponses.add(bulkResponse())
            bulkResponses.add(bulkResponse())
        }

        runBlocking { AlertCleanupService.runTask(TASK_ID) }

        val cursorWrite = cursorWrites.single()
        // Moving alerts is idempotent, but advancing the cursor is not: a holder whose lock was taken over must not be
        // able to write a stale cursor over a newer one, or the range in between is never drained.
        assertEquals(SEQ_NO, cursorWrite.ifSeqNo())
        assertEquals(PRIMARY_TERM, cursorWrite.ifPrimaryTerm())
        val source = cursorWrite.source().utf8ToString()
        val lastSeqNoMoved = AlertMover.MAX_PAGES_PER_WORK_UNIT - 1
        assertTrue(source, source.contains("\"cursor\":$lastSeqNoMoved"))
        assertTrue(source, source.contains("\"moved_count\":${AlertMover.MAX_PAGES_PER_WORK_UNIT}"))
        assertTrue("A task with work left must survive the work unit", deleteRequests.isEmpty())
        // Bounded units are what keep a drain inside the lock's five-minute expiry, so the lock must go back between
        // them rather than being held for the whole backlog.
        assertTrue("Each work unit takes the lock and hands it back", lockReleases.size >= 1)
    }

    fun `test the task is yielded when another node has already advanced its cursor`() {
        documents[AlertCleanupService.CLEANUP_TASK_INDEX] = taskJson(jobDeleted = true)
        repeat(AlertMover.MAX_PAGES_PER_WORK_UNIT) { page ->
            searchResponses.add(pageOf(seqNo = page.toLong()))
            bulkResponses.add(bulkResponse())
            bulkResponses.add(bulkResponse())
        }
        cursorWriteFailure = VersionConflictEngineException(SHARD_ID, TASK_ID, "already advanced")

        runBlocking { AlertCleanupService.runTask(TASK_ID) }

        // The lock is free here, so a node that ignored the conflict would happily re-acquire and re-drain the same
        // range from a cursor the new owner has already moved past.
        assertEquals("The task must be handed back after the conflict", 1, lockAttempts.size)
        assertEquals(1, lockReleases.size)
        assertTrue("The task must be left in place for its new owner", deleteRequests.isEmpty())
    }

    fun `test the lock is released even when the work unit fails`() {
        documents[AlertCleanupService.CLEANUP_TASK_INDEX] = taskJson(jobDeleted = true)
        // No page is queued, so the drain's first search fails with a status the retry policy does not retry.

        runBlocking { AlertCleanupService.runTask(TASK_ID) }

        // A held lock is only handed on after five minutes, so failing to release one delays every other node's attempt
        // at the task by that much.
        assertEquals(1, lockReleases.size)
        val released = lockReleases.single().doc().source().utf8ToString()
        assertTrue(released, released.contains("\"released\":true"))
        assertTrue("A task that could not be drained must survive", deleteRequests.isEmpty())
    }

    fun `test only data nodes are addressed for cleanup`() {
        // A paginated search plus two bulk operations per page is data-plane work, and a dedicated cluster manager
        // node's stability is what the cluster's ability to apply state depends on.
        assertEquals(listOf(dataNode.id), AlertCleanupService.nodeIdsForCleanup().toList())
    }

    fun `test nothing is announced when multi-tenancy is enabled`() {
        AlertCleanupService.multiTenancyEnabled = true

        AlertCleanupService.announce(JOB_ID)

        verify(clusterService, never()).state()
    }

    fun `test the resume pass does not run on a node that holds no data`() {
        whenever(clusterService.localNode()).thenReturn(clusterManagerNode)

        runBlocking { AlertCleanupService.resumeAbandonedTasks() }

        verify(client, never()).search(Mockito.any(SearchRequest::class.java), Mockito.any())
    }

    fun `test the resume pass offers every outstanding task to the cluster`() {
        val workflowTaskId = AlertCleanupTask.taskId("workflow-9", CleanupScope.WORKFLOW)
        searchResponses.add(taskListResponse(listOf(TASK_ID, workflowTaskId)))
        // Every lock is held elsewhere, so the pass only has to prove that it found both tasks and offered both.
        documents[LockService.LOCK_INDEX_NAME] = heldLockJson()
        documents[AlertCleanupService.CLEANUP_TASK_INDEX] = taskJson(jobDeleted = true)

        runBlocking { AlertCleanupService.resumeAbandonedTasks() }

        // Reading the task index is the whole of the discovery mechanism: nothing here infers orphanhood from alerts.
        assertEquals(AlertCleanupService.CLEANUP_TASK_INDEX, searchRequests.single().indices().single())
        assertEquals(
            listOf(LockModel.generateLockId(TASK_ID), LockModel.generateLockId(workflowTaskId)),
            lockAttempts
        )
    }

    private fun taskJson(jobDeleted: Boolean): String {
        val task = AlertCleanupTask(
            jobId = JOB_ID,
            scope = CleanupScope.MONITOR,
            alertIndex = "alerts",
            alertHistoryIndex = "history-write",
            survivingTriggerIds = emptyList(),
            jobDeleted = jobDeleted,
            createdAt = Instant.ofEpochMilli(1_700_000_000_000L)
        )
        return BytesReference.bytes(task.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS)).utf8ToString()
    }

    /** A lock taken a moment ago and not released, which is what a node still working on a task leaves behind. */
    private fun heldLockJson(): String {
        val held = LockModel(TASK_ID, Instant.now(), false)
        return BytesReference.bytes(held.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS)).utf8ToString()
    }

    private fun getResponse(source: String?): GetResponse {
        val response = Mockito.mock(GetResponse::class.java)
        whenever(response.isExists).thenReturn(source != null)
        whenever(response.sourceAsString).thenReturn(source)
        whenever(response.seqNo).thenReturn(SEQ_NO)
        whenever(response.primaryTerm).thenReturn(PRIMARY_TERM)
        return response
    }

    private fun countResponse(total: Long) =
        searchResponse(SearchHits(emptyArray(), TotalHits(total, TotalHits.Relation.EQUAL_TO), 1.0f))

    private fun emptyPage() = countResponse(0L)

    private fun pageOf(seqNo: Long): SearchResponse {
        val source = randomAlert(monitor).toXContentWithUser(XContentFactory.jsonBuilder())
        val hit = SearchHit(seqNo.toInt(), "alert-$seqNo", emptyMap(), emptyMap()).apply {
            version(1L)
            setSeqNo(seqNo)
            setPrimaryTerm(1L)
            sourceRef(BytesReference.bytes(source))
        }
        return searchResponse(SearchHits(arrayOf(hit), TotalHits(500L, TotalHits.Relation.EQUAL_TO), 1.0f))
    }

    private fun taskListResponse(taskIds: List<String>): SearchResponse {
        val hits = taskIds.mapIndexed { index, id -> SearchHit(index, id, emptyMap(), emptyMap()) }.toTypedArray()
        return searchResponse(SearchHits(hits, TotalHits(taskIds.size.toLong(), TotalHits.Relation.EQUAL_TO), 1.0f))
    }

    private fun searchResponse(hits: SearchHits): SearchResponse {
        val response = Mockito.mock(SearchResponse::class.java)
        whenever(response.hits).thenReturn(hits)
        return response
    }

    private fun bulkResponse(): BulkResponse = BulkResponse(
        arrayOf(
            BulkItemResponse(
                0,
                DocWriteRequest.OpType.INDEX,
                IndexResponse(SHARD_ID, "alert-0", 1L, 1L, 1L, true)
            )
        ),
        1L
    )

    private fun nodes(): DiscoveryNodes = DiscoveryNodes.builder()
        .add(dataNode)
        .add(clusterManagerNode)
        .localNodeId(dataNode.id)
        .clusterManagerNodeId(clusterManagerNode.id)
        .build()

    companion object {
        private const val JOB_ID = "monitor-1"
        private const val SEQ_NO = 7L
        private const val PRIMARY_TERM = 3L
        private val TASK_ID = AlertCleanupTask.taskId(JOB_ID, CleanupScope.MONITOR)
        private val SHARD_ID = ShardId(Index("alerts", "uuid"), 0)

        private val dataNode = DiscoveryNode(
            "data-node",
            "data-node-id",
            buildNewFakeTransportAddress(),
            emptyMap(),
            setOf(DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        )
        private val clusterManagerNode = DiscoveryNode(
            "cluster-manager",
            "cluster-manager-id",
            buildNewFakeTransportAddress(),
            emptyMap(),
            setOf(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE),
            Version.CURRENT
        )
    }
}

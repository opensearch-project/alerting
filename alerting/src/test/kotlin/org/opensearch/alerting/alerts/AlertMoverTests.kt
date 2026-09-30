/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.alerts

import kotlinx.coroutines.runBlocking
import org.apache.lucene.search.TotalHits
import org.junit.Before
import org.mockito.Mockito
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.DocWriteRequest
import org.opensearch.action.bulk.BulkItemResponse
import org.opensearch.action.bulk.BulkRequest
import org.opensearch.action.bulk.BulkResponse
import org.opensearch.action.delete.DeleteRequest
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.index.IndexResponse
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.alerting.cleanup.AlertCleanupTask
import org.opensearch.alerting.cleanup.CleanupScope
import org.opensearch.alerting.randomAlert
import org.opensearch.alerting.randomQueryLevelMonitor
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.core.action.ActionListener
import org.opensearch.core.common.bytes.BytesReference
import org.opensearch.core.index.Index
import org.opensearch.core.index.shard.ShardId
import org.opensearch.core.rest.RestStatus
import org.opensearch.index.VersionType
import org.opensearch.search.SearchHit
import org.opensearch.search.SearchHits
import org.opensearch.search.sort.FieldSortBuilder
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.transport.client.Client
import org.mockito.Mockito.`when` as whenever

/**
 * Unit coverage for the bounded work unit that [AlertMover.drainAlerts] performs. The properties asserted here are the
 * ones the cleanup's recoverability rests on: the drain stops after a bounded number of pages, hands back a cursor that
 * a *different* node can resume from, and never advances past a page it did not move.
 */
class AlertMoverTests : OpenSearchTestCase() {

    private lateinit var client: Client
    private val searchRequests = mutableListOf<SearchRequest>()
    private val bulkRequests = mutableListOf<BulkRequest>()
    private val searchResponses = ArrayDeque<SearchResponse>()
    private val bulkResponses = ArrayDeque<BulkResponse>()

    @Before
    fun setup() {
        client = Mockito.mock(Client::class.java)
        searchRequests.clear()
        bulkRequests.clear()
        searchResponses.clear()
        bulkResponses.clear()

        whenever(client.search(Mockito.any(SearchRequest::class.java), Mockito.any())).thenAnswer { invocation ->
            searchRequests.add(invocation.arguments[0] as SearchRequest)
            @Suppress("UNCHECKED_CAST")
            (invocation.arguments[1] as ActionListener<SearchResponse>).onResponse(searchResponses.removeFirst())
        }
        whenever(client.bulk(Mockito.any(BulkRequest::class.java), Mockito.any())).thenAnswer { invocation ->
            bulkRequests.add(invocation.arguments[0] as BulkRequest)
            @Suppress("UNCHECKED_CAST")
            (invocation.arguments[1] as ActionListener<BulkResponse>).onResponse(bulkResponses.removeFirst())
        }
    }

    fun `test the drain stops after maxPages and returns a cursor to resume from`() {
        // Three pages are available but only two may be moved under one lock acquisition.
        repeat(3) { page -> searchResponses.add(pageOf(seqNos = listOf(page * 2L, page * 2L + 1), total = 6)) }
        // One copy and one delete response per page, each covering both alerts on that page.
        repeat(6) { bulkResponses.add(bulkResponse(ids = listOf("alert-0", "alert-1"), failed = false)) }

        val result = runBlocking { AlertMover.drainAlerts(client, task(), maxPages = 2) }

        assertFalse("A drain that hit its page bound is not finished", result.finished)
        assertEquals("The cursor must be the last seq no actually moved", 3L, result.nextCursor)
        assertEquals(4L, result.movedCount)
        assertEquals("Exactly maxPages searches may be issued", 2, searchRequests.size)
        assertEquals(6L, result.remainingAtStart)
        assertNull(result.failure)
    }

    fun `test the drain resumes from the task's cursor`() {
        searchResponses.add(emptyPage())

        runBlocking { AlertMover.drainAlerts(client, task(cursor = 512L)) }

        val source = searchRequests.single().source()
        assertEquals("search_after must start at the persisted cursor", 512L, source.searchAfter()[0])
        assertEquals(AlertMover.MOVE_ALERTS_PAGE_SIZE, source.size())
        // Without an accurate total the outstanding count is capped at 10,000, which is the same class of silent
        // truncation as the default search size of 10.
        assertEquals(Int.MAX_VALUE, source.trackTotalHitsUpTo())
        assertEquals("_seq_no", (source.sorts().single() as FieldSortBuilder).fieldName)
    }

    fun `test the drain finishes when a page comes back empty`() {
        searchResponses.add(pageOf(seqNos = listOf(0L), total = 1))
        bulkResponses.add(bulkResponse(ids = listOf("alert"), failed = false))
        bulkResponses.add(bulkResponse(ids = listOf("alert"), failed = false))
        searchResponses.add(emptyPage())

        val result = runBlocking { AlertMover.drainAlerts(client, task()) }

        assertTrue("An empty page means the task is complete and may be removed", result.finished)
        assertNull(result.nextCursor)
        assertEquals(1L, result.movedCount)
    }

    fun `test the drain ends rather than looping when the cursor cannot advance`() {
        // A page whose last seq no is not greater than the cursor would be searched again forever.
        searchResponses.add(pageOf(seqNos = listOf(100L), total = 1))
        bulkResponses.add(bulkResponse(ids = listOf("alert"), failed = false))
        bulkResponses.add(bulkResponse(ids = listOf("alert"), failed = false))

        val result = runBlocking { AlertMover.drainAlerts(client, task(cursor = 100L)) }

        assertTrue(result.finished)
        assertEquals("The non-advancing page must not be searched twice", 1, searchRequests.size)
    }

    fun `test alerts are copied with EXTERNAL_GTE so a replay is harmless`() {
        searchResponses.add(pageOf(seqNos = listOf(0L), total = 1))
        bulkResponses.add(bulkResponse(ids = listOf("alert-0"), failed = false))
        bulkResponses.add(bulkResponse(ids = listOf("alert-0"), failed = false))
        searchResponses.add(emptyPage())

        runBlocking { AlertMover.drainAlerts(client, task()) }

        val copy = bulkRequests.first().requests().single() as IndexRequest
        assertEquals("history-write", copy.index())
        assertEquals("alert-0", copy.id())
        assertEquals(VersionType.EXTERNAL_GTE, copy.versionType())
        assertEquals(ALERT_VERSION, copy.version())
        assertEquals("monitor-1", copy.routing())
        assertTrue("A moved alert must be recorded as DELETED", copy.source().utf8ToString().contains("\"state\":\"DELETED\""))
    }

    fun `test only successfully copied alerts are deleted from the live index`() {
        searchResponses.add(pageOf(seqNos = listOf(0L, 1L), total = 2))
        // The second alert failed to copy, so deleting it would lose it.
        bulkResponses.add(
            bulkResponse(
                items = listOf(
                    itemResponse(id = "alert-0", failed = false),
                    itemResponse(id = "alert-1", failed = true, status = RestStatus.INTERNAL_SERVER_ERROR)
                )
            )
        )
        bulkResponses.add(bulkResponse(ids = listOf("alert-0"), failed = false))
        searchResponses.add(emptyPage())

        val result = runBlocking { AlertMover.drainAlerts(client, task()) }

        val deletes = bulkRequests[1].requests().map { it as DeleteRequest }
        assertEquals(listOf("alert-0"), deletes.map { it.id() })
        assertEquals("Only the alert that reached history counts as moved", 1L, result.movedCount)
        assertNotNull("The copy failure must be reported", result.failure)
    }

    fun `test a throttled bulk item is reported as 429 so the caller retries`() {
        searchResponses.add(pageOf(seqNos = listOf(0L), total = 1))
        bulkResponses.add(
            bulkResponse(
                items = listOf(itemResponse(id = "alert-0", failed = true, status = RestStatus.TOO_MANY_REQUESTS))
            )
        )
        searchResponses.add(emptyPage())

        val result = runBlocking { AlertMover.drainAlerts(client, task()) }

        // Bulk rejections surface as 429s on individual items, not as a retriable status on the request, so the status
        // has to be lifted onto the returned failure for BackoffPolicy.retry to engage on it.
        assertEquals(RestStatus.TOO_MANY_REQUESTS, result.failure!!.status())
        assertEquals("Nothing was copied, so nothing may be deleted", 1, bulkRequests.size)
        assertEquals(0L, result.movedCount)
    }

    private fun task(cursor: Long = AlertCleanupTask.NO_CURSOR) = AlertCleanupTask(
        jobId = "monitor-1",
        scope = CleanupScope.MONITOR,
        alertIndex = "alerts",
        alertHistoryIndex = "history-write",
        survivingTriggerIds = emptyList(),
        jobDeleted = true,
        cursor = cursor
    )

    private fun emptyPage() = searchResponse(SearchHits(emptyArray(), TotalHits(0L, TotalHits.Relation.EQUAL_TO), 1.0f))

    private fun pageOf(seqNos: List<Long>, total: Long): SearchResponse {
        val hits = seqNos.mapIndexed { index, seqNo -> hit("alert-$index", seqNo) }.toTypedArray()
        return searchResponse(SearchHits(hits, TotalHits(total, TotalHits.Relation.EQUAL_TO), 1.0f))
    }

    private fun searchResponse(hits: SearchHits): SearchResponse {
        val response = Mockito.mock(SearchResponse::class.java)
        whenever(response.hits).thenReturn(hits)
        return response
    }

    private fun hit(id: String, seqNo: Long): SearchHit {
        val source = randomAlert(randomQueryLevelMonitor()).toXContentWithUser(XContentFactory.jsonBuilder())
        return SearchHit(seqNo.toInt(), id, emptyMap(), emptyMap()).apply {
            version(ALERT_VERSION)
            setSeqNo(seqNo)
            setPrimaryTerm(1L)
            sourceRef(BytesReference.bytes(source))
        }
    }

    private fun bulkResponse(ids: List<String>, failed: Boolean): BulkResponse =
        bulkResponse(ids.mapIndexed { index, id -> itemResponse(index, id, failed) })

    private fun bulkResponse(items: List<BulkItemResponse>): BulkResponse =
        BulkResponse(items.toTypedArray(), 1L)

    private fun itemResponse(
        itemId: Int = 0,
        id: String,
        failed: Boolean,
        status: RestStatus = RestStatus.OK,
    ): BulkItemResponse = if (failed) {
        BulkItemResponse(
            itemId,
            DocWriteRequest.OpType.INDEX,
            BulkItemResponse.Failure(INDEX, id, OpenSearchStatusException("rejected", status), status)
        )
    } else {
        BulkItemResponse(
            itemId,
            DocWriteRequest.OpType.INDEX,
            IndexResponse(SHARD_ID, id, 1L, 1L, ALERT_VERSION, true)
        )
    }

    companion object {
        private const val ALERT_VERSION = 3L
        private const val INDEX = "alerts"
        private val SHARD_ID = ShardId(Index(INDEX, "uuid"), 0)
    }
}

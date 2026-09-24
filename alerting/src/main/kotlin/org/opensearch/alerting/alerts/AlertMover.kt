/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.alerts

import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.bulk.BulkRequest
import org.opensearch.action.bulk.BulkResponse
import org.opensearch.action.delete.DeleteRequest
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.alerting.cleanup.AlertCleanupTask
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.core.common.bytes.BytesReference
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParserUtils
import org.opensearch.index.VersionType
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.search.sort.FieldSortBuilder
import org.opensearch.transport.client.Client

private val log = LogManager.getLogger(AlertMover::class.java)

/**
 * What one bounded work unit accomplished.
 *
 * [nextCursor] is `null` when the drain is complete and the task may be removed. Otherwise it is the cursor the next
 * work unit must resume from, and it is written back to the task document before the lock is released.
 */
data class AlertDrainResult(
    val nextCursor: Long?,
    val movedCount: Long,
    /** Alerts still matching the task's query at the start of this work unit, including those moved by it. */
    val remainingAtStart: Long,
    /**
     * Failures accumulated across the pages of this work unit, or `null`. Carries the failure's status so the caller's
     * `BackoffPolicy.retry` can engage on `TOO_MANY_REQUESTS`; the work unit is safe to re-run either way.
     */
    val failure: OpenSearchStatusException?,
) {
    val finished: Boolean get() = nextCursor == null
}

class AlertMover {
    companion object {
        /**
         * The number of alerts moved to the history index per round trip. [drainAlerts] pages within a work unit, so
         * this only bounds the size of each search/bulk pair.
         */
        const val MOVE_ALERTS_PAGE_SIZE = 100

        /**
         * Pages moved under a single lock acquisition before the cursor is persisted and the lock released.
         *
         * The cleanup lock is handed to any claimant once it is five minutes old and its holder has no way to refresh
         * it, so a work unit must finish comfortably inside that window. 20 pages is 2,000 alerts -- four searches and
         * bulk pairs' worth of latency, far short of five minutes, while still amortising the lock round trips over a
         * backlog of tens of thousands.
         */
        const val MAX_PAGES_PER_WORK_UNIT = 20

        /**
         * Moves up to [maxPages] pages of the alerts named by [task] into its history index as [Alert.State.DELETED],
         * resuming from [AlertCleanupTask.cursor].
         *
         * Pagination uses `search_after` on `_seq_no` (the same idiom as JobSweeper.sweepShard) rather than a plain
         * `size`, so the drain is bounded by neither the default search size of 10 nor `index.max_result_window`, does
         * not depend on the deletes from the previous page being visible to the next search, and -- because `_seq_no`
         * is a stable, strictly increasing document property -- can be stopped after any page and resumed later from
         * the same position by a different node.
         *
         * Failures from every page are accumulated and returned once at the end so that a single poison document does
         * not head-of-line block the remaining pages. Re-running a work unit is harmless: copies use
         * `VersionType.EXTERNAL_GTE` and successfully copied alerts are removed from the live index.
         */
        suspend fun drainAlerts(
            client: Client,
            task: AlertCleanupTask,
            maxPages: Int = MAX_PAGES_PER_WORK_UNIT,
        ): AlertDrainResult {
            val boolQuery = task.query()
            var searchAfter: Long? = task.cursor
            var remainingAtStart = -1L
            var movedCount = 0L
            var pages = 0
            val failureMessages = mutableListOf<String>()
            var retryCause: Throwable? = null
            var failureStatus: RestStatus? = null

            fun recordFailures(response: BulkResponse, verb: String) {
                val failedItems = response.items.filter { it.isFailed }
                if (failedItems.isEmpty()) return
                failureMessages.add("Failed to $verb alerts for [${task.taskId}]: ${response.buildFailureMessage()}")
                val throttled = failedItems.firstOrNull { it.status() == RestStatus.TOO_MANY_REQUESTS }
                if (throttled != null) {
                    // Prefer a throttling status/cause so the caller's retry policy engages.
                    retryCause = throttled.failure?.cause
                    failureStatus = RestStatus.TOO_MANY_REQUESTS
                } else if (failureStatus == null) {
                    failureStatus = failedItems.first().status()
                }
            }

            while (searchAfter != null && pages < maxPages) {
                pages++
                val activeAlertsQuery = SearchSourceBuilder.searchSource()
                    .query(boolQuery)
                    .version(true)
                    .seqNoAndPrimaryTerm(true)
                    .sort(FieldSortBuilder("_seq_no").unmappedType("long"))
                    .searchAfter(arrayOf(searchAfter))
                    .size(MOVE_ALERTS_PAGE_SIZE)
                    .trackTotalHits(true)

                val activeAlertsRequest = SearchRequest(task.alertIndex)
                    .routing(task.jobId)
                    .source(activeAlertsQuery)
                val response: SearchResponse = client.suspendUntil { search(activeAlertsRequest, it) }

                if (remainingAtStart < 0) remainingAtStart = response.hits.totalHits?.value ?: 0L

                val hits = response.hits.hits
                // Nothing left to move: the drain is complete.
                if (hits.isEmpty()) {
                    searchAfter = null
                    break
                }

                // Advance the cursor before doing any work so that a page whose documents all fail to copy cannot
                // cause an infinite loop. `_seq_no` is strictly increasing, so a non-advancing cursor ends the drain.
                //
                // Known limitation. An alert that fails to copy is excluded from the delete below and stays in the live
                // index, but the cursor has already moved past its `_seq_no`, so neither a later work unit nor a resume
                // pass revisits it -- and once a subsequent unit reaches the end of the range cleanly, the task is
                // removed and that alert is leaked. This applies to any per-item copy failure, not only a permanent one
                // such as a document that `Alert.parse` cannot read. Trading it for loop-freedom is deliberate: the
                // alternative is tracking the lowest failed `_seq_no` and rewinding to it, which reintroduces exactly
                // the unbounded retry this line prevents unless the number of rewinds is also bounded and persisted.
                // Failures are logged per page, so a leak of this kind is visible in the log rather than silent.
                val nextSearchAfter = hits.last().seqNo
                searchAfter = if (nextSearchAfter > searchAfter!!) nextSearchAfter else null

                val indexRequests = hits.map { hit ->
                    IndexRequest(task.alertHistoryIndex)
                        .routing(task.jobId)
                        .source(
                            Alert.parse(alertContentParser(hit.sourceRef), hit.id, hit.version)
                                .copy(state = Alert.State.DELETED)
                                .toXContentWithUser(XContentFactory.jsonBuilder())
                        )
                        .version(hit.version)
                        .versionType(VersionType.EXTERNAL_GTE)
                        .id(hit.id)
                }
                val copyResponse: BulkResponse = client.suspendUntil { bulk(BulkRequest().add(indexRequests), it) }
                recordFailures(copyResponse, "copy")

                val deleteRequests = copyResponse.items.filterNot { it.isFailed }.map {
                    DeleteRequest(task.alertIndex, it.id)
                        .routing(task.jobId)
                        .version(it.version)
                        .versionType(VersionType.EXTERNAL_GTE)
                }
                if (deleteRequests.isEmpty()) continue

                val deleteResponse: BulkResponse = client.suspendUntil { bulk(BulkRequest().add(deleteRequests), it) }
                recordFailures(deleteResponse, "delete")
                movedCount += deleteResponse.items.count { !it.isFailed }
            }

            log.info(
                "Moved $movedCount alert(s) from ${task.alertIndex} to ${task.alertHistoryIndex} for " +
                    "[${task.taskId}] in $pages page(s); ${if (searchAfter == null) "drain complete" else "resuming at $searchAfter"}."
            )

            return AlertDrainResult(
                nextCursor = searchAfter,
                movedCount = movedCount,
                remainingAtStart = if (remainingAtStart < 0) 0L else remainingAtStart,
                failure = if (failureMessages.isEmpty()) {
                    null
                } else {
                    OpenSearchStatusException(
                        failureMessages.joinToString("; "),
                        failureStatus ?: RestStatus.INTERNAL_SERVER_ERROR,
                        retryCause
                    )
                }
            )
        }

        private fun alertContentParser(bytesReference: BytesReference): XContentParser {
            val xcp = XContentHelper.createParser(
                NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE,
                bytesReference, XContentType.JSON
            )
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
            return xcp
        }
    }
}

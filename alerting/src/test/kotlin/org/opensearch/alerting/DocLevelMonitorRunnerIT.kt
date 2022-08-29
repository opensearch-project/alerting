    /*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.alerting

import org.opensearch.action.support.WriteRequest
import org.opensearch.alerting.action.IndexMonitorAction
import org.opensearch.alerting.action.IndexMonitorRequest
import org.opensearch.alerting.action.IndexMonitorResponse
import org.opensearch.alerting.core.model.DocLevelMonitorInput
import org.opensearch.alerting.core.model.DocLevelQuery
import org.opensearch.alerting.model.Monitor
import org.opensearch.common.settings.Settings
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO
import org.opensearch.rest.RestRequest
import org.opensearch.test.OpenSearchSingleNodeTestCase
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.Locale


//TODO explain the diff between rest tests and transport layer tests for these files
abstract class DocLevelMonitorRunnerIT : OpenSearchSingleNodeTestCase() {

    fun `test execute monitor stores alerts in custom index`() {
        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(ChronoUnit.MILLIS))
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""

        val index = createTestIndex()
        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3")
        val docLevelInput = DocLevelMonitorInput("description", listOf(index), listOf(docQuery))
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val monitorResponse = createMonitor(randomDocumentLevelMonitor(inputs = listOf(docLevelInput), triggers = listOf(trigger)))
        assertFalse(monitorResponse?.id.isNullOrEmpty())
    }

    /** A test index that can be used across tests. Feel free to add new fields but don't remove any. */
    protected fun createTestIndex(index: String = randomAlphaOfLength(10).lowercase(Locale.ROOT)): String {
        createIndex(
            index, Settings.EMPTY,
            """
                "properties" : {
                  "test_strict_date_time" : { "type" : "date", "format" : "strict_date_time" },
                  "test_field" : { "type" : "keyword" }
                }
            """.trimIndent()
        )
        return index
    }

    protected fun createMonitor(monitor: Monitor): IndexMonitorResponse? {
        val request = IndexMonitorRequest(
            monitorId = Monitor.NO_ID,
            seqNo = UNASSIGNED_SEQ_NO,
            primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
            refreshPolicy = WriteRequest.RefreshPolicy.parse("true"),
            method = RestRequest.Method.POST,
            monitor = monitor
        )
        return client().execute(IndexMonitorAction.INSTANCE, request).actionGet()
    }
}

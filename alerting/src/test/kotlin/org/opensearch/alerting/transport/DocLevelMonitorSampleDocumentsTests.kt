/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import org.opensearch.action.support.WriteRequest
import org.opensearch.alerting.ALWAYS_RUN
import org.opensearch.alerting.randomAction
import org.opensearch.alerting.randomDocumentLevelMonitor
import org.opensearch.alerting.randomDocumentLevelTrigger
import org.opensearch.alerting.randomTemplateScript
import org.opensearch.common.UUIDs
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.model.ActionRunResult
import org.opensearch.commons.alerting.model.DocLevelMonitorInput
import org.opensearch.commons.alerting.model.DocLevelQuery
import org.opensearch.commons.alerting.model.DocumentLevelTriggerRunResult
import java.util.Locale

/**
 * Regression tests for the bugfix that ensures doc-level monitor action templates
 * can reliably access the original matched document through `sample_documents`.
 *
 * Bug: For doc-level monitors, `sample_documents` was either empty or contained
 * fields transformed for percolation (with dynamic index-based suffixes), making it
 * impossible to access original document fields like `_source.trigger_name` in Mustache
 * templates. This is particularly problematic for routed indices (e.g. Security Analytics
 * hidden alert indices where `_routing: required: true`).
 *
 * Fix: The original document source is now captured in a deep copy before
 * field-name transformation for percolation, and exposed in `sample_documents` under
 * the stable shape: `{ "_id": ..., "_index": ..., "_source": { ...original fields... } }`.
 */
class DocLevelMonitorSampleDocumentsTests : AlertingSingleNodeTestCase() {

    companion object {
        private const val TRIGGER_NAME_FIELD = "trigger_name"
        private const val SEVERITY_FIELD = "severity"
        private const val STATE_FIELD = "state"
        private const val MONITOR_NAME_FIELD = "monitor_name"
        private const val AGG_ALERT_CONTENT_FIELD = "agg_alert_content"
    }

    /**
     * Creates an index with field mappings that closely resemble a Security Analytics alert document
     * (the real-world routed-index scenario that exposed this bug).
     */
    private fun createAlertLikeIndex(indexName: String) {
        val mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(TRIGGER_NAME_FIELD).field("type", "keyword").endObject()
            .startObject(SEVERITY_FIELD).field("type", "keyword").endObject()
            .startObject(STATE_FIELD).field("type", "keyword").endObject()
            .startObject(MONITOR_NAME_FIELD).field("type", "keyword").endObject()
            .startObject("start_time").field("type", "long").endObject()
            .startObject(AGG_ALERT_CONTENT_FIELD)
            .field("type", "object")
            .startObject("properties")
            .startObject("parent_bucket_path").field("type", "keyword").endObject()
            .startObject("bucket_keys").field("type", "keyword").endObject()
            .startObject("bucket")
            .field("type", "object")
            .startObject("properties")
            .startObject("key").field("type", "keyword").endObject()
            .startObject("doc_count").field("type", "long").endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()

        createIndex(indexName, Settings.EMPTY, mapping)
    }

    /**
     * Returns a JSON document resembling a Security Analytics alert.
     */
    private fun alertLikeDoc(
        triggerName: String = "Test Alert Rule",
        severity: String = "high",
        state: String = "ACTIVE",
        monitorName: String = "Test Monitor",
        startTime: Long = 1779222727893L,
        bucketKey: String = "100.90.123.33",
        docCount: Int = 216
    ): String = """
        {
          "trigger_name": "$triggerName",
          "severity": "$severity",
          "state": "$state",
          "monitor_name": "$monitorName",
          "start_time": $startTime,
          "agg_alert_content": {
            "parent_bucket_path": "result_agg",
            "bucket_keys": ["$bucketKey"],
            "bucket": {
              "key": "$bucketKey",
              "doc_count": $docCount
            }
          }
        }
    """.trimIndent()

    // -----------------------------------------------------------------
    // Test 1: sample_documents exposes _id and _index
    // -----------------------------------------------------------------

    /**
     * Verifies that after the bugfix, each entry in `sample_documents` contains
     * the stable metadata fields `_id` and `_index` pointing to the original
     * matched document.
     */
    fun `test doc-level monitor sample_documents exposes _id and _index`() {
        val sourceIndex = "test-alert-index-${randomAlphaOfLength(8).lowercase(Locale.ROOT)}"
        createAlertLikeIndex(sourceIndex)

        val docId = UUIDs.randomBase64UUID()
        client().prepareIndex(sourceIndex).setId(docId)
            .setSource(alertLikeDoc(), XContentType.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get()

        val docQuery = DocLevelQuery(
            query = "trigger_name:\"Test Alert Rule\"",
            name = "test-query",
            fields = listOf()
        )

        val templateSource =
            "ID={{#ctx.alerts}}{{#sample_documents}}{{_id}}{{/sample_documents}}{{/ctx.alerts}}" +
                "|INDEX={{#ctx.alerts}}{{#sample_documents}}{{_index}}{{/sample_documents}}{{/ctx.alerts}}"

        val action = randomAction(template = randomTemplateScript(templateSource))
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN, actions = listOf(action))
        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(DocLevelMonitorInput("test", listOf(sourceIndex), listOf(docQuery))),
            triggers = listOf(trigger)
        )

        val createdMonitor = createMonitor(monitor)
        assertNotNull("Monitor was not created", createdMonitor)

        val response = executeMonitor(createdMonitor!!.monitor, createdMonitor.id, dryRun = false)
        assertNotNull("Execute monitor returned null", response)

        val triggerResults = response!!.monitorRunResult.triggerResults
        assertTrue("No trigger results returned", triggerResults.isNotEmpty())

        triggerResults.values.filterIsInstance<DocumentLevelTriggerRunResult>().forEach { triggerResult ->
            triggerResult.actionResultsMap.values.forEach { actionResultMap: Map<String, ActionRunResult> ->
                actionResultMap.values.forEach { actionResult ->
                    val message = actionResult.output["message"] ?: ""
                    assertTrue(
                        "Expected _id to be present in rendered message but got: $message",
                        message.contains("ID=") && !message.contains("ID=|")
                    )
                    assertTrue(
                        "Expected _index '$sourceIndex' to be present in rendered message but got: $message",
                        message.contains("INDEX=$sourceIndex")
                    )
                }
            }
        }
    }

    // -----------------------------------------------------------------
    // Test 2: sample_documents exposes original source fields via _source.*
    // -----------------------------------------------------------------

    /**
     * Verifies that the original document fields are accessible through `_source.*`
     * in the notification template, without transformation or dynamic suffixes.
     *
     * This is the main regression test for the bug:
     * "doc-level monitor `sample_documents` contains percolation-transformed fields
     * instead of the original matched document fields".
     */
    fun `test doc-level monitor sample_documents exposes original source fields via _source`() {
        val sourceIndex = "test-alert-index-${randomAlphaOfLength(8).lowercase(Locale.ROOT)}"
        createAlertLikeIndex(sourceIndex)

        val triggerName = "Test Alert Rule"
        val severity = "high"
        val state = "ACTIVE"
        val monitorName = "Test Monitor"

        client().prepareIndex(sourceIndex).setId(UUIDs.randomBase64UUID())
            .setSource(
                alertLikeDoc(
                    triggerName = triggerName,
                    severity = severity,
                    state = state,
                    monitorName = monitorName
                ),
                XContentType.JSON
            )
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get()

        val docQuery = DocLevelQuery(
            query = "trigger_name:\"Test Alert Rule\"",
            name = "test-query",
            fields = listOf()
        )

        val templateSource =
            "RULE={{#ctx.alerts}}{{#sample_documents}}{{_source.trigger_name}}{{/sample_documents}}{{/ctx.alerts}}" +
                "|SEV={{#ctx.alerts}}{{#sample_documents}}{{_source.severity}}{{/sample_documents}}{{/ctx.alerts}}" +
                "|STATE={{#ctx.alerts}}{{#sample_documents}}{{_source.state}}{{/sample_documents}}{{/ctx.alerts}}" +
                "|MON={{#ctx.alerts}}{{#sample_documents}}{{_source.monitor_name}}{{/sample_documents}}{{/ctx.alerts}}"

        val action = randomAction(template = randomTemplateScript(templateSource))
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN, actions = listOf(action))
        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(DocLevelMonitorInput("test", listOf(sourceIndex), listOf(docQuery))),
            triggers = listOf(trigger)
        )

        val createdMonitor = createMonitor(monitor)
        assertNotNull(createdMonitor)

        val response = executeMonitor(createdMonitor!!.monitor, createdMonitor.id, dryRun = false)
        assertNotNull(response)

        val triggerResults = response!!.monitorRunResult.triggerResults
        assertTrue(triggerResults.isNotEmpty())

        triggerResults.values.filterIsInstance<DocumentLevelTriggerRunResult>().forEach { triggerResult ->
            triggerResult.actionResultsMap.values.forEach { actionResultMap: Map<String, ActionRunResult> ->
                actionResultMap.values.forEach { actionResult ->
                    val message = actionResult.output["message"] ?: ""
                    assertTrue(
                        "Expected _source.trigger_name='$triggerName' in rendered message but got: $message",
                        message.contains("RULE=$triggerName")
                    )
                    assertTrue(
                        "Expected _source.severity='$severity' in rendered message but got: $message",
                        message.contains("SEV=$severity")
                    )
                    assertTrue(
                        "Expected _source.state='$state' in rendered message but got: $message",
                        message.contains("STATE=$state")
                    )
                    assertTrue(
                        "Expected _source.monitor_name='$monitorName' in rendered message but got: $message",
                        message.contains("MON=$monitorName")
                    )
                }
            }
        }
    }

    // -----------------------------------------------------------------
    // Test 3: nested source objects survive percolation transformation
    // -----------------------------------------------------------------

    /**
     * Verifies that nested objects inside `_source` are not mutated or replaced by
     * percolation transformation suffixes.
     *
     * This specifically tests the deep-copy protection added in the bugfix.
     * Before the fix, the shared mutable maps used for percolation transformation would
     * contaminate `_source.agg_alert_content.bucket.key` with a suffixed name.
     */
    fun `test doc-level monitor sample_documents nested objects are not mutated by percolation`() {
        val sourceIndex = "test-alert-index-${randomAlphaOfLength(8).lowercase(Locale.ROOT)}"
        createAlertLikeIndex(sourceIndex)

        val bucketKey = "100.90.123.33"
        val docCount = 216

        client().prepareIndex(sourceIndex).setId(UUIDs.randomBase64UUID())
            .setSource(alertLikeDoc(bucketKey = bucketKey, docCount = docCount), XContentType.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get()

        val docQuery = DocLevelQuery(
            query = "trigger_name:\"Test Alert Rule\"",
            name = "test-query",
            fields = listOf()
        )

        val templateSource =
            "BUCKET={{#ctx.alerts}}{{#sample_documents}}" +
                "{{#_source.agg_alert_content}}{{#bucket}}{{key}}{{/bucket}}{{/_source.agg_alert_content}}" +
                "{{/sample_documents}}{{/ctx.alerts}}" +
                "|COUNT={{#ctx.alerts}}{{#sample_documents}}" +
                "{{#_source.agg_alert_content}}{{#bucket}}{{doc_count}}{{/bucket}}{{/_source.agg_alert_content}}" +
                "{{/sample_documents}}{{/ctx.alerts}}" +
                "|KEYS={{#ctx.alerts}}{{#sample_documents}}" +
                "{{#_source.agg_alert_content}}{{bucket_keys}}{{/_source.agg_alert_content}}" +
                "{{/sample_documents}}{{/ctx.alerts}}"

        val action = randomAction(template = randomTemplateScript(templateSource))
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN, actions = listOf(action))
        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(DocLevelMonitorInput("test", listOf(sourceIndex), listOf(docQuery))),
            triggers = listOf(trigger)
        )

        val createdMonitor = createMonitor(monitor)
        assertNotNull(createdMonitor)

        val response = executeMonitor(createdMonitor!!.monitor, createdMonitor.id, dryRun = false)
        assertNotNull(response)

        val triggerResults = response!!.monitorRunResult.triggerResults
        assertTrue(triggerResults.isNotEmpty())

        triggerResults.values.filterIsInstance<DocumentLevelTriggerRunResult>().forEach { triggerResult ->
            triggerResult.actionResultsMap.values.forEach { actionResultMap: Map<String, ActionRunResult> ->
                actionResultMap.values.forEach { actionResult ->
                    val message = actionResult.output["message"] ?: ""
                    assertTrue(
                        "Expected bucket key '$bucketKey' in rendered message but got: $message",
                        message.contains("BUCKET=$bucketKey")
                    )
                    assertTrue(
                        "Expected doc_count '$docCount' in rendered message but got: $message",
                        message.contains("COUNT=$docCount")
                    )
                    assertTrue(
                        "Expected bucket_keys list containing '$bucketKey' in rendered message but got: $message",
                        message.contains(bucketKey)
                    )
                }
            }
        }
    }

    // -----------------------------------------------------------------
    // Test 4: source fields are not exposed with percolation suffixes
    // -----------------------------------------------------------------

    /**
     * Verifies that the transformation suffix added for percolation
     * (e.g. `trigger_name_<index>_<monitorId>`) does not appear in `sample_documents._source`.
     *
     * This confirms that `_source` contains clean, untransformed field names.
     */
    fun `test doc-level monitor sample_documents source fields do not contain percolation suffixes`() {
        val sourceIndex = "test-alert-index-${randomAlphaOfLength(8).lowercase(Locale.ROOT)}"
        createAlertLikeIndex(sourceIndex)

        client().prepareIndex(sourceIndex).setId(UUIDs.randomBase64UUID())
            .setSource(alertLikeDoc(), XContentType.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get()

        val docQuery = DocLevelQuery(
            query = "trigger_name:\"Test Alert Rule\"",
            name = "test-query",
            fields = listOf()
        )

        // Render the full _source object as a string and check that field names
        // do not contain the percolation suffix format (_<indexName>_<monitorId>).
        val templateSource =
            "SOURCE={{#ctx.alerts}}{{#sample_documents}}{{_source}}{{/sample_documents}}{{/ctx.alerts}}"

        val action = randomAction(template = randomTemplateScript(templateSource))
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN, actions = listOf(action))
        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(DocLevelMonitorInput("test", listOf(sourceIndex), listOf(docQuery))),
            triggers = listOf(trigger)
        )

        val createdMonitor = createMonitor(monitor)
        assertNotNull(createdMonitor)
        val monitorId = createdMonitor!!.id

        val response = executeMonitor(createdMonitor.monitor, monitorId, dryRun = false)
        assertNotNull(response)

        val triggerResults = response!!.monitorRunResult.triggerResults
        assertTrue(triggerResults.isNotEmpty())

        triggerResults.values.filterIsInstance<DocumentLevelTriggerRunResult>().forEach { triggerResult ->
            triggerResult.actionResultsMap.values.forEach { actionResultMap: Map<String, ActionRunResult> ->
                actionResultMap.values.forEach { actionResult ->
                    val message = actionResult.output["message"] ?: ""
                    // percolation suffixes follow the pattern _<indexName>_<monitorId>
                    val suffixPattern = "_${sourceIndex}_"
                    assertFalse(
                        "Rendered source should not contain percolation suffix '$suffixPattern' but got: $message",
                        message.contains(suffixPattern)
                    )
                    assertTrue(
                        "Expected clean field 'trigger_name' in rendered source but got: $message",
                        message.contains("trigger_name=")
                    )
                    assertTrue(
                        "Expected clean field 'severity' in rendered source but got: $message",
                        message.contains("severity=")
                    )
                }
            }
        }
    }
}

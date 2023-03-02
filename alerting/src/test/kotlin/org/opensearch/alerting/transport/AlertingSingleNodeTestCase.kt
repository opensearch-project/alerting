/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest
import org.opensearch.action.admin.indices.get.GetIndexRequestBuilder
import org.opensearch.action.admin.indices.get.GetIndexResponse
import org.opensearch.action.admin.indices.refresh.RefreshAction
import org.opensearch.action.admin.indices.refresh.RefreshRequest
import org.opensearch.action.support.WriteRequest
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.action.ExecuteMonitorAction
import org.opensearch.alerting.action.ExecuteMonitorRequest
import org.opensearch.alerting.action.ExecuteMonitorResponse
import org.opensearch.alerting.action.GetMonitorAction
import org.opensearch.alerting.action.GetMonitorRequest
import org.opensearch.alerting.alerts.AlertIndices
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentType
import org.opensearch.common.xcontent.json.JsonXContent
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.action.DeleteMonitorRequest
import org.opensearch.commons.alerting.action.DeleteMonitorResponse
import org.opensearch.commons.alerting.action.GetFindingsRequest
import org.opensearch.commons.alerting.action.GetFindingsResponse
import org.opensearch.commons.alerting.action.IndexMonitorRequest
import org.opensearch.commons.alerting.action.IndexMonitorResponse
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.commons.alerting.model.Finding
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.Table
import org.opensearch.index.IndexService
import org.opensearch.index.query.TermQueryBuilder
import org.opensearch.index.reindex.ReindexPlugin
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.painless.PainlessPlugin
import org.opensearch.plugins.Plugin
import org.opensearch.rest.RestRequest
import org.opensearch.script.mustache.MustachePlugin
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.search.fetch.subphase.FetchSourceContext
import org.opensearch.test.OpenSearchSingleNodeTestCase
import java.time.Instant
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.*

/**
 * A test that keep a singleton node started for all tests that can be used to get
 * references to Guice injectors in unit tests.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
abstract class AlertingSingleNodeTestCase : OpenSearchSingleNodeTestCase() {

    protected val index: String = randomAlphaOfLength(10).lowercase(Locale.ROOT)

    override fun setUp() {
        super.setUp()
        createTestIndex()
    }

    protected fun getAllIndicesFromPattern(pattern: String): List<String> {
        val getIndexResponse = (
            client().admin().indices().prepareGetIndex()
                .setIndices(pattern) as GetIndexRequestBuilder
            ).get() as GetIndexResponse
        getIndexResponse
        return getIndexResponse.indices().toList()
    }

    protected fun executeMonitor(monitor: Monitor, id: String, dryRun: Boolean = true): ExecuteMonitorResponse? {
        val request = ExecuteMonitorRequest(dryRun, TimeValue(Instant.now().toEpochMilli()), id, monitor)
        return client().execute(ExecuteMonitorAction.INSTANCE, request).get()
    }

    protected fun insertSampleTimeSerializedData(index: String, data: List<String>) {
        data.forEachIndexed { i, value ->
            val twoMinsAgo = ZonedDateTime.now().minus(2, ChronoUnit.MINUTES).truncatedTo(ChronoUnit.MILLIS)
            val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(twoMinsAgo)
            val testDoc = """
                {
                  "test_strict_date_time": "$testTime",
                  "test_field_1": "$value",
                  "number": "$i"
                }
            """.trimIndent()
            // Indexing documents with deterministic doc id to allow for easy selected deletion during testing
            indexDoc(index, (i + 1).toString(), testDoc)
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun Map<String, Any>.stringMap(key: String): Map<String, Any>? {
        val map = this as Map<String, Map<String, Any>>
        return map[key]
    }

    /** A test index that can be used across tests. Feel free to add new fields but don't remove any. */
    protected fun createTestIndex() {
        val mapping = XContentFactory.jsonBuilder()
        mapping.startObject()
            .startObject("properties")
            .startObject("test_strict_date_time")
            .field("type", "date")
            .field("format", "strict_date_time")
            .endObject()
            .startObject("test_field_1")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()

        createIndex(
            index, Settings.EMPTY, mapping
        )
    }

    private fun createIndex(
        index: String?,
        settings: Settings?,
        mappings: XContentBuilder?,
    ): IndexService? {
        val createIndexRequestBuilder = client().admin().indices().prepareCreate(index).setSettings(settings)
        if (mappings != null) {
            createIndexRequestBuilder.setMapping(mappings)
        }
        return this.createIndex(index, createIndexRequestBuilder)
    }

    protected fun indexDoc(index: String, id: String, doc: String) {
        client().prepareIndex(index).setId(id)
            .setSource(doc, XContentType.JSON).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get()
    }

    protected fun createMonitor(monitor: Monitor): IndexMonitorResponse? {
        val request = IndexMonitorRequest(
            monitorId = Monitor.NO_ID,
            seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO,
            primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
            refreshPolicy = WriteRequest.RefreshPolicy.parse("true"),
            method = RestRequest.Method.POST,
            monitor = monitor
        )
        return client().execute(AlertingActions.INDEX_MONITOR_ACTION_TYPE, request).actionGet()
    }

    protected fun updateMonitor(monitor: Monitor, monitorId: String): IndexMonitorResponse? {
        val request = IndexMonitorRequest(
            monitorId = monitorId,
            seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO,
            primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
            refreshPolicy = WriteRequest.RefreshPolicy.parse("true"),
            method = RestRequest.Method.PUT,
            monitor = monitor
        )
        return client().execute(AlertingActions.INDEX_MONITOR_ACTION_TYPE, request).actionGet()
    }

    protected fun searchAlerts(id: String, indices: String = AlertIndices.ALERT_INDEX, refresh: Boolean = true): List<Alert> {
        try {
            if (refresh) refreshIndex(indices)
        } catch (e: Exception) {
            logger.warn("Could not refresh index $indices because: ${e.message}")
            return emptyList()
        }
        val ssb = SearchSourceBuilder()
        ssb.version(true)
        ssb.query(TermQueryBuilder(Alert.MONITOR_ID_FIELD, id))
        val searchResponse = client().prepareSearch(indices).setRouting(id).setSource(ssb).get()

        return searchResponse.hits.hits.map {
            val xcp = createParser(JsonXContent.jsonXContent, it.sourceRef).also { it.nextToken() }
            Alert.parse(xcp, it.id, it.version)
        }
    }

    protected fun refreshIndex(index: String) {
        client().execute(RefreshAction.INSTANCE, RefreshRequest(index)).get()
    }

    protected fun searchFindings(
        id: String,
        indices: String = AlertIndices.ALL_FINDING_INDEX_PATTERN,
        refresh: Boolean = true,
    ): List<Finding> {
        if (refresh) refreshIndex(indices)

        val ssb = SearchSourceBuilder()
        ssb.version(true)
        ssb.query(TermQueryBuilder(Alert.MONITOR_ID_FIELD, id))
        val searchResponse = client().prepareSearch(indices).setRouting(id).setSource(ssb).get()

        return searchResponse.hits.hits.map {
            val xcp = createParser(JsonXContent.jsonXContent, it.sourceRef).also { it.nextToken() }
            Finding.parse(xcp)
        }.filter { finding -> finding.monitorId == id }
    }

    protected fun getFindings(
        findingId: String,
        monitorId: String?,
        findingIndexName: String?,
    ): List<Finding> {

        val getFindingsRequest = GetFindingsRequest(
            findingId,
            Table("asc", "monitor_id", null, 100, 0, null),
            monitorId,
            findingIndexName
        )
        val getFindingsResponse: GetFindingsResponse = client().execute(AlertingActions.GET_FINDINGS_ACTION_TYPE, getFindingsRequest).get()

        return getFindingsResponse.findings.map { it.finding }.toList()
    }

    protected fun getMonitorResponse(
        monitorId: String,
        version: Long = 1L,
        fetchSourceContext: FetchSourceContext = FetchSourceContext.FETCH_SOURCE,
    ) = client().execute(
        GetMonitorAction.INSTANCE,
        GetMonitorRequest(monitorId, version, RestRequest.Method.GET, fetchSourceContext)
    ).get()

    protected fun deleteMonitor(monitorId: String): DeleteMonitorResponse = client().execute(
        AlertingActions.DELETE_MONITOR_ACTION_TYPE, DeleteMonitorRequest(monitorId, WriteRequest.RefreshPolicy.IMMEDIATE)
    ).get()

    protected fun deleteIndex(index: String) {
        val response = client().admin().indices().delete(DeleteIndexRequest(index)).get()
        assertTrue("Unable to delete index", response.isAcknowledged())
    }

    override fun getPlugins(): List<Class<out Plugin>> {
        return listOf(AlertingPlugin::class.java, ReindexPlugin::class.java, MustachePlugin::class.java, PainlessPlugin::class.java)
    }

    override fun resetNodeAfterTest(): Boolean {
        return false
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope
import org.opensearch.action.admin.indices.alias.get.GetAliasesRequest
import org.opensearch.action.admin.indices.get.GetIndexRequest
import org.opensearch.action.admin.indices.get.GetIndexRequestBuilder
import org.opensearch.action.admin.indices.get.GetIndexResponse
import org.opensearch.action.admin.indices.refresh.RefreshAction
import org.opensearch.action.admin.indices.refresh.RefreshRequest
import org.opensearch.action.support.IndicesOptions
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
import org.opensearch.common.xcontent.XContentType
import org.opensearch.common.xcontent.json.JsonXContent
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.action.DeleteMonitorRequest
import org.opensearch.commons.alerting.action.GetFindingsRequest
import org.opensearch.commons.alerting.action.GetFindingsResponse
import org.opensearch.commons.alerting.action.IndexMonitorRequest
import org.opensearch.commons.alerting.action.IndexMonitorResponse
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.commons.alerting.model.Finding
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.Table
import org.opensearch.index.query.TermQueryBuilder
import org.opensearch.index.reindex.ReindexPlugin
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.plugins.Plugin
import org.opensearch.rest.RestRequest
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.search.fetch.subphase.FetchSourceContext
import org.opensearch.test.OpenSearchSingleNodeTestCase
import java.time.Instant
import java.util.Locale

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

    /** A test index that can be used across tests. Feel free to add new fields but don't remove any. */
    protected fun createTestIndex() {
        createIndex(
            index, Settings.EMPTY,
            """
                "properties" : {
                  "test_strict_date_time" : { "type" : "date", "format" : "strict_date_time" },
                  "test_field" : { "type" : "keyword" }
                }
            """.trimIndent()
        )
    }

    protected fun indexDoc(index: String, id: String, doc: String) {
        client().prepareIndex(index).setId(id)
            .setSource(doc, XContentType.JSON).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get()
    }

    protected fun assertIndexExists(index: String) {
        val getIndexResponse =
            client().admin().indices().getIndex(
                GetIndexRequest().indices(index).indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_HIDDEN)
            ).get()
        assertTrue(getIndexResponse.indices.size > 0)
    }

    protected fun assertIndexNotExists(index: String) {
        val getIndexResponse =
            client().admin().indices().getIndex(
                GetIndexRequest().indices(index).indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_HIDDEN)
            ).get()
        assertFalse(getIndexResponse.indices.size > 0)
    }

    protected fun assertAliasNotExists(alias: String) {
        val aliasesResponse = client().admin().indices().getAliases(GetAliasesRequest()).get()
        val foundAlias = aliasesResponse.aliases.values().forEach {
            it.value.forEach {
                if (it.alias == alias) {
                    fail("alias exists, but it shouldn't")
                }
            }
        }
    }

    protected fun assertAliasExists(alias: String) {
        val aliasesResponse = client().admin().indices().getAliases(GetAliasesRequest()).get()
        val foundAlias = aliasesResponse.aliases.values().forEach {
            it.value.forEach {
                if (it.alias == alias) {
                    return
                }
            }
        }
        fail("alias doesn't exists, but it should")
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

    protected fun deleteMonitor(monitorId: String): Boolean {
        client().execute(
            AlertingActions.DELETE_MONITOR_ACTION_TYPE, DeleteMonitorRequest(monitorId, WriteRequest.RefreshPolicy.IMMEDIATE)
        ).get()
        return true
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
        refresh: Boolean = true
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
        fetchSourceContext: FetchSourceContext = FetchSourceContext.FETCH_SOURCE
    ) = client().execute(
        GetMonitorAction.INSTANCE,
        GetMonitorRequest(monitorId, version, RestRequest.Method.GET, fetchSourceContext)
    ).get()

    override fun getPlugins(): List<Class<out Plugin>> {
        return listOf(AlertingPlugin::class.java, ReindexPlugin::class.java)
    }

    override fun resetNodeAfterTest(): Boolean {
        return false
    }
}

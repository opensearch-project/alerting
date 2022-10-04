/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.junit.Assert
import org.opensearch.action.admin.cluster.state.ClusterStateRequest
import org.opensearch.action.admin.indices.create.CreateIndexRequest
import org.opensearch.action.search.SearchRequest
import org.opensearch.alerting.action.GetAlertsAction
import org.opensearch.alerting.action.GetAlertsRequest
import org.opensearch.alerting.action.SearchMonitorAction
import org.opensearch.alerting.action.SearchMonitorRequest
import org.opensearch.alerting.core.ScheduledJobIndices
import org.opensearch.alerting.core.model.DocLevelMonitorInput
import org.opensearch.alerting.core.model.DocLevelQuery
import org.opensearch.alerting.core.model.ScheduledJob.Companion.SCHEDULED_JOBS_INDEX
import org.opensearch.alerting.model.DataSources
import org.opensearch.alerting.model.Table
import org.opensearch.alerting.transport.AlertingSingleNodeTestCase
import org.opensearch.common.settings.Settings
import org.opensearch.index.query.MatchQueryBuilder
import org.opensearch.test.OpenSearchTestCase
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit.MILLIS
import java.util.concurrent.TimeUnit

class MonitorDataSourcesIT : AlertingSingleNodeTestCase() {

    fun `test execute monitor with dryrun`() {
        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3")
        val docLevelInput = DocLevelMonitorInput("description", listOf(index), listOf(docQuery))
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        var monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
        )
        val monitorResponse = createMonitor(monitor)
        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""
        assertFalse(monitorResponse?.id.isNullOrEmpty())
        monitor = monitorResponse!!.monitor
        Assert.assertEquals(monitor.owner, "alerting")
        indexDoc(index, "1", testDoc)
        val id = monitorResponse.id
        val executeMonitorResponse = executeMonitor(monitor, id, true)
        Assert.assertEquals(executeMonitorResponse!!.monitorRunResult.monitorName, monitor.name)
        Assert.assertEquals(executeMonitorResponse.monitorRunResult.triggerResults.size, 1)
        searchAlerts(id)
        val table = Table("asc", "id", null, 1, 0, "")
        var getAlertsResponse = client()
            .execute(GetAlertsAction.INSTANCE, GetAlertsRequest(table, "ALL", "ALL", null, null))
            .get()
        Assert.assertTrue(getAlertsResponse != null)
        Assert.assertTrue(getAlertsResponse.alerts.size == 0)
        getAlertsResponse = client()
            .execute(GetAlertsAction.INSTANCE, GetAlertsRequest(table, "ALL", "ALL", id, null))
            .get()
        Assert.assertTrue(getAlertsResponse != null)
        Assert.assertTrue(getAlertsResponse.alerts.size == 0)
        try {
            client()
                .execute(GetAlertsAction.INSTANCE, GetAlertsRequest(table, "ALL", "ALL", null, "wrong_alert_index"))
                .get()
            fail()
        } catch (e: Exception) {
            Assert.assertTrue(e.message!!.contains("IndexNotFoundException"))
        }
    }

    fun `test execute monitor with custom alerts index`() {
        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3")
        val docLevelInput = DocLevelMonitorInput("description", listOf(index), listOf(docQuery))
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val customAlertsIndex = "custom_alerts_index"
        var monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
            dataSources = DataSources(alertsIndex = customAlertsIndex)
        )
        val monitorResponse = createMonitor(monitor)
        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""
        assertFalse(monitorResponse?.id.isNullOrEmpty())
        monitor = monitorResponse!!.monitor
        indexDoc(index, "1", testDoc)
        val id = monitorResponse.id
        val executeMonitorResponse = executeMonitor(monitor, id, false)
        Assert.assertEquals(executeMonitorResponse!!.monitorRunResult.monitorName, monitor.name)
        Assert.assertEquals(executeMonitorResponse.monitorRunResult.triggerResults.size, 1)
        val alerts = searchAlerts(id, customAlertsIndex)
        assertEquals("Alert saved for test monitor", 1, alerts.size)
        val table = Table("asc", "id", null, 1, 0, "")
        var getAlertsResponse = client()
            .execute(GetAlertsAction.INSTANCE, GetAlertsRequest(table, "ALL", "ALL", null, customAlertsIndex))
            .get()
        Assert.assertTrue(getAlertsResponse != null)
        Assert.assertTrue(getAlertsResponse.alerts.size == 1)
        getAlertsResponse = client()
            .execute(GetAlertsAction.INSTANCE, GetAlertsRequest(table, "ALL", "ALL", id, null))
            .get()
        Assert.assertTrue(getAlertsResponse != null)
        Assert.assertTrue(getAlertsResponse.alerts.size == 1)
    }

    fun `test execute monitor with owner field`() {
        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3")
        val docLevelInput = DocLevelMonitorInput("description", listOf(index), listOf(docQuery))
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val customAlertsIndex = "custom_alerts_index"
        var monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
            dataSources = DataSources(alertsIndex = customAlertsIndex),
            owner = "owner"
        )
        val monitorResponse = createMonitor(monitor)
        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""
        assertFalse(monitorResponse?.id.isNullOrEmpty())
        monitor = monitorResponse!!.monitor
        Assert.assertEquals(monitor.owner, "owner")
        indexDoc(index, "1", testDoc)
        val id = monitorResponse.id
        val executeMonitorResponse = executeMonitor(monitor, id, false)
        Assert.assertEquals(executeMonitorResponse!!.monitorRunResult.monitorName, monitor.name)
        Assert.assertEquals(executeMonitorResponse.monitorRunResult.triggerResults.size, 1)
        val alerts = searchAlerts(id, customAlertsIndex)
        assertEquals("Alert saved for test monitor", 1, alerts.size)
        val table = Table("asc", "id", null, 1, 0, "")
        var getAlertsResponse = client()
            .execute(GetAlertsAction.INSTANCE, GetAlertsRequest(table, "ALL", "ALL", null, customAlertsIndex))
            .get()
        Assert.assertTrue(getAlertsResponse != null)
        Assert.assertTrue(getAlertsResponse.alerts.size == 1)
        getAlertsResponse = client()
            .execute(GetAlertsAction.INSTANCE, GetAlertsRequest(table, "ALL", "ALL", id, null))
            .get()
        Assert.assertTrue(getAlertsResponse != null)
        Assert.assertTrue(getAlertsResponse.alerts.size == 1)
    }

    fun `test execute monitor with custom query index`() {
        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3")
        val docLevelInput = DocLevelMonitorInput("description", listOf(index), listOf(docQuery))
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val customQueryIndex = "custom_alerts_index"
        var monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
            dataSources = DataSources(queryIndex = customQueryIndex)
        )
        val monitorResponse = createMonitor(monitor)
        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""
        assertFalse(monitorResponse?.id.isNullOrEmpty())
        monitor = monitorResponse!!.monitor
        indexDoc(index, "1", testDoc)
        val id = monitorResponse.id
        val executeMonitorResponse = executeMonitor(monitor, id, false)
        Assert.assertEquals(executeMonitorResponse!!.monitorRunResult.monitorName, monitor.name)
        Assert.assertEquals(executeMonitorResponse.monitorRunResult.triggerResults.size, 1)
        searchAlerts(id)
        val table = Table("asc", "id", null, 1, 0, "")
        var getAlertsResponse = client()
            .execute(GetAlertsAction.INSTANCE, GetAlertsRequest(table, "ALL", "ALL", null, null))
            .get()
        Assert.assertTrue(getAlertsResponse != null)
        Assert.assertTrue(getAlertsResponse.alerts.size == 1)
        getAlertsResponse = client()
            .execute(GetAlertsAction.INSTANCE, GetAlertsRequest(table, "ALL", "ALL", id, null))
            .get()
        Assert.assertTrue(getAlertsResponse != null)
        Assert.assertTrue(getAlertsResponse.alerts.size == 1)
        var queryIndexSearchResponse = client().search(SearchRequest(customQueryIndex)).get()
        Assert.assertNotNull(queryIndexSearchResponse)
        Assert.assertTrue(queryIndexSearchResponse.hits.hits.size > 0)
        deleteMonitor(id)
        val docDeletedChecker: () -> Boolean = {
            queryIndexSearchResponse = client().search(SearchRequest(customQueryIndex)).get()
            Assert.assertNotNull(queryIndexSearchResponse)
            queryIndexSearchResponse.hits.hits.isEmpty()
        }
        OpenSearchTestCase.waitUntil(docDeletedChecker, 5, TimeUnit.SECONDS)
    }

    fun `test execute monitor with custom query index and custom field mappings`() {
        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3")
        val docLevelInput = DocLevelMonitorInput("description", listOf(index), listOf(docQuery))
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val customQueryIndex = "custom_alerts_index"
        val analyzer = "whitespace"
        var monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
            dataSources = DataSources(
                queryIndex = customQueryIndex,
                queryIndexMappingsByType = mapOf(Pair("text", mapOf(Pair("analyzer", analyzer)))),
            )
        )
        val monitorResponse = createMonitor(monitor)
        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""
        assertFalse(monitorResponse?.id.isNullOrEmpty())
        monitor = monitorResponse!!.monitor
        indexDoc(index, "1", testDoc)
        val id = monitorResponse.id
        val executeMonitorResponse = executeMonitor(monitor, id, false)
        Assert.assertEquals(executeMonitorResponse!!.monitorRunResult.monitorName, monitor.name)
        Assert.assertEquals(executeMonitorResponse.monitorRunResult.triggerResults.size, 1)
        searchAlerts(id)
        val clusterStateResponse = client().admin().cluster().state(ClusterStateRequest().indices(customQueryIndex).metadata(true)).get()
        val mapping = clusterStateResponse.state.metadata.index(customQueryIndex).mapping()
        Assert.assertTrue(mapping?.source()?.string()?.contains("\"analyzer\":\"$analyzer\"") == true)
        val table = Table("asc", "id", null, 1, 0, "")
        var getAlertsResponse = client()
            .execute(GetAlertsAction.INSTANCE, GetAlertsRequest(table, "ALL", "ALL", null, null))
            .get()
        Assert.assertTrue(getAlertsResponse != null)
        Assert.assertTrue(getAlertsResponse.alerts.size == 1)
        getAlertsResponse = client()
            .execute(GetAlertsAction.INSTANCE, GetAlertsRequest(table, "ALL", "ALL", id, null))
            .get()
        Assert.assertTrue(getAlertsResponse != null)
        Assert.assertTrue(getAlertsResponse.alerts.size == 1)
    }

    fun `test execute monitor with custom findings index`() {
        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3")
        val docLevelInput = DocLevelMonitorInput("description", listOf(index), listOf(docQuery))
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val customFindingsIndex = "custom_findings_index"
        var monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
            dataSources = DataSources(findingsIndex = customFindingsIndex)
        )
        val monitorResponse = createMonitor(monitor)
        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""
        assertFalse(monitorResponse?.id.isNullOrEmpty())
        monitor = monitorResponse!!.monitor
        indexDoc(index, "1", testDoc)
        val id = monitorResponse.id
        val executeMonitorResponse = executeMonitor(monitor, id, false)
        Assert.assertEquals(executeMonitorResponse!!.monitorRunResult.monitorName, monitor.name)
        Assert.assertEquals(executeMonitorResponse.monitorRunResult.triggerResults.size, 1)
        searchAlerts(id)
        val findings = searchFindings(id, customFindingsIndex)
        assertEquals("Findings saved for test monitor", 1, findings.size)
        assertTrue("Findings saved for test monitor", findings[0].relatedDocIds.contains("1"))
        val table = Table("asc", "id", null, 1, 0, "")
        var getAlertsResponse = client()
            .execute(GetAlertsAction.INSTANCE, GetAlertsRequest(table, "ALL", "ALL", null, null))
            .get()
        Assert.assertTrue(getAlertsResponse != null)
        Assert.assertTrue(getAlertsResponse.alerts.size == 1)
        getAlertsResponse = client()
            .execute(GetAlertsAction.INSTANCE, GetAlertsRequest(table, "ALL", "ALL", id, null))
            .get()
        Assert.assertTrue(getAlertsResponse != null)
        Assert.assertTrue(getAlertsResponse.alerts.size == 1)
    }

    fun `test execute GetFindingsAction with monitorId param`() {
        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3")
        val docLevelInput = DocLevelMonitorInput("description", listOf(index), listOf(docQuery))
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val customFindingsIndex = "custom_findings_index"
        var monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
            dataSources = DataSources(findingsIndex = customFindingsIndex)
        )
        val monitorResponse = createMonitor(monitor)
        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""
        assertFalse(monitorResponse?.id.isNullOrEmpty())
        monitor = monitorResponse!!.monitor
        indexDoc(index, "1", testDoc)
        val monitorId = monitorResponse.id
        val executeMonitorResponse = executeMonitor(monitor, monitorId, false)
        Assert.assertEquals(executeMonitorResponse!!.monitorRunResult.monitorName, monitor.name)
        Assert.assertEquals(executeMonitorResponse.monitorRunResult.triggerResults.size, 1)
        searchAlerts(monitorId)
        val findings = searchFindings(monitorId, customFindingsIndex)
        assertEquals("Findings saved for test monitor", 1, findings.size)
        assertTrue("Findings saved for test monitor", findings[0].relatedDocIds.contains("1"))
        // fetch findings - pass monitorId as reference to finding_index
        val findingsFromAPI = getFindings(findings.get(0).id, monitorId, null)
        assertEquals(
            "Findings mismatch between manually searched and fetched via GetFindingsAction",
            findings.get(0).id,
            findingsFromAPI.get(0).id
        )
    }

    fun `test execute GetFindingsAction with params monitorId and findingIndex`() {
        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3")
        val docLevelInput = DocLevelMonitorInput("description", listOf(index), listOf(docQuery))
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val customFindingsIndex = "custom_findings_index"
        var monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
            dataSources = DataSources(findingsIndex = customFindingsIndex)
        )
        val monitorResponse = createMonitor(monitor)
        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""
        assertFalse(monitorResponse?.id.isNullOrEmpty())
        monitor = monitorResponse!!.monitor
        indexDoc(index, "1", testDoc)
        val monitorId = monitorResponse.id
        val executeMonitorResponse = executeMonitor(monitor, monitorId, false)
        Assert.assertEquals(executeMonitorResponse!!.monitorRunResult.monitorName, monitor.name)
        Assert.assertEquals(executeMonitorResponse.monitorRunResult.triggerResults.size, 1)
        searchAlerts(monitorId)
        val findings = searchFindings(monitorId, customFindingsIndex)
        assertEquals("Findings saved for test monitor", 1, findings.size)
        assertTrue("Findings saved for test monitor", findings[0].relatedDocIds.contains("1"))
        // fetch findings - pass both monitorId and findingIndexName name. Monitor id should be ignored
        val findingsFromAPI = getFindings(findings.get(0).id, "incorrect_monitor_id", customFindingsIndex)
        assertEquals(
            "Findings mismatch between manually searched and fetched via GetFindingsAction",
            findings.get(0).id,
            findingsFromAPI.get(0).id
        )
    }

    fun `test execute GetFindingsAction with unknown monitorId`() {
        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3")
        val docLevelInput = DocLevelMonitorInput("description", listOf(index), listOf(docQuery))
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val customFindingsIndex = "custom_findings_index"
        var monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
            dataSources = DataSources(findingsIndex = customFindingsIndex)
        )
        val monitorResponse = createMonitor(monitor)
        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""
        assertFalse(monitorResponse?.id.isNullOrEmpty())
        monitor = monitorResponse!!.monitor
        indexDoc(index, "1", testDoc)
        val monitorId = monitorResponse.id
        val executeMonitorResponse = executeMonitor(monitor, monitorId, false)
        Assert.assertEquals(executeMonitorResponse!!.monitorRunResult.monitorName, monitor.name)
        Assert.assertEquals(executeMonitorResponse.monitorRunResult.triggerResults.size, 1)
        searchAlerts(monitorId)
        val findings = searchFindings(monitorId, customFindingsIndex)
        assertEquals("Findings saved for test monitor", 1, findings.size)
        assertTrue("Findings saved for test monitor", findings[0].relatedDocIds.contains("1"))
        // fetch findings - don't send monitorId or findingIndexName. It should fall back to hardcoded finding index name
        try {
            getFindings(findings.get(0).id, "unknown_monitor_id_123456789", null)
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning GetMonitor Action error ",
                    it.contains("Monitor not found")
                )
            }
        }
    }

    fun `test execute GetFindingsAction with unknown findingIndex param`() {
        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3")
        val docLevelInput = DocLevelMonitorInput("description", listOf(index), listOf(docQuery))
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val customFindingsIndex = "custom_findings_index"
        var monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
            dataSources = DataSources(findingsIndex = customFindingsIndex)
        )
        val monitorResponse = createMonitor(monitor)
        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""
        assertFalse(monitorResponse?.id.isNullOrEmpty())
        monitor = monitorResponse!!.monitor
        indexDoc(index, "1", testDoc)
        val monitorId = monitorResponse.id
        val executeMonitorResponse = executeMonitor(monitor, monitorId, false)
        Assert.assertEquals(executeMonitorResponse!!.monitorRunResult.monitorName, monitor.name)
        Assert.assertEquals(executeMonitorResponse.monitorRunResult.triggerResults.size, 1)
        searchAlerts(monitorId)
        val findings = searchFindings(monitorId, customFindingsIndex)
        assertEquals("Findings saved for test monitor", 1, findings.size)
        assertTrue("Findings saved for test monitor", findings[0].relatedDocIds.contains("1"))
        // fetch findings - don't send monitorId or findingIndexName. It should fall back to hardcoded finding index name
        try {
            getFindings(findings.get(0).id, null, "unknown_finding_index_123456789")
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning GetMonitor Action error ",
                    it.contains("no such index")
                )
            }
        }
    }

    fun `test execute pre-existing monitorand update`() {
        val request = CreateIndexRequest(SCHEDULED_JOBS_INDEX).mapping(ScheduledJobIndices.scheduledJobMappings())
            .settings(Settings.builder().put("index.hidden", true).build())
        client().admin().indices().create(request)
        val monitorStringWithoutName = """
        {
        "monitor": {
        "type": "monitor",
        "schema_version": 0,
        "name": "UayEuXpZtb",
        "monitor_type": "doc_level_monitor",
        "user": {
        "name": "",
        "backend_roles": [],
        "roles": [],
        "custom_attribute_names": [],
        "user_requested_tenant": null
        },
        "enabled": true,
        "enabled_time": 1662753436791,
        "schedule": {
        "period": {
        "interval": 5,
        "unit": "MINUTES"
        }
        },
        "inputs": [{
        "doc_level_input": {
        "description": "description",
        "indices": [
        "$index"
        ],
        "queries": [{
        "id": "63efdcce-b5a1-49f4-a25f-6b5f9496a755",
        "name": "3",
        "query": "test_field:\"us-west-2\"",
        "tags": []
        }]
        }
        }],
        "triggers": [{
        "document_level_trigger": {
        "id": "OGnTI4MBv6qt0ATc9Phk",
        "name": "mrbHRMevYI",
        "severity": "1",
        "condition": {
        "script": {
        "source": "return true",
        "lang": "painless"
        }
        },
        "actions": []
        }
        }],
        "last_update_time": 1662753436791
        }
        }
        """.trimIndent()
        val monitorId = "abc"
        indexDoc(SCHEDULED_JOBS_INDEX, monitorId, monitorStringWithoutName)
        val getMonitorResponse = getMonitorResponse(monitorId)
        Assert.assertNotNull(getMonitorResponse)
        Assert.assertNotNull(getMonitorResponse.monitor)
        val monitor = getMonitorResponse.monitor
        val sr = SearchRequest(SCHEDULED_JOBS_INDEX)
        val g =
            client().execute(SearchMonitorAction.INSTANCE, SearchMonitorRequest(sr))
                .get()
        Assert.assertNotNull(g)
        Assert.assertEquals(g.hits.hits.size, 1)
        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""
        indexDoc(index, "1", testDoc)
        var executeMonitorResponse = executeMonitor(monitor!!, monitorId, false)
        Assert.assertNotNull(executeMonitorResponse)
        if (executeMonitorResponse != null) {
            Assert.assertNotNull(executeMonitorResponse.monitorRunResult.monitorName)
        }
        val alerts = searchAlerts(monitorId)
        assertEquals(alerts.size, 1)

        val customAlertsIndex = "custom_alerts_index"
        val customQueryIndex = "custom_query_index"
        val customFindingsIndex = "custom_findings_index"
        val updateMonitorResponse = updateMonitor(
            monitor.copy(
                id = monitorId,
                owner = "security_analytics_plugin",
                dataSources = DataSources(
                    alertsIndex = customAlertsIndex,
                    queryIndex = customQueryIndex,
                    findingsIndex = customFindingsIndex
                )
            ),
            monitorId
        )
        Assert.assertNotNull(updateMonitorResponse)
        Assert.assertEquals(updateMonitorResponse!!.monitor.owner, "security_analytics_plugin")
        indexDoc(index, "2", testDoc)
        if (updateMonitorResponse != null) {
            executeMonitorResponse = executeMonitor(updateMonitorResponse.monitor, monitorId, false)
        }
        val findings = searchFindings(monitorId, customFindingsIndex)
        assertEquals("Findings saved for test monitor", 1, findings.size)
        assertTrue("Findings saved for test monitor", findings[0].relatedDocIds.contains("2"))
        val customAlertsIndexAlerts = searchAlerts(monitorId, customAlertsIndex)
        assertEquals("Alert saved for test monitor", 1, customAlertsIndexAlerts.size)
        val table = Table("asc", "id", null, 1, 0, "")
        var getAlertsResponse = client()
            .execute(GetAlertsAction.INSTANCE, GetAlertsRequest(table, "ALL", "ALL", null, customAlertsIndex))
            .get()
        Assert.assertTrue(getAlertsResponse != null)
        Assert.assertTrue(getAlertsResponse.alerts.size == 1)
        getAlertsResponse = client()
            .execute(GetAlertsAction.INSTANCE, GetAlertsRequest(table, "ALL", "ALL", monitorId, null))
            .get()
        Assert.assertTrue(getAlertsResponse != null)
        Assert.assertTrue(getAlertsResponse.alerts.size == 1)

        val searchRequest = SearchRequest(SCHEDULED_JOBS_INDEX)
        var searchMonitorResponse =
            client().execute(SearchMonitorAction.INSTANCE, SearchMonitorRequest(searchRequest))
                .get()
        Assert.assertEquals(searchMonitorResponse.hits.hits.size, 0)
        searchRequest.source().query(MatchQueryBuilder("monitor.owner", "security_analytics_plugin"))
        searchMonitorResponse =
            client().execute(SearchMonitorAction.INSTANCE, SearchMonitorRequest(searchRequest))
                .get()
        Assert.assertEquals(searchMonitorResponse.hits.hits.size, 1)
    }
}

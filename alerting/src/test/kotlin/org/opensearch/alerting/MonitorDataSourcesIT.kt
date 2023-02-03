/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.junit.Assert
import org.opensearch.action.admin.indices.create.CreateIndexRequest
import org.opensearch.action.admin.indices.get.GetIndexRequest
import org.opensearch.action.admin.indices.get.GetIndexResponse
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.support.WriteRequest
import org.opensearch.alerting.action.DeleteMonitorAction
import org.opensearch.alerting.action.DeleteMonitorRequest
import org.opensearch.alerting.action.GetAlertsAction
import org.opensearch.alerting.action.GetAlertsRequest
import org.opensearch.alerting.alerts.AlertIndices
import org.opensearch.alerting.core.ScheduledJobIndices
import org.opensearch.alerting.core.model.DocLevelMonitorInput
import org.opensearch.alerting.core.model.DocLevelQuery
import org.opensearch.alerting.core.model.ScheduledJob
import org.opensearch.alerting.core.model.ScheduledJob.Companion.DOC_LEVEL_QUERIES_INDEX
import org.opensearch.alerting.core.model.ScheduledJob.Companion.SCHEDULED_JOBS_INDEX
import org.opensearch.alerting.model.DocumentLevelTriggerRunResult
import org.opensearch.alerting.model.Table
import org.opensearch.alerting.transport.AlertingSingleNodeTestCase
import org.opensearch.alerting.util.DocLevelMonitorQueries
import org.opensearch.common.settings.Settings
import org.opensearch.index.mapper.MapperService
import org.opensearch.index.query.QueryBuilders
import org.opensearch.search.builder.SearchSourceBuilder
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit.MILLIS

/**
 * For 2.3 this was backported and some of the tests are not consistent
 * due to missing features launched in newer versions
 */
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
        indexDoc(index, "1", testDoc)
        val id = monitorResponse.id
        val executeMonitorResponse = executeMonitor(monitor, id, true)
        Assert.assertEquals(executeMonitorResponse!!.monitorRunResult.monitorName, monitor.name)
        Assert.assertEquals(executeMonitorResponse.monitorRunResult.triggerResults.size, 1)
        searchAlerts(id)
        val table = Table("asc", "id", null, 1, 0, "")
        var getAlertsResponse = client()
            .execute(GetAlertsAction.INSTANCE, GetAlertsRequest(table, "ALL", "ALL", null))
            .get()
        Assert.assertTrue(getAlertsResponse != null)
        Assert.assertTrue(getAlertsResponse.alerts.size == 0)
        getAlertsResponse = client()
            .execute(GetAlertsAction.INSTANCE, GetAlertsRequest(table, "ALL", "ALL", id))
            .get()
        Assert.assertTrue(getAlertsResponse != null)
        Assert.assertTrue(getAlertsResponse.alerts.size == 0)
    }

    fun `test execute monitor without create when no monitors exists`() {
        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3")
        val docLevelInput = DocLevelMonitorInput("description", listOf(index), listOf(docQuery))
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        var monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )
        var executeMonitorResponse = executeMonitor(monitor, null)
        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""

        assertIndexNotExists(SCHEDULED_JOBS_INDEX)

        val createMonitorResponse = createMonitor(monitor)

        assertIndexExists(SCHEDULED_JOBS_INDEX)

        indexDoc(index, "1", testDoc)

        executeMonitorResponse = executeMonitor(monitor, createMonitorResponse?.id, dryRun = false)

        Assert.assertEquals(executeMonitorResponse!!.monitorRunResult.monitorName, monitor.name)
        Assert.assertEquals(executeMonitorResponse.monitorRunResult.triggerResults.size, 1)
        Assert.assertEquals(
            (executeMonitorResponse.monitorRunResult.triggerResults.iterator().next().value as DocumentLevelTriggerRunResult)
                .triggeredDocs.size,
            1
        )
    }

    fun `test delete monitor deletes all queries and metadata too`() {
        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3")
        val docLevelInput = DocLevelMonitorInput("description", listOf(index), listOf(docQuery))
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        var monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
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
        // Verify queries exist
        var searchResponse = client().search(
            SearchRequest(DOC_LEVEL_QUERIES_INDEX).source(SearchSourceBuilder().query(QueryBuilders.matchAllQuery()))
        ).get()
        assertNotEquals(0, searchResponse.hits.hits.size)
    }

    fun `test execute pre-existing monitor and update`() {
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

        val updateMonitorResponse = updateMonitor(
            monitor.copy(id = monitorId),
            monitorId
        )
        Assert.assertNotNull(updateMonitorResponse)
        indexDoc(index, "2", testDoc)
        executeMonitorResponse = executeMonitor(updateMonitorResponse!!.monitor, monitorId, false)
        val table = Table("asc", "id", null, 1, 0, "")
        var getAlertsResponse = client()
            .execute(GetAlertsAction.INSTANCE, GetAlertsRequest(table, "ALL", "ALL", null))
            .get()
        Assert.assertTrue(getAlertsResponse != null)
        Assert.assertTrue(getAlertsResponse.alerts.size == 1)
        getAlertsResponse = client()
            .execute(GetAlertsAction.INSTANCE, GetAlertsRequest(table, "ALL", "ALL", monitorId))
            .get()
        Assert.assertTrue(getAlertsResponse != null)
        Assert.assertTrue(getAlertsResponse.alerts.size == 1)
    }

    fun `test get alerts by list of monitors containing both existent and non-existent ids`() {
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

        monitor = monitorResponse!!.monitor

        val id = monitorResponse.id

        var monitor1 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
        )
        val monitorResponse1 = createMonitor(monitor1)
        monitor1 = monitorResponse1!!.monitor
        val id1 = monitorResponse1.id
        indexDoc(index, "1", testDoc)
        executeMonitor(monitor1, id1, false)
        executeMonitor(monitor, id, false)
        val alerts = searchAlerts(id)
        assertEquals("Alert saved for test monitor", 1, alerts.size)
        val alerts1 = searchAlerts(id)
        assertEquals("Alert saved for test monitor", 1, alerts1.size)
        val table = Table("asc", "id", null, 1000, 0, "")
        var getAlertsResponse = client()
            .execute(
                GetAlertsAction.INSTANCE,
                GetAlertsRequest(table, "ALL", "ALL", null)
            )
            .get()

        Assert.assertTrue(getAlertsResponse != null)
        Assert.assertTrue(getAlertsResponse.alerts.size == 2)

        var alertsResponseForRequestWithoutCustomIndex = client()
            .execute(
                GetAlertsAction.INSTANCE,
                GetAlertsRequest(table, "ALL", "ALL", null)
            )
            .get()
        Assert.assertTrue(alertsResponseForRequestWithoutCustomIndex != null)
        Assert.assertTrue(alertsResponseForRequestWithoutCustomIndex.alerts.size == 2)
        var getAlertsByAlertIds = client()
            .execute(
                GetAlertsAction.INSTANCE,
                GetAlertsRequest(table, "ALL", "ALL", null)
            )
            .get()
        Assert.assertTrue(getAlertsByAlertIds != null)
        Assert.assertTrue(getAlertsByAlertIds.alerts.size == 2)

        var getAlertsByWrongAlertIds = client()
            .execute(
                GetAlertsAction.INSTANCE,
                GetAlertsRequest(table, "ALL", "ALL", null)
            )
            .get()

        Assert.assertTrue(getAlertsByWrongAlertIds != null)
    }

    fun `test queryIndex rollover and delete monitor success`() {

        val testSourceIndex = "test_source_index"
        createIndex(testSourceIndex, Settings.builder().put("index.mapping.total_fields.limit", "10000").build())

        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3")
        val docLevelInput = DocLevelMonitorInput("description", listOf(testSourceIndex), listOf(docQuery))
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        var monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )
        // This doc should create close to 1000 (limit) fields in index mapping. It's easier to add mappings like this then via api
        val docPayload: StringBuilder = StringBuilder(100000)
        docPayload.append("{")
        for (i in 1..3300) {
            docPayload.append(""" "id$i.somefield.somefield$i":$i,""")
        }
        docPayload.append("\"test_field\" : \"us-west-2\" }")
        indexDoc(testSourceIndex, "1", docPayload.toString())
        // Create monitor #1
        var monitorResponse = createMonitor(monitor)
        assertFalse(monitorResponse?.id.isNullOrEmpty())
        monitor = monitorResponse!!.monitor
        // Execute monitor #1
        var executeMonitorResponse = executeMonitor(monitor, monitorResponse.id, false)
        Assert.assertEquals(executeMonitorResponse!!.monitorRunResult.monitorName, monitor.name)
        Assert.assertEquals(executeMonitorResponse.monitorRunResult.triggerResults.size, 1)
        // Create monitor #2
        var monitorResponse2 = createMonitor(monitor)
        assertFalse(monitorResponse2?.id.isNullOrEmpty())
        monitor = monitorResponse2!!.monitor
        // Insert doc #2. This one should trigger creation of alerts during monitor exec
        val testDoc = """{
            "test_field" : "us-west-2"
        }"""
        indexDoc(testSourceIndex, "2", testDoc)
        // Execute monitor #2
        var executeMonitorResponse2 = executeMonitor(monitor, monitorResponse2.id, false)
        Assert.assertEquals(executeMonitorResponse2!!.monitorRunResult.monitorName, monitor.name)
        Assert.assertEquals(executeMonitorResponse2.monitorRunResult.triggerResults.size, 1)

        refreshIndex(AlertIndices.ALERT_INDEX)
        var alerts = searchAlerts(monitorResponse2.id)
        Assert.assertTrue(alerts != null)
        Assert.assertTrue(alerts.size == 1)

        // Both monitors used same queryIndex alias. Since source index has close to limit amount of fields in mappings,
        // we expect that creation of second monitor would trigger rollover of queryIndex
        var getIndexResponse: GetIndexResponse =
            client().admin().indices().getIndex(GetIndexRequest().indices(ScheduledJob.DOC_LEVEL_QUERIES_INDEX + "*")).get()
        assertEquals(2, getIndexResponse.indices.size)
        assertEquals(DOC_LEVEL_QUERIES_INDEX + "-000001", getIndexResponse.indices[0])
        assertEquals(DOC_LEVEL_QUERIES_INDEX + "-000002", getIndexResponse.indices[1])
        // Now we'll verify that execution of both monitors still works
        indexDoc(testSourceIndex, "3", testDoc)
        // Exec Monitor #1
        executeMonitorResponse = executeMonitor(monitor, monitorResponse.id, false)
        Assert.assertEquals(executeMonitorResponse!!.monitorRunResult.monitorName, monitor.name)
        Assert.assertEquals(executeMonitorResponse.monitorRunResult.triggerResults.size, 1)
        refreshIndex(AlertIndices.ALERT_INDEX)
        alerts = searchAlerts(monitorResponse.id)
        Assert.assertTrue(alerts != null)
        Assert.assertTrue(alerts.size == 2)
        // Exec Monitor #2
        executeMonitorResponse = executeMonitor(monitor, monitorResponse2.id, false)
        Assert.assertEquals(executeMonitorResponse!!.monitorRunResult.monitorName, monitor.name)
        Assert.assertEquals(executeMonitorResponse.monitorRunResult.triggerResults.size, 1)
        refreshIndex(AlertIndices.ALERT_INDEX)
        alerts = searchAlerts(monitorResponse2.id)
        Assert.assertTrue(alerts != null)
        Assert.assertTrue(alerts.size == 2)
        // Delete monitor #1
        client().execute(
            DeleteMonitorAction.INSTANCE, DeleteMonitorRequest(monitorResponse.id, WriteRequest.RefreshPolicy.IMMEDIATE)
        ).get()
        // Expect first concrete queryIndex to be deleted since that one was only used by this monitor
        getIndexResponse =
            client().admin().indices().getIndex(GetIndexRequest().indices(ScheduledJob.DOC_LEVEL_QUERIES_INDEX + "*")).get()
        assertEquals(1, getIndexResponse.indices.size)
        assertEquals(ScheduledJob.DOC_LEVEL_QUERIES_INDEX + "-000002", getIndexResponse.indices[0])
        // Delete monitor #2
        client().execute(
            DeleteMonitorAction.INSTANCE, DeleteMonitorRequest(monitorResponse2.id, WriteRequest.RefreshPolicy.IMMEDIATE)
        ).get()
        // Expect second concrete queryIndex to be deleted since that one was only used by this monitor
        getIndexResponse =
            client().admin().indices().getIndex(GetIndexRequest().indices(ScheduledJob.DOC_LEVEL_QUERIES_INDEX + "*")).get()
        assertEquals(0, getIndexResponse.indices.size)
    }

    fun `test queryIndex rollover failure source_index field count over limit`() {

        val testSourceIndex = "test_source_index"
        createIndex(testSourceIndex, Settings.EMPTY)

        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3")
        val docLevelInput = DocLevelMonitorInput("description", listOf(testSourceIndex), listOf(docQuery))
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        var monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )
        // This doc should create 999 fields in mapping, only 1 field less then limit
        val docPayload: StringBuilder = StringBuilder(100000)
        docPayload.append("{")
        for (i in 1..998) {
            docPayload.append(""" "id$i":$i,""")
        }
        docPayload.append("\"test_field\" : \"us-west-2\" }")
        indexDoc(testSourceIndex, "1", docPayload.toString())
        // Create monitor and expect failure.
        // queryIndex has 3 fields in mappings initially so 999 + 3 > 1000(default limit)
        try {
            createMonitor(monitor)
        } catch (e: Exception) {
            assertTrue(e.message?.contains("can't process index [$testSourceIndex] due to field mapping limit") ?: false)
        }
    }

    fun `test queryIndex not rolling over multiple monitors`() {
        val testSourceIndex = "test_source_index"
        createIndex(testSourceIndex, Settings.EMPTY)

        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3")
        val docLevelInput = DocLevelMonitorInput("description", listOf(testSourceIndex), listOf(docQuery))
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        var monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )
        // Create doc with 11 fields
        val docPayload: StringBuilder = StringBuilder(1000)
        docPayload.append("{")
        for (i in 1..10) {
            docPayload.append(""" "id$i":$i,""")
        }
        docPayload.append("\"test_field\" : \"us-west-2\" }")
        indexDoc(testSourceIndex, "1", docPayload.toString())
        // Create monitor #1
        var monitorResponse = createMonitor(monitor)
        assertFalse(monitorResponse?.id.isNullOrEmpty())
        monitor = monitorResponse!!.monitor
        // Execute monitor #1
        var executeMonitorResponse = executeMonitor(monitor, monitorResponse.id, false)
        Assert.assertEquals(executeMonitorResponse!!.monitorRunResult.monitorName, monitor.name)
        Assert.assertEquals(executeMonitorResponse.monitorRunResult.triggerResults.size, 1)
        // Create monitor #2
        var monitorResponse2 = createMonitor(monitor)
        assertFalse(monitorResponse2?.id.isNullOrEmpty())
        monitor = monitorResponse2!!.monitor
        // Insert doc #2. This one should trigger creation of alerts during monitor exec
        val testDoc = """{
            "test_field" : "us-west-2"
        }"""
        indexDoc(testSourceIndex, "2", testDoc)
        // Execute monitor #2
        var executeMonitorResponse2 = executeMonitor(monitor, monitorResponse2.id, false)
        Assert.assertEquals(executeMonitorResponse2!!.monitorRunResult.monitorName, monitor.name)
        Assert.assertEquals(executeMonitorResponse2.monitorRunResult.triggerResults.size, 1)

        refreshIndex(AlertIndices.ALERT_INDEX)
        var alerts = searchAlerts(monitorResponse2.id)
        Assert.assertTrue(alerts != null)
        Assert.assertTrue(alerts.size == 1)

        // Both monitors used same queryIndex. Since source index has well below limit amount of fields in mappings,
        // we expect only 1 backing queryIndex
        val getIndexResponse: GetIndexResponse =
            client().admin().indices().getIndex(GetIndexRequest().indices(ScheduledJob.DOC_LEVEL_QUERIES_INDEX + "*")).get()
        assertEquals(1, getIndexResponse.indices.size)
        // Now we'll verify that execution of both monitors work
        indexDoc(testSourceIndex, "3", testDoc)
        // Exec Monitor #1
        executeMonitorResponse = executeMonitor(monitor, monitorResponse.id, false)
        Assert.assertEquals(executeMonitorResponse!!.monitorRunResult.monitorName, monitor.name)
        Assert.assertEquals(executeMonitorResponse.monitorRunResult.triggerResults.size, 1)
        refreshIndex(AlertIndices.ALERT_INDEX)
        alerts = searchAlerts(monitorResponse.id)
        Assert.assertTrue(alerts != null)
        Assert.assertTrue(alerts.size == 2)
        // Exec Monitor #2
        executeMonitorResponse = executeMonitor(monitor, monitorResponse2.id, false)
        Assert.assertEquals(executeMonitorResponse!!.monitorRunResult.monitorName, monitor.name)
        Assert.assertEquals(executeMonitorResponse.monitorRunResult.triggerResults.size, 1)
        refreshIndex(AlertIndices.ALERT_INDEX)
        alerts = searchAlerts(monitorResponse2.id)
        Assert.assertTrue(alerts != null)
        Assert.assertTrue(alerts.size == 2)
    }

    /**
     * 1. Create monitor with input source_index with 9000 fields in mappings - can fit 1 in queryIndex
     * 2. Update monitor and change input source_index to a new one with 9000 fields in mappings
     * 3. Expect queryIndex rollover resulting in 2 backing indices
     * 4. Delete monitor and expect that all backing indices are deleted
     * */
    fun `test updating monitor no execution queryIndex rolling over`() {
        val testSourceIndex1 = "test_source_index1"
        val testSourceIndex2 = "test_source_index2"
        createIndex(testSourceIndex1, Settings.builder().put("index.mapping.total_fields.limit", "10000").build())
        createIndex(testSourceIndex2, Settings.builder().put("index.mapping.total_fields.limit", "10000").build())
        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3")
        val docLevelInput = DocLevelMonitorInput("description", listOf(testSourceIndex1), listOf(docQuery))
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        var monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )
        // This doc should create close to 10000 (limit) fields in index mapping. It's easier to add mappings like this then via api
        val docPayload: StringBuilder = StringBuilder(100000)
        docPayload.append("{")
        for (i in 1..9000) {
            docPayload.append(""" "id$i":$i,""")
        }
        docPayload.append("\"test_field\" : \"us-west-2\" }")
        // Indexing docs here as an easier means to set index mappings
        indexDoc(testSourceIndex1, "1", docPayload.toString())
        indexDoc(testSourceIndex2, "1", docPayload.toString())
        // Create monitor
        var monitorResponse = createMonitor(monitor)
        assertFalse(monitorResponse?.id.isNullOrEmpty())
        monitor = monitorResponse!!.monitor

        // Update monitor and change input
        val updatedMonitor = monitor.copy(
            inputs = listOf(
                DocLevelMonitorInput("description", listOf(testSourceIndex2), listOf(docQuery))
            )
        )
        updateMonitor(updatedMonitor, updatedMonitor.id)
        assertFalse(monitorResponse?.id.isNullOrEmpty())

        // Expect queryIndex to rollover after setting new source_index with close to limit amount of fields in mappings
        var getIndexResponse: GetIndexResponse =
            client().admin().indices().getIndex(GetIndexRequest().indices(ScheduledJob.DOC_LEVEL_QUERIES_INDEX + "*")).get()
        assertEquals(2, getIndexResponse.indices.size)

        deleteMonitor(updatedMonitor.id)
        waitUntil {
            getIndexResponse =
                client().admin().indices().getIndex(GetIndexRequest().indices(ScheduledJob.DOC_LEVEL_QUERIES_INDEX + "*")).get()
            return@waitUntil getIndexResponse.indices.isEmpty()
        }
        assertEquals(0, getIndexResponse.indices.size)
    }

    fun `test queryIndex gets increased max fields in mappings`() {
        val testSourceIndex = "test_source_index"
        createIndex(testSourceIndex, Settings.builder().put("index.mapping.total_fields.limit", "10000").build())
        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3")
        val docLevelInput = DocLevelMonitorInput("description", listOf(testSourceIndex), listOf(docQuery))
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        var monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )
        // This doc should create 12000 fields in index mapping. It's easier to add mappings like this then via api
        val docPayload: StringBuilder = StringBuilder(100000)
        docPayload.append("{")
        for (i in 1..9998) {
            docPayload.append(""" "id$i":$i,""")
        }
        docPayload.append("\"test_field\" : \"us-west-2\" }")
        // Indexing docs here as an easier means to set index mappings
        indexDoc(testSourceIndex, "1", docPayload.toString())
        // Create monitor
        var monitorResponse = createMonitor(monitor)
        assertFalse(monitorResponse?.id.isNullOrEmpty())
        monitor = monitorResponse!!.monitor

        // Expect queryIndex to rollover after setting new source_index with close to limit amount of fields in mappings
        var getIndexResponse: GetIndexResponse =
            client().admin().indices().getIndex(GetIndexRequest().indices(ScheduledJob.DOC_LEVEL_QUERIES_INDEX + "*")).get()
        assertEquals(1, getIndexResponse.indices.size)
        val field_max_limit = getIndexResponse
            .getSetting(DOC_LEVEL_QUERIES_INDEX + "-000001", MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.key).toInt()

        assertEquals(10000 + DocLevelMonitorQueries.QUERY_INDEX_BASE_FIELDS_COUNT, field_max_limit)

        deleteMonitor(monitorResponse.id)
        waitUntil {
            getIndexResponse =
                client().admin().indices().getIndex(GetIndexRequest().indices(ScheduledJob.DOC_LEVEL_QUERIES_INDEX + "*")).get()
            return@waitUntil getIndexResponse.indices.isEmpty()
        }
        assertEquals(0, getIndexResponse.indices.size)
    }

    fun `test queryIndex bwc when index was not an alias`() {
        createIndex(DOC_LEVEL_QUERIES_INDEX, Settings.builder().put("index.hidden", true).build())
        assertIndexExists(DOC_LEVEL_QUERIES_INDEX)

        val testSourceIndex = "test_source_index"
        createIndex(testSourceIndex, Settings.EMPTY)

        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3")
        val docLevelInput = DocLevelMonitorInput("description", listOf(testSourceIndex), listOf(docQuery))
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        var monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )
        // This doc should create 999 fields in mapping, only 1 field less then limit
        val docPayload = "{\"test_field\" : \"us-west-2\" }"
        // Create monitor
        try {
            var monitorResponse = createMonitor(monitor)
            indexDoc(testSourceIndex, "1", docPayload)
            var executeMonitorResponse = executeMonitor(monitor, monitorResponse!!.id, false)
            Assert.assertEquals(executeMonitorResponse!!.monitorRunResult.monitorName, monitor.name)
            Assert.assertEquals(executeMonitorResponse.monitorRunResult.triggerResults.size, 1)
            refreshIndex(AlertIndices.ALERT_INDEX)
            val alerts = searchAlerts(monitorResponse.id)
            Assert.assertTrue(alerts != null)
            Assert.assertTrue(alerts.size == 1)
            // check if DOC_LEVEL_QUERIES_INDEX alias exists
            assertAliasExists(DOC_LEVEL_QUERIES_INDEX)
        } catch (e: Exception) {
            fail("Exception happend but it shouldn't!")
        }
    }
}

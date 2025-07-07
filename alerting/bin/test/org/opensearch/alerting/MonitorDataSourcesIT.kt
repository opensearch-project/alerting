/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.junit.Assert
import org.opensearch.action.DocWriteRequest
import org.opensearch.action.admin.cluster.state.ClusterStateRequest
import org.opensearch.action.admin.indices.alias.Alias
import org.opensearch.action.admin.indices.close.CloseIndexRequest
import org.opensearch.action.admin.indices.create.CreateIndexRequest
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest
import org.opensearch.action.admin.indices.get.GetIndexRequest
import org.opensearch.action.admin.indices.get.GetIndexResponse
import org.opensearch.action.admin.indices.mapping.get.GetMappingsRequest
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest
import org.opensearch.action.admin.indices.open.OpenIndexRequest
import org.opensearch.action.admin.indices.refresh.RefreshRequest
import org.opensearch.action.bulk.BulkRequest
import org.opensearch.action.bulk.BulkResponse
import org.opensearch.action.fieldcaps.FieldCapabilitiesRequest
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.support.WriteRequest
import org.opensearch.alerting.alerts.AlertIndices
import org.opensearch.alerting.core.ScheduledJobIndices
import org.opensearch.alerting.model.DocumentLevelTriggerRunResult
import org.opensearch.alerting.model.WorkflowMetadata
import org.opensearch.alerting.transport.AlertingSingleNodeTestCase
import org.opensearch.alerting.util.AlertingException
import org.opensearch.alerting.util.DocLevelMonitorQueries
import org.opensearch.alerting.util.DocLevelMonitorQueries.Companion.INDEX_PATTERN_SUFFIX
import org.opensearch.alerting.workflow.CompositeWorkflowRunner
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.action.AcknowledgeAlertRequest
import org.opensearch.commons.alerting.action.AcknowledgeAlertResponse
import org.opensearch.commons.alerting.action.AcknowledgeChainedAlertRequest
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.action.DeleteMonitorRequest
import org.opensearch.commons.alerting.action.GetAlertsRequest
import org.opensearch.commons.alerting.action.GetAlertsResponse
import org.opensearch.commons.alerting.action.IndexMonitorResponse
import org.opensearch.commons.alerting.action.SearchMonitorRequest
import org.opensearch.commons.alerting.aggregation.bucketselectorext.BucketSelectorExtAggregationBuilder
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.commons.alerting.model.ChainedAlertTrigger
import org.opensearch.commons.alerting.model.ChainedMonitorFindings
import org.opensearch.commons.alerting.model.CompositeInput
import org.opensearch.commons.alerting.model.DataSources
import org.opensearch.commons.alerting.model.Delegate
import org.opensearch.commons.alerting.model.DocLevelMonitorInput
import org.opensearch.commons.alerting.model.DocLevelQuery
import org.opensearch.commons.alerting.model.IntervalSchedule
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.alerting.model.ScheduledJob.Companion.DOC_LEVEL_QUERIES_INDEX
import org.opensearch.commons.alerting.model.ScheduledJob.Companion.SCHEDULED_JOBS_INDEX
import org.opensearch.commons.alerting.model.SearchInput
import org.opensearch.commons.alerting.model.Table
import org.opensearch.commons.alerting.model.Workflow
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParserUtils
import org.opensearch.index.mapper.MapperService
import org.opensearch.index.query.MatchQueryBuilder
import org.opensearch.index.query.QueryBuilders
import org.opensearch.index.query.TermQueryBuilder
import org.opensearch.rest.RestRequest
import org.opensearch.script.Script
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder
import org.opensearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.test.OpenSearchTestCase
import java.time.Instant
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.temporal.ChronoUnit.MILLIS
import java.util.Collections
import java.util.Map
import java.util.UUID
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import java.util.stream.Collectors

class MonitorDataSourcesIT : AlertingSingleNodeTestCase() {

    fun `test execute monitor with dryrun`() {
        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3", fields = listOf())
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
            .execute(AlertingActions.GET_ALERTS_ACTION_TYPE, GetAlertsRequest(table, "ALL", "ALL", null, null))
            .get()
        Assert.assertTrue(getAlertsResponse != null)
        Assert.assertTrue(getAlertsResponse.alerts.size == 0)
        getAlertsResponse = client()
            .execute(AlertingActions.GET_ALERTS_ACTION_TYPE, GetAlertsRequest(table, "ALL", "ALL", id, null))
            .get()
        Assert.assertTrue(getAlertsResponse != null)
        Assert.assertTrue(getAlertsResponse.alerts.size == 0)
        try {
            client()
                .execute(AlertingActions.GET_ALERTS_ACTION_TYPE, GetAlertsRequest(table, "ALL", "ALL", null, "wrong_alert_index"))
                .get()
            fail()
        } catch (e: Exception) {
            Assert.assertTrue(e.message!!.contains("IndexNotFoundException"))
        }
    }

    fun `test execute monitor with custom alerts index`() {
        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3", fields = listOf())
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
            .execute(AlertingActions.GET_ALERTS_ACTION_TYPE, GetAlertsRequest(table, "ALL", "ALL", null, customAlertsIndex))
            .get()
        Assert.assertTrue(getAlertsResponse != null)
        Assert.assertTrue(getAlertsResponse.alerts.size == 1)
        getAlertsResponse = client()
            .execute(AlertingActions.GET_ALERTS_ACTION_TYPE, GetAlertsRequest(table, "ALL", "ALL", id, null))
            .get()
        Assert.assertTrue(getAlertsResponse != null)
        Assert.assertTrue(getAlertsResponse.alerts.size == 1)
        val alertId = getAlertsResponse.alerts.get(0).id
        val acknowledgeAlertResponse = client().execute(
            AlertingActions.ACKNOWLEDGE_ALERTS_ACTION_TYPE,
            AcknowledgeAlertRequest(id, listOf(alertId), WriteRequest.RefreshPolicy.IMMEDIATE)
        ).get()
        Assert.assertEquals(acknowledgeAlertResponse.acknowledged.size, 1)
    }

    fun `test mappings parsing`() {

        val index1 = "index_123"
        val index2 = "index_456"
        val index3 = "index_789"
        val index4 = "index_012"
        val q1 = DocLevelQuery(query = "properties:\"abcd\"", name = "1", fields = listOf())
        val q2 = DocLevelQuery(query = "type.properties:\"abcd\"", name = "2", fields = listOf())
        val q3 = DocLevelQuery(query = "type.something.properties:\"abcd\"", name = "3", fields = listOf())
        val q4 = DocLevelQuery(query = "type.something.properties.lastone:\"abcd\"", name = "4", fields = listOf())

        createIndex(index1, Settings.EMPTY)
        createIndex(index2, Settings.EMPTY)
        createIndex(index3, Settings.EMPTY)
        createIndex(index4, Settings.EMPTY)

        val m1 = """{
                "properties": {
                  "properties": {
                    "type": "keyword"
                  }
                }
        }
        """.trimIndent()
        client().admin().indices().putMapping(PutMappingRequest(index1).source(m1, XContentType.JSON)).get()

        val m2 = """{
                "properties": {
                  "type": {
                    "properties": {
                      "properties": { "type": "keyword" }
                    }
                  }
                }
        }
        """.trimIndent()
        client().admin().indices().putMapping(PutMappingRequest(index2).source(m2, XContentType.JSON)).get()

        val m3 = """{
                "properties": {
                  "type": {
                    "properties": {
                      "something": {
                        "properties" : {
                          "properties": { "type": "keyword" }
                        }
                      }
                    }
                  }
                }
        }
        """.trimIndent()
        client().admin().indices().putMapping(PutMappingRequest(index3).source(m3, XContentType.JSON)).get()

        val m4 = """{
                "properties": {
                  "type": {
                    "properties": {
                      "something": {
                        "properties" : {
                          "properties": { 
                            "properties": {
                              "lastone": { "type": "keyword" }
                            }
                          }
                        }
                      }
                    }
                  }
                }
        }
        """.trimIndent()
        client().admin().indices().putMapping(PutMappingRequest(index4).source(m4, XContentType.JSON)).get()

        val docLevelInput = DocLevelMonitorInput(
            "description",
            listOf(index1, index2, index3, index4),
            listOf(q1, q2, q3, q4)
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val customFindingsIndex = "custom_findings_index"
        val customFindingsIndexPattern = "custom_findings_index-1"
        val customQueryIndex = "custom_alerts_index"
        var monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
            dataSources = DataSources(
                queryIndex = customQueryIndex,
                findingsIndex = customFindingsIndex,
                findingsIndexPattern = customFindingsIndexPattern
            )
        )
        val monitorResponse = createMonitor(monitor)

        val testDoc1 = """{
            "properties": "abcd"
        }"""
        indexDoc(index1, "1", testDoc1)
        val testDoc2 = """{
            "type.properties": "abcd"
        }"""
        indexDoc(index2, "1", testDoc2)
        val testDoc3 = """{
            "type.something.properties": "abcd"
        }"""
        indexDoc(index3, "1", testDoc3)
        val testDoc4 = """{
            "type.something.properties.lastone": "abcd"
        }"""
        indexDoc(index4, "1", testDoc4)

        assertFalse(monitorResponse?.id.isNullOrEmpty())
        monitor = monitorResponse!!.monitor
        val id = monitorResponse.id
        val executeMonitorResponse = executeMonitor(monitor, id, false)
        Assert.assertEquals(executeMonitorResponse!!.monitorRunResult.monitorName, monitor.name)
        Assert.assertEquals(executeMonitorResponse.monitorRunResult.triggerResults.size, 1)
        searchAlerts(id)
        val table = Table("asc", "id", null, 1, 0, "")
        var getAlertsResponse = client()
            .execute(AlertingActions.GET_ALERTS_ACTION_TYPE, GetAlertsRequest(table, "ALL", "ALL", null, null))
            .get()
        Assert.assertTrue(getAlertsResponse != null)
        Assert.assertTrue(getAlertsResponse.alerts.size == 1)
        val findings = searchFindings(id, customFindingsIndex)
        assertEquals("Findings saved for test monitor", 4, findings.size)
    }

    fun `test execute monitor without triggers`() {
        val docQuery = DocLevelQuery(query = "eventType:\"login\"", name = "3", fields = listOf())

        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(docQuery)
        )
        val customFindingsIndex = "custom_findings_index"
        val customFindingsIndexPattern = "custom_findings_index-1"
        val customQueryIndex = "custom_alerts_index"
        var monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(),
            dataSources = DataSources(
                queryIndex = customQueryIndex,
                findingsIndex = customFindingsIndex,
                findingsIndexPattern = customFindingsIndexPattern
            )
        )
        val monitorResponse = createMonitor(monitor)
        assertFalse(monitorResponse?.id.isNullOrEmpty())

        val testDoc = """{
            "eventType" : "login"
        }"""
        indexDoc(index, "1", testDoc)

        monitor = monitorResponse!!.monitor
        val id = monitorResponse.id
        // Execute dry run first and expect no alerts or findings
        var executeMonitorResponse = executeMonitor(monitor, id, true)
        Assert.assertEquals(executeMonitorResponse!!.monitorRunResult.monitorName, monitor.name)
        Assert.assertEquals(executeMonitorResponse.monitorRunResult.triggerResults.size, 0)
        searchAlerts(id)
        var table = Table("asc", "id", null, 1, 0, "")
        var getAlertsResponse = client()
            .execute(AlertingActions.GET_ALERTS_ACTION_TYPE, GetAlertsRequest(table, "ALL", "ALL", null, null))
            .get()
        Assert.assertTrue(getAlertsResponse != null)
        Assert.assertTrue(getAlertsResponse.alerts.isEmpty())
        var findings = searchFindings(id, customFindingsIndex)
        assertEquals("Findings saved for test monitor", 0, findings.size)

        // Execute real run - expect findings, but no alerts
        executeMonitorResponse = executeMonitor(monitor, id, false)

        searchAlerts(id)
        table = Table("asc", "id", null, 1, 0, "")
        getAlertsResponse = client()
            .execute(AlertingActions.GET_ALERTS_ACTION_TYPE, GetAlertsRequest(table, "ALL", "ALL", null, null))
            .get()
        Assert.assertTrue(getAlertsResponse != null)
        Assert.assertTrue(getAlertsResponse.alerts.isEmpty())

        findings = searchFindings(id, customFindingsIndex)
        assertEquals("Findings saved for test monitor", 1, findings.size)
        assertTrue("Findings saved for test monitor", findings[0].relatedDocIds.contains("1"))
        assertEquals("Didn't match query", 1, findings[0].docLevelQueries.size)
    }

    fun `test execute monitor with custom query index`() {
        val q1 = DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3", fields = listOf())
        val q2 = DocLevelQuery(query = "source.ip.v6.v2:16645", name = "4", fields = listOf())
        val q3 = DocLevelQuery(query = "source.ip.v4.v0:120", name = "5", fields = listOf())
        val q4 = DocLevelQuery(query = "alias.some.fff:\"us-west-2\"", name = "6", fields = listOf())
        val q5 = DocLevelQuery(query = "message:\"This is an error from IAD region\"", name = "7", fields = listOf())
        val q6 = DocLevelQuery(query = "f1.type.f4:\"hello\"", name = "8", fields = listOf())
        val q7 = DocLevelQuery(query = "f1.type.f2.f3:\"world\"", name = "9", fields = listOf())
        val q8 = DocLevelQuery(query = "type:\"some type\"", name = "10", fields = listOf())
        val q9 = DocLevelQuery(query = "properties:123", name = "11", fields = listOf())

        val docLevelInput = DocLevelMonitorInput(
            "description",
            listOf(index),
            listOf(q1, q2, q3, q4, q5, q6, q7, q8, q9)
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val customFindingsIndex = "custom_findings_index"
        val customFindingsIndexPattern = "custom_findings_index-1"
        val customQueryIndex = "custom_alerts_index"
        var monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
            dataSources = DataSources(
                queryIndex = customQueryIndex,
                findingsIndex = customFindingsIndex,
                findingsIndexPattern = customFindingsIndexPattern
            )
        )
        val monitorResponse = createMonitor(monitor)
        // Trying to test here few different "nesting" situations and "wierd" characters
        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "source.ip.v6.v1" : 12345,
            "source.ip.v6.v2" : 16645,
            "source.ip.v4.v0" : 120,
            "test_bad_char" : "\u0000", 
            "test_strict_date_time" : "$testTime",
            "test_field.some_other_field" : "us-west-2",
            "f1.type.f2.f3" : "world",
            "f1.type.f4" : "hello",
            "type" : "some type",
            "properties": 123
        }"""
        indexDoc(index, "1", testDoc)
        client().admin().indices().putMapping(
            PutMappingRequest(index).source("alias.some.fff", "type=alias,path=test_field.some_other_field")
        )
        val mappings = "{\"properties\":{\"type\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\"," +
            "\"ignore_above\":256}}},\"query\":{\"type\":\"text\"}}}"
        val mappingsResp = client().admin().indices().putMapping(
            PutMappingRequest(index).source(mappings, XContentType.JSON)
        ).get()
        assertFalse(monitorResponse?.id.isNullOrEmpty())
        monitor = monitorResponse!!.monitor
        val id = monitorResponse.id
        val executeMonitorResponse = executeMonitor(monitor, id, false)
        Assert.assertEquals(executeMonitorResponse!!.monitorRunResult.monitorName, monitor.name)
        Assert.assertEquals(executeMonitorResponse.monitorRunResult.triggerResults.size, 1)
        searchAlerts(id)
        val table = Table("asc", "id", null, 1, 0, "")
        var getAlertsResponse = client()
            .execute(AlertingActions.GET_ALERTS_ACTION_TYPE, GetAlertsRequest(table, "ALL", "ALL", null, null))
            .get()
        Assert.assertTrue(getAlertsResponse != null)
        Assert.assertTrue(getAlertsResponse.alerts.size == 1)
        val findings = searchFindings(id, customFindingsIndex)
        assertEquals("Findings saved for test monitor", 1, findings.size)
        assertTrue("Findings saved for test monitor", findings[0].relatedDocIds.contains("1"))
        assertEquals("Didn't match all 9 queries", 9, findings[0].docLevelQueries.size)
    }

    fun `test execute monitor with non-flattened json doc as source`() {
        val docQuery1 = DocLevelQuery(query = "source.device.port:12345 OR source.device.hwd.id:12345", name = "3", fields = listOf())

        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(docQuery1)
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val customFindingsIndex = "custom_findings_index"
        val customFindingsIndexPattern = "custom_findings_index-1"
        val customQueryIndex = "custom_alerts_index"
        var monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
            dataSources = DataSources(
                queryIndex = customQueryIndex,
                findingsIndex = customFindingsIndex,
                findingsIndexPattern = customFindingsIndexPattern
            )
        )
        val monitorResponse = createMonitor(monitor)

        val mappings = """{
            "properties": {
                "source.device.port": { "type": "long" },
                "source.device.hwd.id": { "type": "long" },
                "nested_field": {
                  "type": "nested",
                  "properties": {
                    "test1": {
                      "type": "keyword"
                    }
                  }
                },
                "my_join_field": { 
                  "type": "join",
                  "relations": {
                     "question": "answer" 
                  }
               }
            }
        }"""

        client().admin().indices().putMapping(PutMappingRequest(index).source(mappings, XContentType.JSON)).get()
        val getFieldCapabilitiesResp = client().fieldCaps(FieldCapabilitiesRequest().indices(index).fields("*")).get()
        assertTrue(getFieldCapabilitiesResp.getField("source").containsKey("object"))
        assertTrue(getFieldCapabilitiesResp.getField("source.device").containsKey("object"))
        assertTrue(getFieldCapabilitiesResp.getField("source.device.hwd").containsKey("object"))
        // testing both, nested and flatten documents
        val testDocuments = mutableListOf<String>()
        testDocuments += """{
            "source" : { "device": {"port" : 12345 } },
            "nested_field": { "test1": "some text" }
        }"""
        testDocuments += """{
            "source.device.port" : "12345"
        }"""
        testDocuments += """{
            "source.device.port" : 12345
        }"""
        testDocuments += """{
            "source" : { "device": {"hwd": { "id": 12345 } } }
        }"""
        testDocuments += """{
            "source.device.hwd.id" : 12345
        }"""
        // Document with join field
        testDocuments += """{
            "source" : { "device" : { "hwd": { "id" : 12345 } } },
            "my_join_field": { "name": "question" }
        }"""
        // Checking if these pointless but valid documents cause any issues
        testDocuments += """{
            "source" : {}
        }"""
        testDocuments += """{
            "source.device" : null
        }"""
        testDocuments += """{
            "source.device" : {}
        }"""
        testDocuments += """{
            "source.device.hwd" : {}
        }"""
        testDocuments += """{
            "source.device.hwd.id" : null
        }"""
        testDocuments += """{
            "some.multi.val.field" : [12345, 10, 11]
        }"""
        // Insert all documents
        for (i in testDocuments.indices) {
            indexDoc(index, "$i", testDocuments[i])
        }
        assertFalse(monitorResponse?.id.isNullOrEmpty())
        monitor = monitorResponse!!.monitor
        val id = monitorResponse.id
        val executeMonitorResponse = executeMonitor(monitor, id, false)
        Assert.assertEquals(executeMonitorResponse!!.monitorRunResult.monitorName, monitor.name)
        Assert.assertEquals(executeMonitorResponse.monitorRunResult.triggerResults.size, 1)
        searchAlerts(id)
        val table = Table("asc", "id", null, 1, 0, "")
        var getAlertsResponse = client()
            .execute(AlertingActions.GET_ALERTS_ACTION_TYPE, GetAlertsRequest(table, "ALL", "ALL", null, null))
            .get()
        Assert.assertTrue(getAlertsResponse != null)
        Assert.assertTrue(getAlertsResponse.alerts.size == 1)
        val findings = searchFindings(id, customFindingsIndex)
        assertEquals("Findings saved for test monitor", 6, findings.size)
        assertEquals("Didn't match query", 1, findings[0].docLevelQueries.size)
    }

    fun `test execute monitor with custom query index old`() {
        val docQuery1 = DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3", fields = listOf())
        val docQuery2 = DocLevelQuery(query = "source.ip.v6.v2:16645", name = "4", fields = listOf())
        val docQuery3 = DocLevelQuery(query = "source.ip.v4.v0:120", name = "5", fields = listOf())
        val docQuery4 = DocLevelQuery(query = "alias.some.fff:\"us-west-2\"", name = "6", fields = listOf())
        val docQuery5 = DocLevelQuery(query = "message:\"This is an error from IAD region\"", name = "7", fields = listOf())
        val docQuery6 = DocLevelQuery(query = "type.subtype:\"some subtype\"", name = "8", fields = listOf())
        val docQuery7 = DocLevelQuery(query = "supertype.type:\"some type\"", name = "9", fields = listOf())
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(docQuery1, docQuery2, docQuery3, docQuery4, docQuery5, docQuery6, docQuery7)
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val customFindingsIndex = "custom_findings_index"
        val customFindingsIndexPattern = "custom_findings_index-1"
        val customQueryIndex = "custom_alerts_index"
        var monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
            dataSources = DataSources(
                queryIndex = customQueryIndex,
                findingsIndex = customFindingsIndex,
                findingsIndexPattern = customFindingsIndexPattern
            )
        )
        val monitorResponse = createMonitor(monitor)
        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        // Trying to test here few different "nesting" situations and "wierd" characters
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "source.ip.v6.v1" : 12345,
            "source.ip.v6.v2" : 16645,
            "source.ip.v4.v0" : 120,
            "test_bad_char" : "\u0000", 
            "test_strict_date_time" : "$testTime",
            "test_field.some_other_field" : "us-west-2",
            "type.subtype" : "some subtype",
            "supertype.type" : "some type"
        }"""
        indexDoc(index, "1", testDoc)
        client().admin().indices().putMapping(
            PutMappingRequest(index).source("alias.some.fff", "type=alias,path=test_field.some_other_field")
        )
        assertFalse(monitorResponse?.id.isNullOrEmpty())
        monitor = monitorResponse!!.monitor
        val id = monitorResponse.id
        val executeMonitorResponse = executeMonitor(monitor, id, false)
        Assert.assertEquals(executeMonitorResponse!!.monitorRunResult.monitorName, monitor.name)
        Assert.assertEquals(executeMonitorResponse.monitorRunResult.triggerResults.size, 1)
        searchAlerts(id)
        val table = Table("asc", "id", null, 1, 0, "")
        var getAlertsResponse = client()
            .execute(AlertingActions.GET_ALERTS_ACTION_TYPE, GetAlertsRequest(table, "ALL", "ALL", null, null))
            .get()
        Assert.assertTrue(getAlertsResponse != null)
        Assert.assertTrue(getAlertsResponse.alerts.size == 1)
        val findings = searchFindings(id, customFindingsIndex)
        assertEquals("Findings saved for test monitor", 1, findings.size)
        assertTrue("Findings saved for test monitor", findings[0].relatedDocIds.contains("1"))
        assertEquals("Didn't match all 7 queries", 7, findings[0].docLevelQueries.size)
    }

    fun `test monitor error alert created and updated with new error`() {
        val docQuery = DocLevelQuery(query = "source:12345", name = "1", fields = listOf())
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(docQuery)
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        var monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )

        val testDoc = """{
            "message" : "This is an error from IAD region"
        }"""

        val monitorResponse = createMonitor(monitor)
        assertFalse(monitorResponse?.id.isNullOrEmpty())

        monitor = monitorResponse!!.monitor
        val id = monitorResponse.id

        // Close index to force error alert
        client().admin().indices().close(CloseIndexRequest(index)).get()

        var executeMonitorResponse = executeMonitor(monitor, id, false)
        Assert.assertEquals(executeMonitorResponse!!.monitorRunResult.monitorName, monitor.name)
        Assert.assertEquals(executeMonitorResponse.monitorRunResult.triggerResults.size, 0)
        searchAlerts(id)
        var table = Table("asc", "id", null, 1, 0, "")
        var getAlertsResponse = client()
            .execute(AlertingActions.GET_ALERTS_ACTION_TYPE, GetAlertsRequest(table, "ALL", "ALL", null, null))
            .get()
        Assert.assertTrue(getAlertsResponse != null)
        Assert.assertTrue(getAlertsResponse.alerts.size == 1)
        Assert.assertTrue(getAlertsResponse.alerts[0].errorMessage == "IndexClosedException[closed]")
        // Reopen index
        client().admin().indices().open(OpenIndexRequest(index)).get()
        // Close queryIndex
        client().admin().indices().close(CloseIndexRequest(DOC_LEVEL_QUERIES_INDEX + INDEX_PATTERN_SUFFIX)).get()

        indexDoc(index, "1", testDoc)

        executeMonitorResponse = executeMonitor(monitor, id, false)
        Assert.assertEquals(executeMonitorResponse!!.monitorRunResult.monitorName, monitor.name)
        Assert.assertEquals(executeMonitorResponse.monitorRunResult.triggerResults.size, 0)
        searchAlerts(id)
        table = Table("asc", "id", null, 10, 0, "")
        getAlertsResponse = client()
            .execute(AlertingActions.GET_ALERTS_ACTION_TYPE, GetAlertsRequest(table, "ALL", "ALL", null, null))
            .get()
        Assert.assertTrue(getAlertsResponse != null)
        Assert.assertTrue(getAlertsResponse.alerts.size == 1)
        Assert.assertTrue(getAlertsResponse.alerts[0].errorHistory[0].message == "IndexClosedException[closed]")
        Assert.assertEquals(1, getAlertsResponse.alerts[0].errorHistory.size)
        Assert.assertTrue(getAlertsResponse.alerts[0].errorMessage!!.contains("Failed to run percolate search"))
    }

    fun `test monitor error alert created trigger run errored 2 times same error`() {
        val docQuery = DocLevelQuery(query = "source:12345", name = "1", fields = listOf())
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(docQuery)
        )
        val trigger = randomDocumentLevelTrigger(condition = Script("invalid script code"))
        var monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )

        val monitorResponse = createMonitor(monitor)
        assertFalse(monitorResponse?.id.isNullOrEmpty())

        monitor = monitorResponse!!.monitor
        val id = monitorResponse.id

        var executeMonitorResponse = executeMonitor(monitor, id, false)
        Assert.assertEquals(executeMonitorResponse!!.monitorRunResult.monitorName, monitor.name)
        Assert.assertEquals(executeMonitorResponse.monitorRunResult.triggerResults.size, 1)
        searchAlerts(id)
        var table = Table("asc", "id", null, 1, 0, "")
        var getAlertsResponse = client()
            .execute(AlertingActions.GET_ALERTS_ACTION_TYPE, GetAlertsRequest(table, "ALL", "ALL", null, null))
            .get()
        Assert.assertTrue(getAlertsResponse != null)
        Assert.assertTrue(getAlertsResponse.alerts.size == 1)
        Assert.assertTrue(getAlertsResponse.alerts[0].errorMessage!!.contains("Trigger errors"))

        val oldLastNotificationTime = getAlertsResponse.alerts[0].lastNotificationTime

        executeMonitorResponse = executeMonitor(monitor, id, false)
        Assert.assertEquals(executeMonitorResponse!!.monitorRunResult.monitorName, monitor.name)
        Assert.assertEquals(executeMonitorResponse.monitorRunResult.triggerResults.size, 1)
        searchAlerts(id)
        table = Table("asc", "id", null, 10, 0, "")
        getAlertsResponse = client()
            .execute(AlertingActions.GET_ALERTS_ACTION_TYPE, GetAlertsRequest(table, "ALL", "ALL", null, null))
            .get()
        Assert.assertTrue(getAlertsResponse != null)
        Assert.assertTrue(getAlertsResponse.alerts.size == 1)
        Assert.assertEquals(0, getAlertsResponse.alerts[0].errorHistory.size)
        Assert.assertTrue(getAlertsResponse.alerts[0].errorMessage!!.contains("Trigger errors"))
        Assert.assertTrue(getAlertsResponse.alerts[0].lastNotificationTime!!.isAfter(oldLastNotificationTime))
    }

    fun `test monitor error alert cleared after successful monitor run`() {
        val customAlertIndex = "custom-alert-index"
        val customAlertHistoryIndex = "custom-alert-history-index"
        val customAlertHistoryIndexPattern = "<custom_alert_history_index-{now/d}-1>"
        val docQuery = DocLevelQuery(query = "source:12345", name = "1", fields = listOf())
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(docQuery)
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        var monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
            dataSources = DataSources(
                alertsIndex = customAlertIndex,
                alertsHistoryIndex = customAlertHistoryIndex,
                alertsHistoryIndexPattern = customAlertHistoryIndexPattern
            )
        )

        val monitorResponse = createMonitor(monitor)
        assertFalse(monitorResponse?.id.isNullOrEmpty())

        monitor = monitorResponse!!.monitor
        val id = monitorResponse.id

        // Close index to force error alert
        client().admin().indices().close(CloseIndexRequest(index)).get()

        var executeMonitorResponse = executeMonitor(monitor, id, false)
        Assert.assertEquals(executeMonitorResponse!!.monitorRunResult.monitorName, monitor.name)
        Assert.assertEquals(executeMonitorResponse.monitorRunResult.triggerResults.size, 0)
        searchAlerts(id)
        var table = Table("asc", "id", null, 1, 0, "")
        var getAlertsResponse = client()
            .execute(AlertingActions.GET_ALERTS_ACTION_TYPE, GetAlertsRequest(table, "ALL", "ALL", id, customAlertIndex))
            .get()
        Assert.assertTrue(getAlertsResponse != null)
        Assert.assertEquals(1, getAlertsResponse.alerts.size)
        Assert.assertTrue(getAlertsResponse.alerts[0].errorMessage == "IndexClosedException[closed]")
        Assert.assertNull(getAlertsResponse.alerts[0].endTime)

        // Open index to have monitor run successfully
        client().admin().indices().open(OpenIndexRequest(index)).get()
        // Execute monitor again and expect successful run
        executeMonitorResponse = executeMonitor(monitor, id, false)
        Assert.assertEquals(executeMonitorResponse!!.monitorRunResult.monitorName, monitor.name)
        Assert.assertEquals(executeMonitorResponse.monitorRunResult.triggerResults.size, 1)
        // Verify that alert is moved to history index
        table = Table("asc", "id", null, 10, 0, "")
        getAlertsResponse = client()
            .execute(AlertingActions.GET_ALERTS_ACTION_TYPE, GetAlertsRequest(table, "ALL", "ALL", id, customAlertIndex))
            .get()
        Assert.assertTrue(getAlertsResponse != null)
        Assert.assertEquals(0, getAlertsResponse.alerts.size)

        table = Table("asc", "id", null, 10, 0, "")
        getAlertsResponse = client()
            .execute(AlertingActions.GET_ALERTS_ACTION_TYPE, GetAlertsRequest(table, "ALL", "ALL", id, customAlertHistoryIndex))
            .get()
        Assert.assertTrue(getAlertsResponse != null)
        Assert.assertEquals(1, getAlertsResponse.alerts.size)
        Assert.assertTrue(getAlertsResponse.alerts[0].errorMessage == "IndexClosedException[closed]")
        Assert.assertNotNull(getAlertsResponse.alerts[0].endTime)
    }

    fun `test multiple monitor error alerts cleared after successful monitor run`() {
        val customAlertIndex = "custom-alert-index"
        val customAlertHistoryIndex = "custom-alert-history-index"
        val customAlertHistoryIndexPattern = "<custom_alert_history_index-{now/d}-1>"
        val docQuery = DocLevelQuery(query = "source:12345", name = "1", fields = listOf())
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(docQuery)
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        var monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
            dataSources = DataSources(
                alertsIndex = customAlertIndex,
                alertsHistoryIndex = customAlertHistoryIndex,
                alertsHistoryIndexPattern = customAlertHistoryIndexPattern
            )
        )

        val monitorResponse = createMonitor(monitor)
        assertFalse(monitorResponse?.id.isNullOrEmpty())

        monitor = monitorResponse!!.monitor
        val monitorId = monitorResponse.id

        // Close index to force error alert
        client().admin().indices().close(CloseIndexRequest(index)).get()

        var executeMonitorResponse = executeMonitor(monitor, monitorId, false)
        Assert.assertEquals(executeMonitorResponse!!.monitorRunResult.monitorName, monitor.name)
        Assert.assertEquals(executeMonitorResponse.monitorRunResult.triggerResults.size, 0)
        // Create 10 old alerts to simulate having "old error alerts"(2.6)
        for (i in 1..10) {
            val startTimestamp = Instant.now().minusSeconds(3600 * 24 * i.toLong()).toEpochMilli()
            val oldErrorAlertAsString = """
                {"id":"$i","version":-1,"monitor_id":"$monitorId",
                "schema_version":4,"monitor_version":1,"monitor_name":"geCNcHKTlp","monitor_user":{"name":"","backend_roles":[],
                "roles":[],"custom_attribute_names":[],"user_requested_tenant":null},"trigger_id":"_nnk_YcB5pHgSZwYwO2r",
                "trigger_name":"NoOp trigger","finding_ids":[],"related_doc_ids":[],"state":"ERROR","error_message":"some monitor error",
                "alert_history":[],"severity":"","action_execution_results":[],
                "start_time":$startTimestamp,"last_notification_time":$startTimestamp,"end_time":null,"acknowledged_time":null}
            """.trimIndent()

            client().index(
                IndexRequest(customAlertIndex)
                    .id("$i")
                    .routing(monitorId)
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .source(oldErrorAlertAsString, XContentType.JSON)
            ).get()
        }
        var table = Table("asc", "id", null, 1000, 0, "")
        var getAlertsResponse = client()
            .execute(AlertingActions.GET_ALERTS_ACTION_TYPE, GetAlertsRequest(table, "ALL", "ALL", monitorId, customAlertIndex))
            .get()

        Assert.assertTrue(getAlertsResponse != null)
        Assert.assertEquals(1 + 10, getAlertsResponse.alerts.size)
        val newErrorAlert = getAlertsResponse.alerts.firstOrNull { it.errorMessage == "IndexClosedException[closed]" }
        Assert.assertNotNull(newErrorAlert)
        Assert.assertNull(newErrorAlert!!.endTime)

        // Open index to have monitor run successfully
        client().admin().indices().open(OpenIndexRequest(index)).get()
        // Execute monitor again and expect successful run
        executeMonitorResponse = executeMonitor(monitor, monitorId, false)
        Assert.assertEquals(executeMonitorResponse!!.monitorRunResult.monitorName, monitor.name)
        Assert.assertEquals(executeMonitorResponse.monitorRunResult.triggerResults.size, 1)
        // Verify that alert is moved to history index
        table = Table("asc", "id", null, 1000, 0, "")
        getAlertsResponse = client()
            .execute(AlertingActions.GET_ALERTS_ACTION_TYPE, GetAlertsRequest(table, "ALL", "ALL", monitorId, customAlertIndex))
            .get()
        Assert.assertTrue(getAlertsResponse != null)
        Assert.assertEquals(0, getAlertsResponse.alerts.size)

        table = Table("asc", "id", null, 1000, 0, "")
        getAlertsResponse = client()
            .execute(AlertingActions.GET_ALERTS_ACTION_TYPE, GetAlertsRequest(table, "ALL", "ALL", monitorId, customAlertHistoryIndex))
            .get()
        Assert.assertTrue(getAlertsResponse != null)
        Assert.assertEquals(11, getAlertsResponse.alerts.size)
        getAlertsResponse.alerts.forEach { alert -> assertNotNull(alert.endTime) }
    }

    fun `test execute monitor with custom query index and nested mappings`() {
        val docQuery1 = DocLevelQuery(query = "message:\"msg 1 2 3 4\"", name = "3", fields = listOf())
        val docLevelInput = DocLevelMonitorInput("description", listOf(index), listOf(docQuery1))
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val customFindingsIndex = "custom_findings_index"
        val customFindingsIndexPattern = "custom_findings_index-1"
        val customQueryIndex = "custom_alerts_index"
        var monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
            dataSources = DataSources(
                queryIndex = customQueryIndex,
                findingsIndex = customFindingsIndex,
                findingsIndexPattern = customFindingsIndexPattern
            )
        )
        val monitorResponse = createMonitor(monitor)

        // We are verifying here that index with nested mappings and nested aliases
        // won't break query matching

        // Create index mappings
        val m: MutableMap<String, Any> = HashMap()
        val m1: MutableMap<String, Any> = HashMap()
        m1["title"] = Map.of("type", "text")
        m1["category"] = Map.of("type", "keyword")
        m["rule"] = Map.of("type", "nested", "properties", m1)
        val properties = Map.of<String, Any>("properties", m)

        client().admin().indices().putMapping(
            PutMappingRequest(
                index
            ).source(properties)
        ).get()

        // Put alias for nested fields
        val mm: MutableMap<String, Any> = HashMap()
        val mm1: MutableMap<String, Any> = HashMap()
        mm1["title_alias"] = Map.of("type", "alias", "path", "rule.title")
        mm["rule"] = Map.of("type", "nested", "properties", mm1)
        val properties1 = Map.of<String, Any>("properties", mm)
        client().admin().indices().putMapping(
            PutMappingRequest(
                index
            ).source(properties1)
        ).get()

        val testDoc = """{
            "rule": {"title": "some_title"},
            "message": "msg 1 2 3 4"
        }"""
        indexDoc(index, "2", testDoc)

        client().admin().indices().putMapping(
            PutMappingRequest(index).source("alias.some.fff", "type=alias,path=test_field.some_other_field")
        )
        assertFalse(monitorResponse?.id.isNullOrEmpty())
        monitor = monitorResponse!!.monitor
        val id = monitorResponse.id
        val executeMonitorResponse = executeMonitor(monitor, id, false)
        Assert.assertEquals(executeMonitorResponse!!.monitorRunResult.monitorName, monitor.name)
        Assert.assertEquals(executeMonitorResponse.monitorRunResult.triggerResults.size, 1)
        searchAlerts(id)
        val table = Table("asc", "id", null, 1, 0, "")
        var getAlertsResponse = client()
            .execute(AlertingActions.GET_ALERTS_ACTION_TYPE, GetAlertsRequest(table, "ALL", "ALL", null, null))
            .get()
        Assert.assertTrue(getAlertsResponse != null)
        Assert.assertTrue(getAlertsResponse.alerts.size == 1)
        val findings = searchFindings(id, customFindingsIndex)
        assertEquals("Findings saved for test monitor", 1, findings.size)
        assertTrue("Findings saved for test monitor", findings[0].relatedDocIds.contains("2"))
        assertEquals("Didn't match all 4 queries", 1, findings[0].docLevelQueries.size)
    }

    fun `test cleanup monitor on partial create monitor failure`() {
        val docQuery = DocLevelQuery(query = "dnbkjndsfkjbnds:\"us-west-2\"", name = "3", fields = listOf())
        val docLevelInput = DocLevelMonitorInput("description", listOf(index), listOf(docQuery))
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val customQueryIndex = "custom_alerts_index"
        val analyzer = "dfbdfbafd"
        val testDoc = """{
            "rule": {"title": "some_title"},
            "message": "msg 1 2 3 4"
        }"""
        indexDoc(index, "2", testDoc)
        client().admin().indices()
            .create(
                CreateIndexRequest(customQueryIndex + "-000001").alias(Alias(customQueryIndex))
                    .mapping(
                        """
                        {
                          "_meta": {
                            "schema_version": 1
                          },
                          "properties": {
                            "query": {
                              "type": "percolator_ext"
                            },
                            "monitor_id": {
                              "type": "text"
                            },
                            "index": {
                              "type": "text"
                            }
                          }
                        }
                        """.trimIndent()
                    )
            ).get()

        client().admin().indices().close(CloseIndexRequest(customQueryIndex + "-000001")).get()
        var monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
            dataSources = DataSources(
                queryIndex = customQueryIndex,
                queryIndexMappingsByType = mapOf(Pair("text", mapOf(Pair("analyzer", analyzer)))),
            )
        )
        try {
            createMonitor(monitor)
            fail("monitor creation should fail due to incorrect analyzer name in test setup")
        } catch (e: Exception) {
            Assert.assertEquals(client().search(SearchRequest(SCHEDULED_JOBS_INDEX)).get().hits.hits.size, 0)
        }
    }

    fun `test execute monitor without create when no monitors exists`() {
        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3", fields = listOf())
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

    fun `test execute monitor with custom query index and custom field mappings`() {
        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3", fields = listOf())
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
        val mapping = client().admin().indices().getMappings(GetMappingsRequest().indices(customQueryIndex)).get()
        Assert.assertTrue(mapping.toString().contains("\"analyzer\":\"$analyzer\""))
    }

    fun `test delete monitor deletes all queries and metadata too`() {
        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3", fields = listOf())
        val docLevelInput = DocLevelMonitorInput("description", listOf(index), listOf(docQuery))
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val customQueryIndex = "custom_query_index"
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
        val monitorId = monitorResponse.id
        val executeMonitorResponse = executeMonitor(monitor, monitorId, false)
        Assert.assertEquals(executeMonitorResponse!!.monitorRunResult.monitorName, monitor.name)
        Assert.assertEquals(executeMonitorResponse.monitorRunResult.triggerResults.size, 1)
        searchAlerts(monitorId)
        val clusterStateResponse = client().admin().cluster().state(ClusterStateRequest().indices(customQueryIndex).metadata(true)).get()
        val mapping = client().admin().indices().getMappings(GetMappingsRequest().indices(customQueryIndex)).get()
        Assert.assertTrue(mapping.toString().contains("\"analyzer\":\"$analyzer\"") == true)
        // Verify queries exist
        var searchResponse = client().search(
            SearchRequest(customQueryIndex).source(SearchSourceBuilder().query(QueryBuilders.matchAllQuery()))
        ).get()
        assertNotEquals(0, searchResponse.hits.hits.size)

        deleteMonitor(monitorId)
        assertIndexNotExists(customQueryIndex + "*")
        assertAliasNotExists(customQueryIndex)
    }

    fun `test execute monitor with custom findings index and pattern`() {
        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3", fields = listOf())
        val docLevelInput = DocLevelMonitorInput("description", listOf(index), listOf(docQuery))
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val customFindingsIndex = "custom_findings_index"
        val customFindingsIndexPattern = "<custom_findings_index-{now/d}-1>"
        var monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
            dataSources = DataSources(findingsIndex = customFindingsIndex, findingsIndexPattern = customFindingsIndexPattern)
        )
        val monitorResponse = createMonitor(monitor)
        client().admin().indices().refresh(RefreshRequest("*"))
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
        var executeMonitorResponse = executeMonitor(monitor, id, false)
        Assert.assertEquals(executeMonitorResponse!!.monitorRunResult.monitorName, monitor.name)
        Assert.assertEquals(executeMonitorResponse.monitorRunResult.triggerResults.size, 1)

        var findings = searchFindings(id, "custom_findings_index*", true)
        assertEquals("Findings saved for test monitor", 1, findings.size)
        assertTrue("Findings saved for test monitor", findings[0].relatedDocIds.contains("1"))

        indexDoc(index, "2", testDoc)
        executeMonitorResponse = executeMonitor(monitor, id, false)
        Assert.assertEquals(executeMonitorResponse!!.monitorRunResult.monitorName, monitor.name)
        Assert.assertEquals(executeMonitorResponse.monitorRunResult.triggerResults.size, 1)
        searchAlerts(id)
        findings = searchFindings(id, "custom_findings_index*", true)
        assertEquals("Findings saved for test monitor", 2, findings.size)
        assertTrue("Findings saved for test monitor", findings[1].relatedDocIds.contains("2"))

        val indices = getAllIndicesFromPattern("custom_findings_index*")
        Assert.assertTrue(indices.isNotEmpty())
    }

    fun `test execute monitor with multiple indices in input success`() {

        val testSourceIndex1 = "test_source_index1"
        val testSourceIndex2 = "test_source_index2"

        createIndex(testSourceIndex1, Settings.EMPTY)
        createIndex(testSourceIndex2, Settings.EMPTY)

        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3", fields = listOf())
        val docLevelInput = DocLevelMonitorInput("description", listOf(testSourceIndex1, testSourceIndex2), listOf(docQuery))
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val customFindingsIndex = "custom_findings_index"
        val customFindingsIndexPattern = "<custom_findings_index-{now/d}-1>"
        var monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
            dataSources = DataSources(findingsIndex = customFindingsIndex, findingsIndexPattern = customFindingsIndexPattern)
        )
        val monitorResponse = createMonitor(monitor)
        client().admin().indices().refresh(RefreshRequest("*"))
        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""
        assertFalse(monitorResponse?.id.isNullOrEmpty())
        monitor = monitorResponse!!.monitor

        indexDoc(testSourceIndex1, "1", testDoc)
        indexDoc(testSourceIndex2, "1", testDoc)

        val id = monitorResponse.id
        var executeMonitorResponse = executeMonitor(monitor, id, false)
        Assert.assertEquals(executeMonitorResponse!!.monitorRunResult.monitorName, monitor.name)
        Assert.assertEquals(executeMonitorResponse.monitorRunResult.triggerResults.size, 1)

        var findings = searchFindings(id, "custom_findings_index*", true)
        assertEquals("Findings saved for test monitor", 2, findings.size)
        var foundFindings = findings.filter { it.relatedDocIds.contains("1") }
        assertEquals("Didn't find 2 findings", 2, foundFindings.size)

        indexDoc(testSourceIndex1, "2", testDoc)
        indexDoc(testSourceIndex2, "2", testDoc)
        executeMonitorResponse = executeMonitor(monitor, id, false)
        Assert.assertEquals(executeMonitorResponse!!.monitorRunResult.monitorName, monitor.name)
        Assert.assertEquals(executeMonitorResponse.monitorRunResult.triggerResults.size, 1)
        searchAlerts(id)
        findings = searchFindings(id, "custom_findings_index*", true)
        assertEquals("Findings saved for test monitor", 4, findings.size)
        foundFindings = findings.filter { it.relatedDocIds.contains("2") }
        assertEquals("Didn't find 2 findings", 2, foundFindings.size)

        val indices = getAllIndicesFromPattern("custom_findings_index*")
        Assert.assertTrue(indices.isNotEmpty())
    }

    fun `test execute monitor with multiple indices in input first index gets deleted`() {
        // Index #1 does not exist
        val testSourceIndex1 = "test_source_index1"
        val testSourceIndex2 = "test_source_index2"

        createIndex(testSourceIndex1, Settings.EMPTY)
        createIndex(testSourceIndex2, Settings.EMPTY)

        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3", fields = listOf())
        val docLevelInput = DocLevelMonitorInput("description", listOf(testSourceIndex1, testSourceIndex2), listOf(docQuery))
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val customFindingsIndex = "custom_findings_index"
        val customFindingsIndexPattern = "<custom_findings_index-{now/d}-1>"
        var monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
            dataSources = DataSources(findingsIndex = customFindingsIndex, findingsIndexPattern = customFindingsIndexPattern)
        )
        val monitorResponse = createMonitor(monitor)
        client().admin().indices().refresh(RefreshRequest("*"))
        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""
        assertFalse(monitorResponse?.id.isNullOrEmpty())
        monitor = monitorResponse!!.monitor

        indexDoc(testSourceIndex2, "1", testDoc)

        client().admin().indices().delete(DeleteIndexRequest(testSourceIndex1)).get()

        val id = monitorResponse.id
        var executeMonitorResponse = executeMonitor(monitor, id, false)
        Assert.assertEquals(executeMonitorResponse!!.monitorRunResult.monitorName, monitor.name)
        Assert.assertEquals(executeMonitorResponse.monitorRunResult.triggerResults.size, 1)

        var findings = searchFindings(id, "custom_findings_index*", true)
        assertEquals("Findings saved for test monitor", 1, findings.size)
        var foundFindings = findings.filter { it.relatedDocIds.contains("1") }
        assertEquals("Didn't find 2 findings", 1, foundFindings.size)

        indexDoc(testSourceIndex2, "2", testDoc)
        executeMonitorResponse = executeMonitor(monitor, id, false)
        Assert.assertEquals(executeMonitorResponse!!.monitorRunResult.monitorName, monitor.name)
        Assert.assertEquals(executeMonitorResponse.monitorRunResult.triggerResults.size, 1)
        searchAlerts(id)
        findings = searchFindings(id, "custom_findings_index*", true)
        assertEquals("Findings saved for test monitor", 2, findings.size)
        foundFindings = findings.filter { it.relatedDocIds.contains("2") }
        assertEquals("Didn't find 2 findings", 1, foundFindings.size)

        val indices = getAllIndicesFromPattern("custom_findings_index*")
        Assert.assertTrue(indices.isNotEmpty())
    }

    fun `test execute monitor with multiple indices in input second index gets deleted`() {
        // Second index does not exist
        val testSourceIndex1 = "test_source_index1"
        val testSourceIndex2 = "test_source_index2"

        createIndex(testSourceIndex1, Settings.EMPTY)
        createIndex(testSourceIndex2, Settings.EMPTY)

        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3", fields = listOf())
        val docLevelInput = DocLevelMonitorInput("description", listOf(testSourceIndex1, testSourceIndex2), listOf(docQuery))
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val customFindingsIndex = "custom_findings_index"
        val customFindingsIndexPattern = "<custom_findings_index-{now/d}-1>"
        var monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
            dataSources = DataSources(findingsIndex = customFindingsIndex, findingsIndexPattern = customFindingsIndexPattern)
        )
        val monitorResponse = createMonitor(monitor)
        client().admin().indices().refresh(RefreshRequest("*"))
        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "test_strict_date_time" : "$testTime",
            "test_field" : "us-west-2"
        }"""
        assertFalse(monitorResponse?.id.isNullOrEmpty())
        monitor = monitorResponse!!.monitor

        indexDoc(testSourceIndex1, "1", testDoc)

        client().admin().indices().delete(DeleteIndexRequest(testSourceIndex2)).get()

        val id = monitorResponse.id
        var executeMonitorResponse = executeMonitor(monitor, id, false)
        Assert.assertEquals(executeMonitorResponse!!.monitorRunResult.monitorName, monitor.name)
        Assert.assertEquals(executeMonitorResponse.monitorRunResult.triggerResults.size, 1)

        var findings = searchFindings(id, "custom_findings_index*", true)
        assertEquals("Findings saved for test monitor", 1, findings.size)
        var foundFindings = findings.filter { it.relatedDocIds.contains("1") }
        assertEquals("Didn't find 2 findings", 1, foundFindings.size)

        indexDoc(testSourceIndex1, "2", testDoc)

        executeMonitorResponse = executeMonitor(monitor, id, false)
        Assert.assertEquals(executeMonitorResponse!!.monitorRunResult.monitorName, monitor.name)
        Assert.assertEquals(executeMonitorResponse.monitorRunResult.triggerResults.size, 1)
        searchAlerts(id)
        findings = searchFindings(id, "custom_findings_index*", true)
        assertEquals("Findings saved for test monitor", 2, findings.size)
        foundFindings = findings.filter { it.relatedDocIds.contains("2") }
        assertEquals("Didn't find 2 findings", 1, foundFindings.size)

        val indices = getAllIndicesFromPattern("custom_findings_index*")
        Assert.assertTrue(indices.isNotEmpty())
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
        assertEquals(1, alerts.size)

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
            .execute(AlertingActions.GET_ALERTS_ACTION_TYPE, GetAlertsRequest(table, "ALL", "ALL", null, customAlertsIndex))
            .get()
        Assert.assertTrue(getAlertsResponse != null)
        Assert.assertTrue(getAlertsResponse.alerts.size == 1)
        getAlertsResponse = client()
            .execute(AlertingActions.GET_ALERTS_ACTION_TYPE, GetAlertsRequest(table, "ALL", "ALL", monitorId, null))
            .get()
        Assert.assertTrue(getAlertsResponse != null)
        Assert.assertTrue(getAlertsResponse.alerts.size == 1)
        val searchRequest = SearchRequest(SCHEDULED_JOBS_INDEX)
        var searchMonitorResponse =
            client().execute(AlertingActions.SEARCH_MONITORS_ACTION_TYPE, SearchMonitorRequest(searchRequest))
                .get()
        Assert.assertEquals(searchMonitorResponse.hits.hits.size, 0)
        searchRequest.source().query(MatchQueryBuilder("monitor.owner", "security_analytics_plugin"))
        searchMonitorResponse =
            client().execute(AlertingActions.SEARCH_MONITORS_ACTION_TYPE, SearchMonitorRequest(searchRequest))
                .get()
        Assert.assertEquals(searchMonitorResponse.hits.hits.size, 1)
    }

    fun `test execute pre-existing monitor without triggers`() {
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
        "triggers": [],
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
        assertEquals(0, alerts.size)

        val findings = searchFindings(monitorId)
        assertEquals("Findings saved for test monitor", 1, findings.size)
        assertTrue("Findings saved for test monitor", findings[0].relatedDocIds.contains("1"))
    }

    fun `test execute monitor with empty source index`() {
        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3", fields = listOf())
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

        val monitorId = monitorResponse.id
        var executeMonitorResponse = executeMonitor(monitor, monitorId, false)

        Assert.assertEquals(executeMonitorResponse!!.monitorRunResult.monitorName, monitor.name)
        Assert.assertEquals(executeMonitorResponse.monitorRunResult.triggerResults.size, 1)

        refreshIndex(customFindingsIndex)

        var findings = searchFindings(monitorId, customFindingsIndex)
        assertEquals("Findings saved for test monitor", 0, findings.size)

        indexDoc(index, "1", testDoc)

        executeMonitor(monitor, monitorId, false)

        refreshIndex(customFindingsIndex)

        findings = searchFindings(monitorId, customFindingsIndex)
        assertTrue("Findings saved for test monitor", findings[0].relatedDocIds.contains("1"))
    }

    fun `test execute GetFindingsAction with monitorId param`() {
        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3", fields = listOf())
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

    fun `test execute GetFindingsAction with unknown monitorId`() {
        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3", fields = listOf())
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

    fun `test execute monitor with owner field`() {
        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3", fields = listOf())
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
            .execute(AlertingActions.GET_ALERTS_ACTION_TYPE, GetAlertsRequest(table, "ALL", "ALL", null, customAlertsIndex))
            .get()
        Assert.assertTrue(getAlertsResponse != null)
        Assert.assertTrue(getAlertsResponse.alerts.size == 1)
        getAlertsResponse = client()
            .execute(AlertingActions.GET_ALERTS_ACTION_TYPE, GetAlertsRequest(table, "ALL", "ALL", id, null))
            .get()
        Assert.assertTrue(getAlertsResponse != null)
        Assert.assertTrue(getAlertsResponse.alerts.size == 1)
    }

    fun `test execute GetFindingsAction with unknown findingIndex param`() {
        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3", fields = listOf())
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

    fun `test search custom alerts history index`() {
        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3", fields = listOf())
        val docLevelInput = DocLevelMonitorInput("description", listOf(index), listOf(docQuery))
        val trigger1 = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val trigger2 = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val customAlertsIndex = "custom_alerts_index"
        val customAlertsHistoryIndex = "custom_alerts_history_index"
        val customAlertsHistoryIndexPattern = "<custom_alerts_history_index-{now/d}-1>"
        var monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger1, trigger2),
            dataSources = DataSources(
                alertsIndex = customAlertsIndex,
                alertsHistoryIndex = customAlertsHistoryIndex,
                alertsHistoryIndexPattern = customAlertsHistoryIndexPattern
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
        val monitorId = monitorResponse.id
        val executeMonitorResponse = executeMonitor(monitor, monitorId, false)
        var alertsBefore = searchAlerts(monitorId, customAlertsIndex)
        Assert.assertEquals(2, alertsBefore.size)
        Assert.assertEquals(executeMonitorResponse!!.monitorRunResult.monitorName, monitor.name)
        Assert.assertEquals(executeMonitorResponse.monitorRunResult.triggerResults.size, 2)
        // Remove 1 trigger from monitor to force moveAlerts call to move alerts to history index
        monitor = monitor.copy(triggers = listOf(trigger1))
        updateMonitor(monitor, monitorId)

        var alerts = listOf<Alert>()
        OpenSearchTestCase.waitUntil({
            alerts = searchAlerts(monitorId, customAlertsHistoryIndex)
            if (alerts.size == 1) {
                return@waitUntil true
            }
            return@waitUntil false
        }, 30, TimeUnit.SECONDS)
        assertEquals("Alerts from custom history index", 1, alerts.size)
    }

    fun `test search custom alerts history index after alert ack`() {
        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3", fields = listOf())
        val docLevelInput = DocLevelMonitorInput("description", listOf(index), listOf(docQuery))
        val trigger1 = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val trigger2 = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val customAlertsIndex = "custom_alerts_index"
        val customAlertsHistoryIndex = "custom_alerts_history_index"
        val customAlertsHistoryIndexPattern = "<custom_alerts_history_index-{now/d}-1>"
        var monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger1, trigger2),
            dataSources = DataSources(
                alertsIndex = customAlertsIndex,
                alertsHistoryIndex = customAlertsHistoryIndex,
                alertsHistoryIndexPattern = customAlertsHistoryIndexPattern
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
        val monitorId = monitorResponse.id
        val executeMonitorResponse = executeMonitor(monitor, monitorId, false)
        var alertsBefore = searchAlerts(monitorId, customAlertsIndex)
        Assert.assertEquals(2, alertsBefore.size)
        Assert.assertEquals(executeMonitorResponse!!.monitorRunResult.monitorName, monitor.name)
        Assert.assertEquals(executeMonitorResponse.monitorRunResult.triggerResults.size, 2)

        var alerts = listOf<Alert>()
        OpenSearchTestCase.waitUntil({
            alerts = searchAlerts(monitorId, customAlertsIndex)
            if (alerts.size == 1) {
                return@waitUntil true
            }
            return@waitUntil false
        }, 30, TimeUnit.SECONDS)
        assertEquals("Alerts from custom index", 2, alerts.size)

        val ackReq = AcknowledgeAlertRequest(monitorId, alerts.map { it.id }.toMutableList(), WriteRequest.RefreshPolicy.IMMEDIATE)
        client().execute(AlertingActions.ACKNOWLEDGE_ALERTS_ACTION_TYPE, ackReq).get()

        // verify alerts moved from alert index to alert history index
        alerts = listOf<Alert>()
        OpenSearchTestCase.waitUntil({
            alerts = searchAlerts(monitorId, customAlertsHistoryIndex)
            if (alerts.size == 1) {
                return@waitUntil true
            }
            return@waitUntil false
        }, 30, TimeUnit.SECONDS)
        assertEquals("Alerts from custom history index", 2, alerts.size)

        // verify alerts deleted from alert index
        alerts = listOf<Alert>()
        OpenSearchTestCase.waitUntil({
            alerts = searchAlerts(monitorId, customAlertsIndex)
            if (alerts.size == 1) {
                return@waitUntil true
            }
            return@waitUntil false
        }, 30, TimeUnit.SECONDS)
        assertEquals("Alerts from custom history index", 0, alerts.size)
    }

    fun `test get alerts by list of monitors containing both existent and non-existent ids`() {
        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3", fields = listOf())
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
                AlertingActions.GET_ALERTS_ACTION_TYPE,
                GetAlertsRequest(table, "ALL", "ALL", null, null)
            )
            .get()

        Assert.assertTrue(getAlertsResponse != null)
        Assert.assertTrue(getAlertsResponse.alerts.size == 2)

        var alertsResponseForRequestWithoutCustomIndex = client()
            .execute(
                AlertingActions.GET_ALERTS_ACTION_TYPE,
                GetAlertsRequest(table, "ALL", "ALL", null, null, monitorIds = listOf(id, id1, "1", "2"))
            )
            .get()
        Assert.assertTrue(alertsResponseForRequestWithoutCustomIndex != null)
        Assert.assertTrue(alertsResponseForRequestWithoutCustomIndex.alerts.size == 2)
        val alertIds = getAlertsResponse.alerts.stream().map { alert -> alert.id }.collect(Collectors.toList())
        var getAlertsByAlertIds = client()
            .execute(
                AlertingActions.GET_ALERTS_ACTION_TYPE,
                GetAlertsRequest(table, "ALL", "ALL", null, null, alertIds = alertIds)
            )
            .get()
        Assert.assertTrue(getAlertsByAlertIds != null)
        Assert.assertTrue(getAlertsByAlertIds.alerts.size == 2)

        var getAlertsByWrongAlertIds = client()
            .execute(
                AlertingActions.GET_ALERTS_ACTION_TYPE,
                GetAlertsRequest(table, "ALL", "ALL", null, null, alertIds = listOf("1", "2"))
            )
            .get()

        Assert.assertTrue(getAlertsByWrongAlertIds != null)
        Assert.assertEquals(getAlertsByWrongAlertIds.alerts.size, 0)
    }

    fun `test queryIndex rollover and delete monitor success`() {

        val testSourceIndex = "test_source_index"
        createIndex(testSourceIndex, Settings.builder().put("index.mapping.total_fields.limit", "10000").build())

        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3", fields = listOf())
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
            AlertingActions.DELETE_MONITOR_ACTION_TYPE, DeleteMonitorRequest(monitorResponse.id, WriteRequest.RefreshPolicy.IMMEDIATE)
        ).get()
        // Expect first concrete queryIndex to be deleted since that one was only used by this monitor
        getIndexResponse =
            client().admin().indices().getIndex(GetIndexRequest().indices(ScheduledJob.DOC_LEVEL_QUERIES_INDEX + "*")).get()
        assertEquals(1, getIndexResponse.indices.size)
        assertEquals(ScheduledJob.DOC_LEVEL_QUERIES_INDEX + "-000002", getIndexResponse.indices[0])
        // Delete monitor #2
        client().execute(
            AlertingActions.DELETE_MONITOR_ACTION_TYPE, DeleteMonitorRequest(monitorResponse2.id, WriteRequest.RefreshPolicy.IMMEDIATE)
        ).get()
        // Expect second concrete queryIndex to be deleted since that one was only used by this monitor
        getIndexResponse =
            client().admin().indices().getIndex(GetIndexRequest().indices(ScheduledJob.DOC_LEVEL_QUERIES_INDEX + "*")).get()
        assertEquals(0, getIndexResponse.indices.size)
    }

    fun `test queryIndex rollover failure source_index field count over limit`() {

        val testSourceIndex = "test_source_index"
        createIndex(testSourceIndex, Settings.EMPTY)

        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3", fields = listOf())
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

        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3", fields = listOf())
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
        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3", fields = listOf())
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
        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3", fields = listOf())
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

        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3", fields = listOf())
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

    // TODO - revisit single node integ tests setup to figure out why we cannot have multiple test classes implementing it

    fun `test execute workflow with custom alerts and finding index when bucket monitor is used in chained finding of doc monitor`() {
        val query = QueryBuilders.rangeQuery("test_strict_date_time")
            .gt("{{period_end}}||-10d")
            .lte("{{period_end}}")
            .format("epoch_millis")
        val compositeSources = listOf(
            TermsValuesSourceBuilder("test_field_1").field("test_field_1")
        )
        val customAlertsHistoryIndex = "custom_alerts_history_index"
        val customAlertsHistoryIndexPattern = "<custom_alerts_history_index-{now/d}-1>"
        val compositeAgg = CompositeAggregationBuilder("composite_agg", compositeSources)
        val input = SearchInput(indices = listOf(index), query = SearchSourceBuilder().size(0).query(query).aggregation(compositeAgg))
        // Bucket level monitor will reduce the size of matched doc ids on those that belong
        // to a bucket that contains more than 1 document after term grouping
        val triggerScript = """
            params.docCount > 1
        """.trimIndent()

        var trigger = randomBucketLevelTrigger()
        trigger = trigger.copy(
            bucketSelector = BucketSelectorExtAggregationBuilder(
                name = trigger.id,
                bucketsPathsMap = mapOf("docCount" to "_count"),
                script = Script(triggerScript),
                parentBucketPath = "composite_agg",
                filter = null,
            )
        )
        val bucketCustomAlertsIndex = "custom_alerts_index"
        val bucketCustomFindingsIndex = "custom_findings_index"
        val bucketCustomFindingsIndexPattern = "custom_findings_index-1"

        val bucketLevelMonitorResponse = createMonitor(
            randomBucketLevelMonitor(
                inputs = listOf(input),
                enabled = false,
                triggers = listOf(trigger),
                dataSources = DataSources(
                    findingsEnabled = true,
                    alertsIndex = bucketCustomAlertsIndex,
                    findingsIndex = bucketCustomFindingsIndex,
                    findingsIndexPattern = bucketCustomFindingsIndexPattern
                )
            )
        )!!

        val docQuery1 = DocLevelQuery(query = "test_field_1:\"test_value_2\"", name = "1", fields = listOf())
        val docQuery2 = DocLevelQuery(query = "test_field_1:\"test_value_1\"", name = "2", fields = listOf())
        val docQuery3 = DocLevelQuery(query = "test_field_1:\"test_value_3\"", name = "3", fields = listOf())
        val docLevelInput = DocLevelMonitorInput("description", listOf(index), listOf(docQuery1, docQuery2, docQuery3))
        val docTrigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val docCustomAlertsIndex = "custom_alerts_index"
        val docCustomFindingsIndex = "custom_findings_index"
        val docCustomFindingsIndexPattern = "custom_findings_index-1"
        var docLevelMonitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(docTrigger),
            dataSources = DataSources(
                alertsIndex = docCustomAlertsIndex,
                findingsIndex = docCustomFindingsIndex,
                findingsIndexPattern = docCustomFindingsIndexPattern
            )
        )

        val docLevelMonitorResponse = createMonitor(docLevelMonitor)!!
        // 1. bucketMonitor (chainedFinding = null) 2. docMonitor (chainedFinding = bucketMonitor)
        var workflow = randomWorkflow(
            monitorIds = listOf(bucketLevelMonitorResponse.id, docLevelMonitorResponse.id),
            enabled = false,
            auditDelegateMonitorAlerts = false
        )
        val workflowResponse = upsertWorkflow(workflow)!!
        val workflowById = searchWorkflow(workflowResponse.id)
        assertNotNull(workflowById)

        // Creates 5 documents
        insertSampleTimeSerializedData(
            index,
            listOf(
                "test_value_1",
                "test_value_1", // adding duplicate to verify aggregation
                "test_value_2",
                "test_value_2",
                "test_value_3"
            )
        )

        val workflowId = workflowResponse.id
        // 1. bucket level monitor should reduce the doc findings to 4 (1, 2, 3, 4)
        // 2. Doc level monitor will match those 4 documents although it contains rules for matching all 5 documents (docQuery3 matches the fifth)
        val executeWorkflowResponse = executeWorkflow(workflowById, workflowId, false)!!
        assertNotNull(executeWorkflowResponse)

        for (monitorRunResults in executeWorkflowResponse.workflowRunResult.monitorRunResults) {
            if (bucketLevelMonitorResponse.monitor.name == monitorRunResults.monitorName) {
                val searchResult = monitorRunResults.inputResults.results.first()

                @Suppress("UNCHECKED_CAST")
                val buckets = searchResult.stringMap("aggregations")?.stringMap("composite_agg")
                    ?.get("buckets") as List<kotlin.collections.Map<String, Any>>
                assertEquals("Incorrect search result", 3, buckets.size)

                val getAlertsResponse = assertAlerts(bucketLevelMonitorResponse.id, bucketCustomAlertsIndex, 2, workflowId)
                assertAcknowledges(getAlertsResponse.alerts, bucketLevelMonitorResponse.id, 2)
                assertFindings(bucketLevelMonitorResponse.id, bucketCustomFindingsIndex, 1, 4, listOf("1", "2", "3", "4"))
            } else {
                assertEquals(1, monitorRunResults.inputResults.results.size)
                val values = monitorRunResults.triggerResults.values
                assertEquals(1, values.size)
                @Suppress("UNCHECKED_CAST")
                val docLevelTrigger = values.iterator().next() as DocumentLevelTriggerRunResult
                val triggeredDocIds = docLevelTrigger.triggeredDocs.map { it.split("|")[0] }
                val expectedTriggeredDocIds = listOf("1", "2", "3", "4")
                assertEquals(expectedTriggeredDocIds, triggeredDocIds.sorted())

                val getAlertsResponse = assertAlerts(docLevelMonitorResponse.id, docCustomAlertsIndex, 4, workflowId)
                assertAcknowledges(getAlertsResponse.alerts, docLevelMonitorResponse.id, 4)
                assertFindings(docLevelMonitorResponse.id, docCustomFindingsIndex, 4, 4, listOf("1", "2", "3", "4"))
            }
        }
    }

    fun `test execute workflow with custom alerts and finding index when doc level delegate is used in chained finding`() {
        val docQuery1 = DocLevelQuery(query = "test_field_1:\"test_value_2\"", name = "1", fields = listOf())
        val docQuery2 = DocLevelQuery(query = "test_field_1:\"test_value_3\"", name = "2", fields = listOf())

        var docLevelMonitor = randomDocumentLevelMonitor(
            inputs = listOf(DocLevelMonitorInput("description", listOf(index), listOf(docQuery1, docQuery2))),
            triggers = listOf(randomDocumentLevelTrigger(condition = ALWAYS_RUN)),
            dataSources = DataSources(
                alertsIndex = "custom_alerts_index",
                findingsIndex = "custom_findings_index",
                findingsIndexPattern = "custom_findings_index-1"
            )
        )

        val docLevelMonitorResponse = createMonitor(docLevelMonitor)!!

        val query = QueryBuilders.rangeQuery("test_strict_date_time")
            .gt("{{period_end}}||-10d")
            .lte("{{period_end}}")
            .format("epoch_millis")
        val compositeSources = listOf(
            TermsValuesSourceBuilder("test_field_1").field("test_field_1")
        )
        val compositeAgg = CompositeAggregationBuilder("composite_agg", compositeSources)
        val input = SearchInput(indices = listOf(index), query = SearchSourceBuilder().size(0).query(query).aggregation(compositeAgg))
        // Bucket level monitor will reduce the size of matched doc ids on those that belong to a bucket that contains more than 1 document after term grouping
        val triggerScript = """
            params.docCount > 1
        """.trimIndent()

        var trigger = randomBucketLevelTrigger()
        trigger = trigger.copy(
            bucketSelector = BucketSelectorExtAggregationBuilder(
                name = trigger.id,
                bucketsPathsMap = mapOf("docCount" to "_count"),
                script = Script(triggerScript),
                parentBucketPath = "composite_agg",
                filter = null,
            )
        )

        val bucketLevelMonitorResponse = createMonitor(
            randomBucketLevelMonitor(
                inputs = listOf(input),
                enabled = false,
                triggers = listOf(trigger),
                dataSources = DataSources(
                    findingsEnabled = true,
                    alertsIndex = "custom_alerts_index",
                    findingsIndex = "custom_findings_index",
                    findingsIndexPattern = "custom_findings_index-1"
                )
            )
        )!!

        var docLevelMonitor1 = randomDocumentLevelMonitor(
            // Match the documents with test_field_1: test_value_3
            inputs = listOf(DocLevelMonitorInput("description", listOf(index), listOf(docQuery2))),
            triggers = listOf(randomDocumentLevelTrigger(condition = ALWAYS_RUN)),
            dataSources = DataSources(
                findingsEnabled = true,
                alertsIndex = "custom_alerts_index_1",
                findingsIndex = "custom_findings_index_1",
                findingsIndexPattern = "custom_findings_index_1-1"
            )
        )

        val docLevelMonitorResponse1 = createMonitor(docLevelMonitor1)!!

        val queryMonitorInput = SearchInput(
            indices = listOf(index),
            query = SearchSourceBuilder().query(
                QueryBuilders
                    .rangeQuery("test_strict_date_time")
                    .gt("{{period_end}}||-10d")
                    .lte("{{period_end}}")
                    .format("epoch_millis")
            )
        )
        val queryTriggerScript = """
            return ctx.results[0].hits.hits.size() > 0
        """.trimIndent()

        val queryLevelTrigger = randomQueryLevelTrigger(condition = Script(queryTriggerScript))
        val queryMonitorResponse =
            createMonitor(randomQueryLevelMonitor(inputs = listOf(queryMonitorInput), triggers = listOf(queryLevelTrigger)))!!

        // 1. docMonitor (chainedFinding = null) 2. bucketMonitor (chainedFinding = docMonitor) 3. docMonitor (chainedFinding = bucketMonitor) 4. queryMonitor (chainedFinding = docMonitor 3)
        var workflow = randomWorkflow(
            monitorIds = listOf(
                docLevelMonitorResponse.id,
                bucketLevelMonitorResponse.id,
                docLevelMonitorResponse1.id,
                queryMonitorResponse.id
            ),
            auditDelegateMonitorAlerts = false
        )
        val workflowResponse = upsertWorkflow(workflow)!!
        val workflowById = searchWorkflow(workflowResponse.id)
        assertNotNull(workflowById)

        // Creates 5 documents
        insertSampleTimeSerializedData(
            index,
            listOf(
                "test_value_1",
                "test_value_1", // adding duplicate to verify aggregation
                "test_value_2",
                "test_value_2",
                "test_value_3",
                "test_value_3"
            )
        )

        val workflowId = workflowResponse.id
        // 1. Doc level monitor should reduce the doc findings to 4 (3 - test_value_2, 4 - test_value_2, 5 - test_value_3, 6 - test_value_3)
        // 2. Bucket level monitor will match the fetch the docs from current findings execution, although it contains rules for matching documents which has test_value_2 and test value_3
        val executeWorkflowResponse = executeWorkflow(workflowById, workflowId, false)!!
        assertNotNull(executeWorkflowResponse)

        for (monitorRunResults in executeWorkflowResponse.workflowRunResult.monitorRunResults) {
            when (monitorRunResults.monitorName) {
                // Verify first doc level monitor execution, alerts and findings
                docLevelMonitorResponse.monitor.name -> {
                    assertEquals(1, monitorRunResults.inputResults.results.size)
                    val values = monitorRunResults.triggerResults.values
                    assertEquals(1, values.size)
                    @Suppress("UNCHECKED_CAST")
                    val docLevelTrigger = values.iterator().next() as DocumentLevelTriggerRunResult
                    val triggeredDocIds = docLevelTrigger.triggeredDocs.map { it.split("|")[0] }
                    val expectedTriggeredDocIds = listOf("3", "4", "5", "6")
                    assertEquals(expectedTriggeredDocIds, triggeredDocIds.sorted())

                    val getAlertsResponse =
                        assertAlerts(docLevelMonitorResponse.id, docLevelMonitorResponse.monitor.dataSources.alertsIndex, 4, workflowId)
                    assertAcknowledges(getAlertsResponse.alerts, docLevelMonitorResponse.id, 4)
                    assertFindings(
                        docLevelMonitorResponse.id,
                        docLevelMonitorResponse.monitor.dataSources.findingsIndex,
                        4,
                        4,
                        listOf("3", "4", "5", "6")
                    )
                }
                // Verify second bucket level monitor execution, alerts and findings
                bucketLevelMonitorResponse.monitor.name -> {
                    val searchResult = monitorRunResults.inputResults.results.first()

                    @Suppress("UNCHECKED_CAST")
                    val buckets =
                        searchResult
                            .stringMap("aggregations")?.stringMap("composite_agg")
                            ?.get("buckets") as List<kotlin.collections.Map<String, Any>>
                    assertEquals("Incorrect search result", 2, buckets.size)

                    val getAlertsResponse =
                        assertAlerts(
                            bucketLevelMonitorResponse.id,
                            bucketLevelMonitorResponse.monitor.dataSources.alertsIndex,
                            2,
                            workflowId
                        )
                    assertAcknowledges(getAlertsResponse.alerts, bucketLevelMonitorResponse.id, 2)
                    assertFindings(
                        bucketLevelMonitorResponse.id,
                        bucketLevelMonitorResponse.monitor.dataSources.findingsIndex,
                        1,
                        4,
                        listOf("3", "4", "5", "6")
                    )
                }
                // Verify third doc level monitor execution, alerts and findings
                docLevelMonitorResponse1.monitor.name -> {
                    assertEquals(1, monitorRunResults.inputResults.results.size)
                    val values = monitorRunResults.triggerResults.values
                    assertEquals(1, values.size)
                    @Suppress("UNCHECKED_CAST")
                    val docLevelTrigger = values.iterator().next() as DocumentLevelTriggerRunResult
                    val triggeredDocIds = docLevelTrigger.triggeredDocs.map { it.split("|")[0] }
                    val expectedTriggeredDocIds = listOf("5", "6")
                    assertEquals(expectedTriggeredDocIds, triggeredDocIds.sorted())

                    val getAlertsResponse =
                        assertAlerts(docLevelMonitorResponse1.id, docLevelMonitorResponse1.monitor.dataSources.alertsIndex, 2, workflowId)
                    assertAcknowledges(getAlertsResponse.alerts, docLevelMonitorResponse1.id, 2)
                    assertFindings(
                        docLevelMonitorResponse1.id,
                        docLevelMonitorResponse1.monitor.dataSources.findingsIndex,
                        2,
                        2,
                        listOf("5", "6")
                    )
                }
                // Verify fourth query level monitor execution
                queryMonitorResponse.monitor.name -> {
                    assertEquals(1, monitorRunResults.inputResults.results.size)
                    val values = monitorRunResults.triggerResults.values
                    assertEquals(1, values.size)
                    @Suppress("UNCHECKED_CAST")
                    val totalHits =
                        (
                            (
                                monitorRunResults.inputResults.results[0]["hits"] as kotlin.collections.Map<String, Any>
                                )["total"] as kotlin.collections.Map<String, Any>
                            )["value"]
                    assertEquals(2, totalHits)
                    @Suppress("UNCHECKED_CAST")
                    val docIds =
                        (
                            (
                                monitorRunResults.inputResults.results[0]["hits"] as kotlin.collections.Map<String, Any>
                                )["hits"] as List<kotlin.collections.Map<String, String>>
                            ).map { it["_id"]!! }
                    assertEquals(listOf("5", "6"), docIds.sorted())
                }
            }
        }
    }

    private fun assertAlerts(
        monitorId: String,
        customAlertsIndex: String,
        alertSize: Int,
        workflowId: String,
    ): GetAlertsResponse {
        val table = Table("asc", "id", null, alertSize, 0, "")
        val getAlertsResponse = client()
            .execute(
                AlertingActions.GET_ALERTS_ACTION_TYPE,
                GetAlertsRequest(
                    table, "ALL", "ALL", monitorId, customAlertsIndex,
                    workflowIds = listOf(workflowId)
                )
            )
            .get()
        assertTrue(getAlertsResponse != null)
        assertTrue(getAlertsResponse.alerts.size == alertSize)
        return getAlertsResponse
    }

    fun `test execute workflow with custom alerts and finding index with doc level delegates`() {
        val docQuery1 = DocLevelQuery(query = "test_field_1:\"us-west-2\"", name = "3", fields = listOf())
        val docLevelInput1 = DocLevelMonitorInput("description", listOf(index), listOf(docQuery1))
        val trigger1 = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val customAlertsIndex1 = "custom_alerts_index"
        val customFindingsIndex1 = "custom_findings_index"
        val customFindingsIndexPattern1 = "custom_findings_index-1"
        var monitor1 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput1),
            triggers = listOf(trigger1),
            dataSources = DataSources(
                alertsIndex = customAlertsIndex1,
                findingsIndex = customFindingsIndex1,
                findingsIndexPattern = customFindingsIndexPattern1
            )
        )
        val monitorResponse = createMonitor(monitor1)!!

        val docQuery2 = DocLevelQuery(query = "source.ip.v6.v2:16645", name = "4", fields = listOf())
        val docLevelInput2 = DocLevelMonitorInput("description", listOf(index), listOf(docQuery2))
        val trigger2 = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val customAlertsIndex2 = "custom_alerts_index_2"
        val customFindingsIndex2 = "custom_findings_index_2"
        val customFindingsIndexPattern2 = "custom_findings_index-2"
        var monitor2 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput2),
            triggers = listOf(trigger2),
            dataSources = DataSources(
                alertsIndex = customAlertsIndex2,
                findingsIndex = customFindingsIndex2,
                findingsIndexPattern = customFindingsIndexPattern2
            )
        )

        val monitorResponse2 = createMonitor(monitor2)!!

        var workflow = randomWorkflow(
            monitorIds = listOf(monitorResponse.id, monitorResponse2.id), auditDelegateMonitorAlerts = false
        )
        val workflowResponse = upsertWorkflow(workflow)!!
        val workflowById = searchWorkflow(workflowResponse.id)
        assertNotNull(workflowById)

        var testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(ChronoUnit.MILLIS))
        // Matches monitor1
        val testDoc1 = """{
            "message" : "This is an error from IAD region",
            "source.ip.v6.v2" : 16644, 
            "test_strict_date_time" : "$testTime",
            "test_field_1" : "us-west-2"
        }"""
        indexDoc(index, "1", testDoc1)

        testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(ChronoUnit.MILLIS))
        // Matches monitor1 and monitor2
        val testDoc2 = """{
            "message" : "This is an error from IAD region",
            "source.ip.v6.v2" : 16645, 
            "test_strict_date_time" : "$testTime",
            "test_field_1" : "us-west-2"
        }"""
        indexDoc(index, "2", testDoc2)

        testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(ChronoUnit.MILLIS))
        // Doesn't match
        val testDoc3 = """{
            "message" : "This is an error from IAD region",
            "source.ip.v6.v2" : 16645, 
            "test_strict_date_time" : "$testTime",
            "test_field_1" : "us-east-1"
        }"""
        indexDoc(index, "3", testDoc3)

        val workflowId = workflowResponse.id
        val executeWorkflowResponse = executeWorkflow(workflowById, workflowId, false)!!
        val monitorsRunResults = executeWorkflowResponse.workflowRunResult.monitorRunResults
        assertEquals(2, monitorsRunResults.size)

        assertEquals(monitor1.name, monitorsRunResults[0].monitorName)
        assertEquals(1, monitorsRunResults[0].triggerResults.size)

        Assert.assertEquals(monitor2.name, monitorsRunResults[1].monitorName)
        Assert.assertEquals(1, monitorsRunResults[1].triggerResults.size)

        val getAlertsResponse = assertAlerts(monitorResponse.id, customAlertsIndex1, alertSize = 2, workflowId = workflowId)
        assertAcknowledges(getAlertsResponse.alerts, monitorResponse.id, 2)
        assertFindings(monitorResponse.id, customFindingsIndex1, 2, 2, listOf("1", "2"))

        val getAlertsResponse2 = assertAlerts(monitorResponse2.id, customAlertsIndex2, alertSize = 1, workflowId = workflowId)
        assertAcknowledges(getAlertsResponse2.alerts, monitorResponse2.id, 1)
        assertFindings(monitorResponse2.id, customFindingsIndex2, 1, 1, listOf("2"))
    }

    fun `test execute workflow with multiple monitors in chained monitor findings of single monitor`() {
        val docQuery1 = DocLevelQuery(query = "test_field_1:\"us-west-2\"", name = "3", fields = listOf())
        val docLevelInput1 = DocLevelMonitorInput("description", listOf(index), listOf(docQuery1))
        val trigger1 = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val customAlertsIndex1 = "custom_alerts_index"
        val customFindingsIndex1 = "custom_findings_index"
        val customFindingsIndexPattern1 = "custom_findings_index-1"
        var monitor1 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput1),
            triggers = listOf(trigger1),
            enabled = false,
            dataSources = DataSources(
                alertsIndex = customAlertsIndex1,
                findingsIndex = customFindingsIndex1,
                findingsIndexPattern = customFindingsIndexPattern1
            )
        )
        val monitorResponse = createMonitor(monitor1)!!

        val docQuery2 = DocLevelQuery(query = "source.ip.v6.v2:16645", name = "4", fields = listOf())
        val docLevelInput2 = DocLevelMonitorInput("description", listOf(index), listOf(docQuery2))
        val trigger2 = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        var monitor2 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput2),
            triggers = listOf(trigger2),
            enabled = false,
            dataSources = DataSources(
                alertsIndex = customAlertsIndex1,
                findingsIndex = customFindingsIndex1,
                findingsIndexPattern = customFindingsIndexPattern1
            )
        )

        val monitorResponse2 = createMonitor(monitor2)!!
        val docQuery3 = DocLevelQuery(query = "_id:*", name = "5", fields = listOf())
        val docLevelInput3 = DocLevelMonitorInput("description", listOf(index), listOf(docQuery3))
        val trigger3 = randomDocumentLevelTrigger(condition = ALWAYS_RUN)

        var monitor3 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput3),
            triggers = listOf(trigger3),
            enabled = false,
            dataSources = DataSources(
                alertsIndex = customAlertsIndex1,
                findingsIndex = customFindingsIndex1,
                findingsIndexPattern = customFindingsIndexPattern1
            )
        )

        val monitorResponse3 = createMonitor(monitor3)!!
        val d1 = Delegate(1, monitorResponse.id)
        val d2 = Delegate(2, monitorResponse2.id)
        val d3 = Delegate(
            3, monitorResponse3.id,
            ChainedMonitorFindings(null, listOf(monitorResponse.id, monitorResponse2.id))
        )
        var workflow = Workflow(
            id = "",
            name = "test",
            enabled = false,
            schedule = IntervalSchedule(interval = 5, unit = ChronoUnit.MINUTES),
            lastUpdateTime = Instant.now(),
            enabledTime = null,
            workflowType = Workflow.WorkflowType.COMPOSITE,
            user = randomUser(),
            inputs = listOf(CompositeInput(org.opensearch.commons.alerting.model.Sequence(listOf(d1, d2, d3)))),
            version = -1L,
            schemaVersion = 0,
            triggers = emptyList(),
            auditDelegateMonitorAlerts = false

        )
        val workflowResponse = upsertWorkflow(workflow)!!
        val workflowById = searchWorkflow(workflowResponse.id)
        assertNotNull(workflowById)

        var testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(ChronoUnit.MILLIS))
        // Matches monitor1
        val testDoc1 = """{
            "message" : "This is an error from IAD region",
            "source.ip.v6.v2" : 16644, 
            "test_strict_date_time" : "$testTime",
            "test_field_1" : "us-west-2"
        }"""
        indexDoc(index, "1", testDoc1)

        testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(ChronoUnit.MILLIS))
        // Matches monitor1 and monitor2
        val testDoc2 = """{
            "message" : "This is an error from IAD region",
            "source.ip.v6.v2" : 16645, 
            "test_strict_date_time" : "$testTime",
            "test_field_1" : "us-west-2"
        }"""
        indexDoc(index, "2", testDoc2)

        testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(ChronoUnit.MILLIS))
        // Matches monitor1 and monitor2
        val testDoc3 = """{
            "message" : "This is an error from IAD region",
            "source.ip.v6.v2" : 16645, 
            "test_strict_date_time" : "$testTime",
            "test_field_1" : "us-east-1"
        }"""
        indexDoc(index, "3", testDoc3)

        val workflowId = workflowResponse.id
        val executeWorkflowResponse = executeWorkflow(workflowById, workflowId, false)!!
        val monitorsRunResults = executeWorkflowResponse.workflowRunResult.monitorRunResults
        assertEquals(3, monitorsRunResults.size)
        assertFindings(monitorResponse.id, customFindingsIndex1, 2, 2, listOf("1", "2"))
        assertFindings(monitorResponse2.id, customFindingsIndex1, 2, 2, listOf("2", "3"))
        assertFindings(monitorResponse3.id, customFindingsIndex1, 3, 3, listOf("1", "2", "3"))
    }

    fun `test execute workflows with shared doc level monitor delegate`() {
        val docQuery = DocLevelQuery(query = "test_field_1:\"us-west-2\"", name = "3", fields = listOf())
        val docLevelInput = DocLevelMonitorInput("description", listOf(index), listOf(docQuery))
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val customAlertsIndex = "custom_alerts_index"
        val customFindingsIndex = "custom_findings_index"
        val customFindingsIndexPattern = "custom_findings_index-1"
        var monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
            dataSources = DataSources(
                alertsIndex = customAlertsIndex,
                findingsIndex = customFindingsIndex,
                findingsIndexPattern = customFindingsIndexPattern
            )
        )
        val monitorResponse = createMonitor(monitor)!!

        var workflow = randomWorkflow(
            monitorIds = listOf(monitorResponse.id),
            auditDelegateMonitorAlerts = false
        )
        val workflowResponse = upsertWorkflow(workflow)!!
        val workflowById = searchWorkflow(workflowResponse.id)
        assertNotNull(workflowById)

        var workflow1 = randomWorkflow(
            monitorIds = listOf(monitorResponse.id),
            auditDelegateMonitorAlerts = false
        )
        val workflowResponse1 = upsertWorkflow(workflow1)!!
        val workflowById1 = searchWorkflow(workflowResponse1.id)
        assertNotNull(workflowById1)

        var testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(ChronoUnit.MILLIS))
        // Matches monitor1
        val testDoc1 = """{
            "message" : "This is an error from IAD region",
            "source.ip.v6.v2" : 16644, 
            "test_strict_date_time" : "$testTime",
            "test_field_1" : "us-west-2"
        }"""
        indexDoc(index, "1", testDoc1)

        testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(ChronoUnit.MILLIS))
        val testDoc2 = """{
            "message" : "This is an error from IAD region",
            "source.ip.v6.v2" : 16645, 
            "test_strict_date_time" : "$testTime",
            "test_field_1" : "us-west-2"
        }"""
        indexDoc(index, "2", testDoc2)

        val workflowId = workflowResponse.id
        val executeWorkflowResponse = executeWorkflow(workflowById, workflowId, false)!!
        val monitorsRunResults = executeWorkflowResponse.workflowRunResult.monitorRunResults
        assertEquals(1, monitorsRunResults.size)

        assertEquals(monitor.name, monitorsRunResults[0].monitorName)
        assertEquals(1, monitorsRunResults[0].triggerResults.size)

        // Assert and not ack the alerts (in order to verify later on that all the alerts are generated)
        assertAlerts(monitorResponse.id, customAlertsIndex, alertSize = 2, workflowId)
        assertFindings(monitorResponse.id, customFindingsIndex, 2, 2, listOf("1", "2"))
        // Verify workflow and monitor delegate metadata
        val workflowMetadata = searchWorkflowMetadata(id = workflowId)
        assertNotNull("Workflow metadata not initialized", workflowMetadata)
        assertEquals(
            "Workflow metadata execution id not correct",
            executeWorkflowResponse.workflowRunResult.executionId,
            workflowMetadata!!.latestExecutionId
        )
        val monitorMetadataId = getDelegateMonitorMetadataId(workflowMetadata, monitorResponse)
        val monitorMetadata = searchMonitorMetadata(monitorMetadataId)
        assertNotNull(monitorMetadata)

        // Execute second workflow
        val workflowId1 = workflowResponse1.id
        val executeWorkflowResponse1 = executeWorkflow(workflowById1, workflowId1, false)!!
        val monitorsRunResults1 = executeWorkflowResponse1.workflowRunResult.monitorRunResults
        assertEquals(1, monitorsRunResults1.size)

        assertEquals(monitor.name, monitorsRunResults1[0].monitorName)
        assertEquals(1, monitorsRunResults1[0].triggerResults.size)

        val getAlertsResponse = assertAlerts(monitorResponse.id, customAlertsIndex, alertSize = 2, workflowId1)
        assertAcknowledges(getAlertsResponse.alerts, monitorResponse.id, 2)
        assertFindings(monitorResponse.id, customFindingsIndex, 4, 4, listOf("1", "2", "1", "2"))
        // Verify workflow and monitor delegate metadata
        val workflowMetadata1 = searchWorkflowMetadata(id = workflowId1)
        assertNotNull("Workflow metadata not initialized", workflowMetadata1)
        assertEquals(
            "Workflow metadata execution id not correct",
            executeWorkflowResponse1.workflowRunResult.executionId,
            workflowMetadata1!!.latestExecutionId
        )
        val monitorMetadataId1 = getDelegateMonitorMetadataId(workflowMetadata1, monitorResponse)
        val monitorMetadata1 = searchMonitorMetadata(monitorMetadataId1)
        assertNotNull(monitorMetadata1)
        // Verify that for two workflows two different doc level monitor metadata has been created
        assertTrue("Different monitor is used in workflows", monitorMetadata!!.monitorId == monitorMetadata1!!.monitorId)
        assertTrue(monitorMetadata.id != monitorMetadata1.id)
    }

    fun `test execute workflows with shared doc level monitor delegate updating delegate datasource`() {
        val docQuery = DocLevelQuery(query = "test_field_1:\"us-west-2\"", name = "3", fields = listOf())
        val docLevelInput = DocLevelMonitorInput("description", listOf(index), listOf(docQuery))
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)

        var monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )
        val monitorResponse = createMonitor(monitor)!!

        val workflow = randomWorkflow(
            monitorIds = listOf(monitorResponse.id), auditDelegateMonitorAlerts = false
        )
        val workflowResponse = upsertWorkflow(workflow)!!
        val workflowById = searchWorkflow(workflowResponse.id)
        assertNotNull(workflowById)

        val workflow1 = randomWorkflow(
            monitorIds = listOf(monitorResponse.id), auditDelegateMonitorAlerts = false
        )
        val workflowResponse1 = upsertWorkflow(workflow1)!!
        val workflowById1 = searchWorkflow(workflowResponse1.id)
        assertNotNull(workflowById1)

        var testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(ChronoUnit.MILLIS))
        // Matches monitor1
        val testDoc1 = """{
            "message" : "This is an error from IAD region",
            "source.ip.v6.v2" : 16644, 
            "test_strict_date_time" : "$testTime",
            "test_field_1" : "us-west-2"
        }"""
        indexDoc(index, "1", testDoc1)

        testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(ChronoUnit.MILLIS))
        val testDoc2 = """{
            "message" : "This is an error from IAD region",
            "source.ip.v6.v2" : 16645, 
            "test_strict_date_time" : "$testTime",
            "test_field_1" : "us-west-2"
        }"""
        indexDoc(index, "2", testDoc2)

        val workflowId = workflowResponse.id
        val executeWorkflowResponse = executeWorkflow(workflowById, workflowId, false)!!
        val monitorsRunResults = executeWorkflowResponse.workflowRunResult.monitorRunResults
        assertEquals(1, monitorsRunResults.size)

        assertEquals(monitor.name, monitorsRunResults[0].monitorName)
        assertEquals(1, monitorsRunResults[0].triggerResults.size)

        assertAlerts(monitorResponse.id, AlertIndices.ALERT_INDEX, alertSize = 2, workflowId)
        assertFindings(monitorResponse.id, AlertIndices.FINDING_HISTORY_WRITE_INDEX, 2, 2, listOf("1", "2"))
        // Verify workflow and monitor delegate metadata
        val workflowMetadata = searchWorkflowMetadata(id = workflowId)
        assertNotNull("Workflow metadata not initialized", workflowMetadata)
        assertEquals(
            "Workflow metadata execution id not correct",
            executeWorkflowResponse.workflowRunResult.executionId,
            workflowMetadata!!.latestExecutionId
        )
        val monitorMetadataId = getDelegateMonitorMetadataId(workflowMetadata, monitorResponse)
        val monitorMetadata = searchMonitorMetadata(monitorMetadataId)
        assertNotNull(monitorMetadata)

        val customAlertsIndex = "custom_alerts_index"
        val customFindingsIndex = "custom_findings_index"
        val customFindingsIndexPattern = "custom_findings_index-1"
        val monitorId = monitorResponse.id
        updateMonitor(
            monitor = monitor.copy(
                dataSources = DataSources(
                    alertsIndex = customAlertsIndex,
                    findingsIndex = customFindingsIndex,
                    findingsIndexPattern = customFindingsIndexPattern
                )
            ),
            monitorId
        )

        // Execute second workflow
        val workflowId1 = workflowResponse1.id
        val executeWorkflowResponse1 = executeWorkflow(workflowById1, workflowId1, false)!!
        val monitorsRunResults1 = executeWorkflowResponse1.workflowRunResult.monitorRunResults
        assertEquals(1, monitorsRunResults1.size)

        assertEquals(monitor.name, monitorsRunResults1[0].monitorName)
        assertEquals(1, monitorsRunResults1[0].triggerResults.size)

        // Verify alerts for the custom index
        val getAlertsResponse = assertAlerts(monitorResponse.id, customAlertsIndex, alertSize = 2, workflowId1)
        assertAcknowledges(getAlertsResponse.alerts, monitorResponse.id, 2)
        assertFindings(monitorResponse.id, customFindingsIndex, 2, 2, listOf("1", "2"))

        // Verify workflow and monitor delegate metadata
        val workflowMetadata1 = searchWorkflowMetadata(id = workflowId1)
        assertNotNull("Workflow metadata not initialized", workflowMetadata1)
        assertEquals(
            "Workflow metadata execution id not correct",
            executeWorkflowResponse1.workflowRunResult.executionId,
            workflowMetadata1!!.latestExecutionId
        )
        val monitorMetadataId1 = getDelegateMonitorMetadataId(workflowMetadata1, monitorResponse)
        val monitorMetadata1 = searchMonitorMetadata(monitorMetadataId1)
        assertNotNull(monitorMetadata1)
        // Verify that for two workflows two different doc level monitor metadata has been created
        assertTrue("Different monitor is used in workflows", monitorMetadata!!.monitorId == monitorMetadata1!!.monitorId)
        assertTrue(monitorMetadata.id != monitorMetadata1.id)
    }

    fun `test execute workflow verify workflow metadata`() {
        val docQuery1 = DocLevelQuery(query = "test_field_1:\"us-west-2\"", name = "3", fields = listOf())
        val docLevelInput1 = DocLevelMonitorInput("description", listOf(index), listOf(docQuery1))
        val trigger1 = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        var monitor1 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput1),
            triggers = listOf(trigger1)
        )
        val monitorResponse = createMonitor(monitor1)!!

        val docQuery2 = DocLevelQuery(query = "source.ip.v6.v2:16645", name = "4", fields = listOf())
        val docLevelInput2 = DocLevelMonitorInput("description", listOf(index), listOf(docQuery2))
        val trigger2 = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        var monitor2 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput2),
            triggers = listOf(trigger2),
        )

        val monitorResponse2 = createMonitor(monitor2)!!

        var workflow = randomWorkflow(
            monitorIds = listOf(monitorResponse.id, monitorResponse2.id)
        )
        val workflowResponse = upsertWorkflow(workflow)!!
        val workflowById = searchWorkflow(workflowResponse.id)
        assertNotNull(workflowById)

        var testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(ChronoUnit.MILLIS))
        // Matches monitor1
        val testDoc1 = """{
            "message" : "This is an error from IAD region",
            "source.ip.v6.v2" : 16644, 
            "test_strict_date_time" : "$testTime",
            "test_field_1" : "us-west-2"
        }"""
        indexDoc(index, "1", testDoc1)
        // First execution
        val workflowId = workflowResponse.id
        val executeWorkflowResponse = executeWorkflow(workflowById, workflowId, false)!!
        val monitorsRunResults = executeWorkflowResponse.workflowRunResult.monitorRunResults
        assertEquals(2, monitorsRunResults.size)

        val workflowMetadata = searchWorkflowMetadata(id = workflowId)
        assertNotNull("Workflow metadata not initialized", workflowMetadata)
        assertEquals(
            "Workflow metadata execution id not correct",
            executeWorkflowResponse.workflowRunResult.executionId,
            workflowMetadata!!.latestExecutionId
        )
        val monitorMetadataId = getDelegateMonitorMetadataId(workflowMetadata, monitorResponse)
        val monitorMetadata = searchMonitorMetadata(monitorMetadataId)
        assertNotNull(monitorMetadata)

        // Second execution
        val executeWorkflowResponse1 = executeWorkflow(workflowById, workflowId, false)!!
        val monitorsRunResults1 = executeWorkflowResponse1.workflowRunResult.monitorRunResults
        assertEquals(2, monitorsRunResults1.size)

        val workflowMetadata1 = searchWorkflowMetadata(id = workflowId)
        assertNotNull("Workflow metadata not initialized", workflowMetadata)
        assertEquals(
            "Workflow metadata execution id not correct",
            executeWorkflowResponse1.workflowRunResult.executionId,
            workflowMetadata1!!.latestExecutionId
        )
        val monitorMetadataId1 = getDelegateMonitorMetadataId(workflowMetadata1, monitorResponse)
        assertTrue(monitorMetadataId == monitorMetadataId1)
        val monitorMetadata1 = searchMonitorMetadata(monitorMetadataId1)
        assertNotNull(monitorMetadata1)
    }

    fun `test execute workflow dryrun verify workflow metadata not created`() {
        val docQuery1 = DocLevelQuery(query = "test_field_1:\"us-west-2\"", name = "3", fields = listOf())
        val docLevelInput1 = DocLevelMonitorInput("description", listOf(index), listOf(docQuery1))
        val trigger1 = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        var monitor1 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput1),
            triggers = listOf(trigger1)
        )
        val monitorResponse = createMonitor(monitor1)!!

        val docQuery2 = DocLevelQuery(query = "source.ip.v6.v2:16645", name = "4", fields = listOf())
        val docLevelInput2 = DocLevelMonitorInput("description", listOf(index), listOf(docQuery2))
        val trigger2 = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        var monitor2 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput2),
            triggers = listOf(trigger2),
        )

        val monitorResponse2 = createMonitor(monitor2)!!

        var workflow = randomWorkflow(
            monitorIds = listOf(monitorResponse.id, monitorResponse2.id)
        )
        val workflowResponse = upsertWorkflow(workflow)!!
        val workflowById = searchWorkflow(workflowResponse.id)
        assertNotNull(workflowById)

        var testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(ChronoUnit.MILLIS))
        // Matches monitor1
        val testDoc1 = """{
            "message" : "This is an error from IAD region",
            "source.ip.v6.v2" : 16644, 
            "test_strict_date_time" : "$testTime",
            "test_field_1" : "us-west-2"
        }"""
        indexDoc(index, "1", testDoc1)
        // First execution
        val workflowId = workflowResponse.id
        val executeWorkflowResponse = executeWorkflow(workflowById, workflowId, true)

        assertNotNull("Workflow run result is null", executeWorkflowResponse)
        val monitorsRunResults = executeWorkflowResponse!!.workflowRunResult.monitorRunResults
        assertEquals(2, monitorsRunResults.size)

        var exception: java.lang.Exception? = null
        try {
            searchWorkflowMetadata(id = workflowId)
        } catch (ex: java.lang.Exception) {
            exception = ex
            assertTrue(exception is java.util.NoSuchElementException)
        }
    }

    fun `test execute workflow with custom alerts and finding index with bucket and doc monitor bucket monitor used as chained finding`() {
        val query = QueryBuilders.rangeQuery("test_strict_date_time")
            .gt("{{period_end}}||-10d")
            .lte("{{period_end}}")
            .format("epoch_millis")
        val compositeSources = listOf(
            TermsValuesSourceBuilder("test_field_1").field("test_field_1")
        )
        val compositeAgg = CompositeAggregationBuilder("composite_agg", compositeSources)
        val input = SearchInput(indices = listOf(index), query = SearchSourceBuilder().size(0).query(query).aggregation(compositeAgg))
        // Bucket level monitor will reduce the size of matched doc ids on those that belong to a bucket that contains more than 1 document after term grouping
        val triggerScript = """
            params.docCount > 1
        """.trimIndent()

        var trigger = randomBucketLevelTrigger()
        trigger = trigger.copy(
            bucketSelector = BucketSelectorExtAggregationBuilder(
                name = trigger.id,
                bucketsPathsMap = mapOf("docCount" to "_count"),
                script = Script(triggerScript),
                parentBucketPath = "composite_agg",
                filter = null,
            )
        )
        val bucketCustomAlertsIndex = "custom_alerts_index"
        val bucketCustomFindingsIndex = "custom_findings_index"
        val bucketCustomFindingsIndexPattern = "custom_findings_index-1"

        val bucketLevelMonitorResponse = createMonitor(
            randomBucketLevelMonitor(
                inputs = listOf(input),
                enabled = false,
                triggers = listOf(trigger),
                dataSources = DataSources(
                    findingsEnabled = true,
                    alertsIndex = bucketCustomAlertsIndex,
                    findingsIndex = bucketCustomFindingsIndex,
                    findingsIndexPattern = bucketCustomFindingsIndexPattern
                )
            )
        )!!

        val docQuery1 = DocLevelQuery(query = "test_field_1:\"test_value_2\"", name = "1", fields = listOf())
        val docQuery2 = DocLevelQuery(query = "test_field_1:\"test_value_1\"", name = "2", fields = listOf())
        val docQuery3 = DocLevelQuery(query = "test_field_1:\"test_value_3\"", name = "3", fields = listOf())
        val docLevelInput = DocLevelMonitorInput("description", listOf(index), listOf(docQuery1, docQuery2, docQuery3))
        val docTrigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val docCustomAlertsIndex = "custom_alerts_index"
        val docCustomFindingsIndex = "custom_findings_index"
        val docCustomFindingsIndexPattern = "custom_findings_index-1"
        var docLevelMonitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(docTrigger),
            dataSources = DataSources(
                alertsIndex = docCustomAlertsIndex,
                findingsIndex = docCustomFindingsIndex,
                findingsIndexPattern = docCustomFindingsIndexPattern
            )
        )

        val docLevelMonitorResponse = createMonitor(docLevelMonitor)!!
        // 1. bucketMonitor (chainedFinding = null) 2. docMonitor (chainedFinding = bucketMonitor)
        var workflow = randomWorkflow(
            monitorIds = listOf(bucketLevelMonitorResponse.id, docLevelMonitorResponse.id), auditDelegateMonitorAlerts = false
        )
        val workflowResponse = upsertWorkflow(workflow)!!
        val workflowById = searchWorkflow(workflowResponse.id)
        assertNotNull(workflowById)

        // Creates 5 documents
        insertSampleTimeSerializedData(
            index,
            listOf(
                "test_value_1",
                "test_value_1", // adding duplicate to verify aggregation
                "test_value_2",
                "test_value_2",
                "test_value_3"
            )
        )

        val workflowId = workflowResponse.id
        // 1. bucket level monitor should reduce the doc findings to 4 (1, 2, 3, 4)
        // 2. Doc level monitor will match those 4 documents although it contains rules for matching all 5 documents (docQuery3 matches the fifth)
        val executeWorkflowResponse = executeWorkflow(workflowById, workflowId, false)!!
        assertNotNull(executeWorkflowResponse)

        for (monitorRunResults in executeWorkflowResponse.workflowRunResult.monitorRunResults) {
            if (bucketLevelMonitorResponse.monitor.name == monitorRunResults.monitorName) {
                val searchResult = monitorRunResults.inputResults.results.first()

                @Suppress("UNCHECKED_CAST")
                val buckets = searchResult.stringMap("aggregations")?.stringMap("composite_agg")
                    ?.get("buckets") as List<kotlin.collections.Map<String, Any>>
                assertEquals("Incorrect search result", 3, buckets.size)

                val getAlertsResponse = assertAlerts(bucketLevelMonitorResponse.id, bucketCustomAlertsIndex, alertSize = 2, workflowId)
                assertAcknowledges(getAlertsResponse.alerts, bucketLevelMonitorResponse.id, 2)
                assertFindings(bucketLevelMonitorResponse.id, bucketCustomFindingsIndex, 1, 4, listOf("1", "2", "3", "4"))
            } else {
                assertEquals(1, monitorRunResults.inputResults.results.size)
                val values = monitorRunResults.triggerResults.values
                assertEquals(1, values.size)
                @Suppress("UNCHECKED_CAST")
                val docLevelTrigger = values.iterator().next() as DocumentLevelTriggerRunResult
                val triggeredDocIds = docLevelTrigger.triggeredDocs.map { it.split("|")[0] }
                val expectedTriggeredDocIds = listOf("1", "2", "3", "4")
                assertEquals(expectedTriggeredDocIds, triggeredDocIds.sorted())

                val getAlertsResponse = assertAlerts(docLevelMonitorResponse.id, docCustomAlertsIndex, alertSize = 4, workflowId)
                assertAcknowledges(getAlertsResponse.alerts, docLevelMonitorResponse.id, 4)
                assertFindings(docLevelMonitorResponse.id, docCustomFindingsIndex, 4, 4, listOf("1", "2", "3", "4"))
            }
        }
    }

    fun `test chained alerts for bucket level monitors generating audit alerts custom alerts index`() {
        val customAlertIndex = "custom-alert-index"
        val customAlertHistoryIndex = "custom-alert-history-index"
        val customAlertHistoryIndexPattern = "<custom_alert_history_index-{now/d}-1>"
        val query = QueryBuilders.rangeQuery("test_strict_date_time")
            .gt("{{period_end}}||-10d")
            .lte("{{period_end}}")
            .format("epoch_millis")
        val compositeSources = listOf(
            TermsValuesSourceBuilder("test_field_1").field("test_field_1")
        )
        val compositeAgg = CompositeAggregationBuilder("composite_agg", compositeSources)
        val input = SearchInput(indices = listOf(index), query = SearchSourceBuilder().size(0).query(query).aggregation(compositeAgg))
        // Bucket level monitor will reduce the size of matched doc ids on those that belong to a bucket that contains more than 1 document after term grouping
        val triggerScript = """
            params.docCount > 1
        """.trimIndent()

        var trigger = randomBucketLevelTrigger()
        trigger = trigger.copy(
            bucketSelector = BucketSelectorExtAggregationBuilder(
                name = trigger.id,
                bucketsPathsMap = mapOf("docCount" to "_count"),
                script = Script(triggerScript),
                parentBucketPath = "composite_agg",
                filter = null,
            )
        )

        val bucketLevelMonitorResponse = createMonitor(
            randomBucketLevelMonitor(
                inputs = listOf(input),
                enabled = false,
                triggers = listOf(trigger),
                dataSources = DataSources(
                    alertsIndex = customAlertIndex,
                    alertsHistoryIndexPattern = customAlertHistoryIndexPattern,
                    alertsHistoryIndex = customAlertHistoryIndex

                )
            )
        )!!

        val bucketLevelMonitorResponse2 = createMonitor(
            randomBucketLevelMonitor(
                inputs = listOf(input),
                enabled = false,
                triggers = listOf(trigger),
                dataSources = DataSources(
                    alertsIndex = customAlertIndex,
                    alertsHistoryIndexPattern = customAlertHistoryIndexPattern,
                    alertsHistoryIndex = customAlertHistoryIndex

                )
            )
        )!!

        val andTrigger = randomChainedAlertTrigger(
            name = "1And2",
            condition = Script("monitor[id=${bucketLevelMonitorResponse.id}] && monitor[id=${bucketLevelMonitorResponse2.id}]")
        )
        // 1. bucketMonitor (chainedFinding = null) 2. docMonitor (chainedFinding = bucketMonitor)
        var workflow = randomWorkflow(
            monitorIds = listOf(bucketLevelMonitorResponse.id, bucketLevelMonitorResponse2.id),
            triggers = listOf(andTrigger)
        )
        val workflowResponse = upsertWorkflow(workflow)!!
        val workflowById = searchWorkflow(workflowResponse.id)
        assertNotNull(workflowById)

        // Creates 5 documents
        insertSampleTimeSerializedData(
            index,
            listOf(
                "test_value_1",
                "test_value_1", // adding duplicate to verify aggregation
                "test_value_2",
                "test_value_2",
                "test_value_3"
            )
        )

        val workflowId = workflowResponse.id
        // 1. bucket level monitor should reduce the doc findings to 4 (1, 2, 3, 4)
        // 2. Doc level monitor will match those 4 documents although it contains rules for matching all 5 documents (docQuery3 matches the fifth)
        val executeWorkflowResponse = executeWorkflow(workflowById, workflowId, false)!!
        assertNotNull(executeWorkflowResponse)

        Assert.assertTrue(executeWorkflowResponse.workflowRunResult.triggerResults.isNotEmpty())
        Assert.assertTrue(executeWorkflowResponse.workflowRunResult.triggerResults.containsKey(andTrigger.id))
        Assert.assertTrue(executeWorkflowResponse.workflowRunResult.triggerResults[andTrigger.id]!!.triggered)

        val auditStateAlerts = getAuditStateAlerts(
            alertsIndex = customAlertHistoryIndex,
            monitorId = bucketLevelMonitorResponse.id,
            executionId = executeWorkflowResponse.workflowRunResult.executionId
        )
        Assert.assertEquals(auditStateAlerts.size, 2)

        val auditStateAlerts2 = getAuditStateAlerts(
            alertsIndex = customAlertHistoryIndex,
            monitorId = bucketLevelMonitorResponse2.id,
            executionId = executeWorkflowResponse.workflowRunResult.executionId
        )
        Assert.assertEquals(auditStateAlerts2.size, 2)
    }

    fun `test chained alerts for bucket level monitors generating audit alerts`() {
        val query = QueryBuilders.rangeQuery("test_strict_date_time")
            .gt("{{period_end}}||-10d")
            .lte("{{period_end}}")
            .format("epoch_millis")
        val compositeSources = listOf(
            TermsValuesSourceBuilder("test_field_1").field("test_field_1")
        )
        val compositeAgg = CompositeAggregationBuilder("composite_agg", compositeSources)
        val input = SearchInput(indices = listOf(index), query = SearchSourceBuilder().size(0).query(query).aggregation(compositeAgg))
        // Bucket level monitor will reduce the size of matched doc ids on those that belong to a bucket that contains more than 1 document after term grouping
        val triggerScript = """
            params.docCount > 1
        """.trimIndent()

        var trigger = randomBucketLevelTrigger()
        trigger = trigger.copy(
            bucketSelector = BucketSelectorExtAggregationBuilder(
                name = trigger.id,
                bucketsPathsMap = mapOf("docCount" to "_count"),
                script = Script(triggerScript),
                parentBucketPath = "composite_agg",
                filter = null,
            )
        )

        val bucketLevelMonitorResponse = createMonitor(
            randomBucketLevelMonitor(
                inputs = listOf(input),
                enabled = false,
                triggers = listOf(trigger)
            )
        )!!

        val bucketLevelMonitorResponse2 = createMonitor(
            randomBucketLevelMonitor(
                inputs = listOf(input),
                enabled = false,
                triggers = listOf(trigger)
            )
        )!!

        val andTrigger = randomChainedAlertTrigger(
            name = "1And2",
            condition = Script("monitor[id=${bucketLevelMonitorResponse.id}] && monitor[id=${bucketLevelMonitorResponse2.id}]")
        )
        // 1. bucketMonitor (chainedFinding = null) 2. docMonitor (chainedFinding = bucketMonitor)
        var workflow = randomWorkflow(
            monitorIds = listOf(bucketLevelMonitorResponse.id, bucketLevelMonitorResponse2.id),
            triggers = listOf(andTrigger)
        )
        val workflowResponse = upsertWorkflow(workflow)!!
        val workflowById = searchWorkflow(workflowResponse.id)
        assertNotNull(workflowById)

        // Creates 5 documents
        insertSampleTimeSerializedData(
            index,
            listOf(
                "test_value_1",
                "test_value_1", // adding duplicate to verify aggregation
                "test_value_2",
                "test_value_2",
                "test_value_3"
            )
        )

        val workflowId = workflowResponse.id
        // 1. bucket level monitor should reduce the doc findings to 4 (1, 2, 3, 4)
        // 2. Doc level monitor will match those 4 documents although it contains rules for matching all 5 documents (docQuery3 matches the fifth)
        val executeWorkflowResponse = executeWorkflow(workflowById, workflowId, false)!!
        assertNotNull(executeWorkflowResponse)

        Assert.assertTrue(executeWorkflowResponse.workflowRunResult.triggerResults.isNotEmpty())
        Assert.assertTrue(executeWorkflowResponse.workflowRunResult.triggerResults.containsKey(andTrigger.id))
        Assert.assertTrue(executeWorkflowResponse.workflowRunResult.triggerResults[andTrigger.id]!!.triggered)

        val auditStateAlerts = getAuditStateAlerts(
            alertsIndex = bucketLevelMonitorResponse.monitor.dataSources.alertsHistoryIndex,
            monitorId = bucketLevelMonitorResponse.id,
            executionId = executeWorkflowResponse.workflowRunResult.executionId
        )
        Assert.assertEquals(auditStateAlerts.size, 2)

        val auditStateAlerts2 = getAuditStateAlerts(
            alertsIndex = bucketLevelMonitorResponse.monitor.dataSources.alertsHistoryIndex,
            monitorId = bucketLevelMonitorResponse2.id,
            executionId = executeWorkflowResponse.workflowRunResult.executionId
        )
        Assert.assertEquals(auditStateAlerts2.size, 2)
    }

    fun `test execute with custom alerts and finding index with bucket and doc monitor when doc monitor  is used in chained finding`() {
        val docQuery1 = DocLevelQuery(query = "test_field_1:\"test_value_2\"", name = "1", fields = listOf())
        val docQuery2 = DocLevelQuery(query = "test_field_1:\"test_value_3\"", name = "2", fields = listOf())

        var docLevelMonitor = randomDocumentLevelMonitor(
            inputs = listOf(DocLevelMonitorInput("description", listOf(index), listOf(docQuery1, docQuery2))),
            triggers = listOf(randomDocumentLevelTrigger(condition = ALWAYS_RUN)),
            dataSources = DataSources(
                alertsIndex = "custom_alerts_index",
                findingsIndex = "custom_findings_index",
                findingsIndexPattern = "custom_findings_index-1"
            )
        )

        val docLevelMonitorResponse = createMonitor(docLevelMonitor)!!

        val query = QueryBuilders.rangeQuery("test_strict_date_time")
            .gt("{{period_end}}||-10d")
            .lte("{{period_end}}")
            .format("epoch_millis")
        val compositeSources = listOf(
            TermsValuesSourceBuilder("test_field_1").field("test_field_1")
        )
        val compositeAgg = CompositeAggregationBuilder("composite_agg", compositeSources)
        val input = SearchInput(indices = listOf(index), query = SearchSourceBuilder().size(0).query(query).aggregation(compositeAgg))
        // Bucket level monitor will reduce the size of matched doc ids on those that belong to a bucket that contains more than 1 document after term grouping
        val triggerScript = """
            params.docCount > 1
        """.trimIndent()

        var trigger = randomBucketLevelTrigger()
        trigger = trigger.copy(
            bucketSelector = BucketSelectorExtAggregationBuilder(
                name = trigger.id,
                bucketsPathsMap = mapOf("docCount" to "_count"),
                script = Script(triggerScript),
                parentBucketPath = "composite_agg",
                filter = null,
            )
        )

        val bucketLevelMonitorResponse = createMonitor(
            randomBucketLevelMonitor(
                inputs = listOf(input),
                enabled = false,
                triggers = listOf(trigger),
                dataSources = DataSources(
                    findingsEnabled = true,
                    alertsIndex = "custom_alerts_index",
                    findingsIndex = "custom_findings_index",
                    findingsIndexPattern = "custom_findings_index-1"
                )
            )
        )!!

        var docLevelMonitor1 = randomDocumentLevelMonitor(
            // Match the documents with test_field_1: test_value_3
            inputs = listOf(DocLevelMonitorInput("description", listOf(index), listOf(docQuery2))),
            triggers = listOf(randomDocumentLevelTrigger(condition = ALWAYS_RUN)),
            dataSources = DataSources(
                findingsEnabled = true,
                alertsIndex = "custom_alerts_index_1",
                findingsIndex = "custom_findings_index_1",
                findingsIndexPattern = "custom_findings_index_1-1"
            )
        )

        val docLevelMonitorResponse1 = createMonitor(docLevelMonitor1)!!

        val queryMonitorInput = SearchInput(
            indices = listOf(index),
            query = SearchSourceBuilder().query(
                QueryBuilders
                    .rangeQuery("test_strict_date_time")
                    .gt("{{period_end}}||-10d")
                    .lte("{{period_end}}")
                    .format("epoch_millis")
            )
        )
        val queryTriggerScript = """
            return ctx.results[0].hits.hits.size() > 0
        """.trimIndent()

        val queryLevelTrigger = randomQueryLevelTrigger(condition = Script(queryTriggerScript))
        val queryMonitorResponse =
            createMonitor(randomQueryLevelMonitor(inputs = listOf(queryMonitorInput), triggers = listOf(queryLevelTrigger)))!!

        // 1. docMonitor (chainedFinding = null) 2. bucketMonitor (chainedFinding = docMonitor) 3. docMonitor (chainedFinding = bucketMonitor) 4. queryMonitor (chainedFinding = docMonitor 3)
        var workflow = randomWorkflow(
            monitorIds = listOf(
                docLevelMonitorResponse.id,
                bucketLevelMonitorResponse.id,
                docLevelMonitorResponse1.id,
                queryMonitorResponse.id
            ),
            auditDelegateMonitorAlerts = false
        )
        val workflowResponse = upsertWorkflow(workflow)!!
        val workflowById = searchWorkflow(workflowResponse.id)
        assertNotNull(workflowById)

        // Creates 5 documents
        insertSampleTimeSerializedData(
            index,
            listOf(
                "test_value_1",
                "test_value_1", // adding duplicate to verify aggregation
                "test_value_2",
                "test_value_2",
                "test_value_3",
                "test_value_3"
            )
        )

        val workflowId = workflowResponse.id
        // 1. Doc level monitor should reduce the doc findings to 4 (3 - test_value_2, 4 - test_value_2, 5 - test_value_3, 6 - test_value_3)
        // 2. Bucket level monitor will match the fetch the docs from current findings execution, although it contains rules for matching documents which has test_value_2 and test value_3
        val executeWorkflowResponse = executeWorkflow(workflowById, workflowId, false)!!
        assertNotNull(executeWorkflowResponse)

        for (monitorRunResults in executeWorkflowResponse.workflowRunResult.monitorRunResults) {
            when (monitorRunResults.monitorName) {
                // Verify first doc level monitor execution, alerts and findings
                docLevelMonitorResponse.monitor.name -> {
                    assertEquals(1, monitorRunResults.inputResults.results.size)
                    val values = monitorRunResults.triggerResults.values
                    assertEquals(1, values.size)
                    @Suppress("UNCHECKED_CAST")
                    val docLevelTrigger = values.iterator().next() as DocumentLevelTriggerRunResult
                    val triggeredDocIds = docLevelTrigger.triggeredDocs.map { it.split("|")[0] }
                    val expectedTriggeredDocIds = listOf("3", "4", "5", "6")
                    assertEquals(expectedTriggeredDocIds, triggeredDocIds.sorted())

                    val getAlertsResponse =
                        assertAlerts(
                            docLevelMonitorResponse.id,
                            docLevelMonitorResponse.monitor.dataSources.alertsIndex,
                            alertSize = 4,
                            workflowId = workflowId
                        )
                    assertAcknowledges(getAlertsResponse.alerts, docLevelMonitorResponse.id, 4)
                    assertFindings(
                        docLevelMonitorResponse.id,
                        docLevelMonitorResponse.monitor.dataSources.findingsIndex,
                        4,
                        4,
                        listOf("3", "4", "5", "6")
                    )
                }
                // Verify second bucket level monitor execution, alerts and findings
                bucketLevelMonitorResponse.monitor.name -> {
                    val searchResult = monitorRunResults.inputResults.results.first()

                    @Suppress("UNCHECKED_CAST")
                    val buckets =
                        searchResult
                            .stringMap("aggregations")?.stringMap("composite_agg")
                            ?.get("buckets") as List<kotlin.collections.Map<String, Any>>
                    assertEquals("Incorrect search result", 2, buckets.size)

                    val getAlertsResponse =
                        assertAlerts(
                            bucketLevelMonitorResponse.id,
                            bucketLevelMonitorResponse.monitor.dataSources.alertsIndex,
                            alertSize = 2,
                            workflowId
                        )
                    assertAcknowledges(getAlertsResponse.alerts, bucketLevelMonitorResponse.id, 2)
                    assertFindings(
                        bucketLevelMonitorResponse.id,
                        bucketLevelMonitorResponse.monitor.dataSources.findingsIndex,
                        1,
                        4,
                        listOf("3", "4", "5", "6")
                    )
                }
                // Verify third doc level monitor execution, alerts and findings
                docLevelMonitorResponse1.monitor.name -> {
                    assertEquals(1, monitorRunResults.inputResults.results.size)
                    val values = monitorRunResults.triggerResults.values
                    assertEquals(1, values.size)
                    @Suppress("UNCHECKED_CAST")
                    val docLevelTrigger = values.iterator().next() as DocumentLevelTriggerRunResult
                    val triggeredDocIds = docLevelTrigger.triggeredDocs.map { it.split("|")[0] }
                    val expectedTriggeredDocIds = listOf("5", "6")
                    assertEquals(expectedTriggeredDocIds, triggeredDocIds.sorted())

                    val getAlertsResponse =
                        assertAlerts(
                            docLevelMonitorResponse1.id,
                            docLevelMonitorResponse1.monitor.dataSources.alertsIndex,
                            alertSize = 2,
                            workflowId
                        )
                    assertAcknowledges(getAlertsResponse.alerts, docLevelMonitorResponse1.id, 2)
                    assertFindings(
                        docLevelMonitorResponse1.id,
                        docLevelMonitorResponse1.monitor.dataSources.findingsIndex,
                        2,
                        2,
                        listOf("5", "6")
                    )
                }
                // Verify fourth query level monitor execution
                queryMonitorResponse.monitor.name -> {
                    assertEquals(1, monitorRunResults.inputResults.results.size)
                    val values = monitorRunResults.triggerResults.values
                    assertEquals(1, values.size)
                    @Suppress("UNCHECKED_CAST")
                    val totalHits =
                        (
                            (
                                monitorRunResults.inputResults.results[0]["hits"]
                                    as kotlin.collections.Map<String, Any>
                                )["total"] as kotlin.collections.Map<String, Any>
                            )["value"]
                    assertEquals(2, totalHits)
                    @Suppress("UNCHECKED_CAST")
                    val docIds =
                        (
                            (
                                monitorRunResults.inputResults.results[0]["hits"]
                                    as kotlin.collections.Map<String, Any>
                                )["hits"] as List<kotlin.collections.Map<String, String>>
                            )
                            .map { it["_id"]!! }
                    assertEquals(listOf("5", "6"), docIds.sorted())
                }
            }
        }
    }

    fun `test execute workflow input error`() {
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3", fields = listOf()))
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)

        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )

        val monitorResponse = createMonitor(monitor)!!
        var workflow = randomWorkflow(
            monitorIds = listOf(monitorResponse.id), auditDelegateMonitorAlerts = false
        )
        val workflowResponse = upsertWorkflow(workflow)!!
        val workflowById = searchWorkflow(workflowResponse.id)
        assertNotNull(workflowById)

        deleteIndex(index)

        val response = executeWorkflow(workflowById, workflowById!!.id, false)!!
        val error = response.workflowRunResult.monitorRunResults[0].error
        assertNotNull(error)
        assertTrue(error is AlertingException)
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, (error as AlertingException).status)
        assertTrue(error.message!!.contains("no such index [$index]"))
    }

    fun `test execute workflow wrong workflow id`() {
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3", fields = listOf()))
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)

        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )

        val monitorResponse = createMonitor(monitor)!!

        val workflowRequest = randomWorkflow(
            monitorIds = listOf(monitorResponse.id)
        )
        val workflowResponse = upsertWorkflow(workflowRequest)!!
        val workflowId = workflowResponse.id
        val getWorkflowResponse = getWorkflowById(id = workflowResponse.id)

        assertNotNull(getWorkflowResponse)
        assertEquals(workflowId, getWorkflowResponse.id)

        var exception: java.lang.Exception? = null
        val badWorkflowId = getWorkflowResponse.id + "bad"
        try {
            executeWorkflow(id = badWorkflowId)
        } catch (ex: java.lang.Exception) {
            exception = ex
        }
        assertTrue(exception is ExecutionException)
        assertTrue(exception!!.cause is AlertingException)
        assertEquals(RestStatus.NOT_FOUND, (exception.cause as AlertingException).status)
        assertEquals("Can't find workflow with id: $badWorkflowId", exception.cause!!.message)
    }

    private fun assertFindings(
        monitorId: String,
        customFindingsIndex: String,
        findingSize: Int,
        matchedQueryNumber: Int,
        relatedDocIds: List<String>,
    ) {
        val findings = searchFindings(monitorId, customFindingsIndex)
        assertEquals("Findings saved for test monitor", findingSize, findings.size)

        val findingDocIds = findings.flatMap { it.relatedDocIds }

        assertEquals("Didn't match $matchedQueryNumber query", matchedQueryNumber, findingDocIds.size)
        assertTrue("Findings saved for test monitor", relatedDocIds.containsAll(findingDocIds))
    }

    private fun getAuditStateAlerts(
        alertsIndex: String? = AlertIndices.ALERT_INDEX,
        monitorId: String,
        executionId: String? = null,
    ): List<Alert> {
        val searchRequest = SearchRequest(alertsIndex)
        val boolQueryBuilder = QueryBuilders.boolQuery()
        boolQueryBuilder.must(TermQueryBuilder("monitor_id", monitorId))
        if (executionId.isNullOrEmpty() == false)
            boolQueryBuilder.must(TermQueryBuilder("execution_id", executionId))
        searchRequest.source().query(boolQueryBuilder)
        val searchResponse = client().search(searchRequest).get()
        return searchResponse.hits.map { hit ->
            val xcp = XContentHelper.createParser(
                xContentRegistry(), LoggingDeprecationHandler.INSTANCE,
                hit.sourceRef, XContentType.JSON
            )
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
            val alert = Alert.parse(xcp, hit.id, hit.version)
            alert
        }
    }

    private fun assertAlerts(
        monitorId: String,
        alertsIndex: String? = AlertIndices.ALERT_INDEX,
        executionId: String? = null,
        alertSize: Int,
        workflowId: String,
    ): GetAlertsResponse {
        val alerts = searchAlerts(monitorId, alertsIndex!!, executionId = executionId)
        assertEquals("Alert saved for test monitor", alertSize, alerts.size)
        val table = Table("asc", "id", null, alertSize, 0, "")
        var getAlertsResponse = client()
            .execute(
                AlertingActions.GET_ALERTS_ACTION_TYPE,
                GetAlertsRequest(table, "ALL", "ALL", null, alertsIndex)
            )
            .get()
        assertTrue(getAlertsResponse != null)
        assertTrue(getAlertsResponse.alerts.size == alertSize)
        getAlertsResponse = client()
            .execute(
                AlertingActions.GET_ALERTS_ACTION_TYPE,
                GetAlertsRequest(table, "ALL", "ALL", monitorId, null, workflowIds = listOf(workflowId))
            )
            .get()
        assertTrue(getAlertsResponse != null)
        assertTrue(getAlertsResponse.alerts.size == alertSize)

        return getAlertsResponse
    }

    private fun assertAcknowledges(
        alerts: List<Alert>,
        monitorId: String,
        alertSize: Int,
    ) {
        val alertIds = alerts.map { it.id }
        val acknowledgeAlertResponse = client().execute(
            AlertingActions.ACKNOWLEDGE_ALERTS_ACTION_TYPE,
            AcknowledgeAlertRequest(monitorId, alertIds, WriteRequest.RefreshPolicy.IMMEDIATE)
        ).get()

        assertEquals(alertSize, acknowledgeAlertResponse.acknowledged.size)
    }

    private fun verifyAcknowledgeChainedAlerts(
        alerts: List<Alert>,
        workflowId: String,
        alertSize: Int,
    ) {
        val alertIds = alerts.map { it.id }.toMutableList()
        val acknowledgeAlertResponse = ackChainedAlerts(alertIds, workflowId)
        assertTrue(acknowledgeAlertResponse.acknowledged.stream().map { it.id }.collect(Collectors.toList()).containsAll(alertIds))
        assertEquals(alertSize, acknowledgeAlertResponse.acknowledged.size)
        alertIds.add("dummy")
        val redundantAck = ackChainedAlerts(alertIds, workflowId)
        Assert.assertTrue(redundantAck.acknowledged.isEmpty())
        Assert.assertTrue(redundantAck.missing.contains("dummy"))
        alertIds.remove("dummy")
        Assert.assertTrue(redundantAck.failed.map { it.id }.toList().containsAll(alertIds))
    }

    private fun ackChainedAlerts(alertIds: List<String>, workflowId: String): AcknowledgeAlertResponse {

        return client().execute(
            AlertingActions.ACKNOWLEDGE_CHAINED_ALERTS_ACTION_TYPE,
            AcknowledgeChainedAlertRequest(workflowId, alertIds)
        ).get()
    }

    private fun assertAuditStateAlerts(
        monitorId: String,
        alerts: List<Alert>,
    ) {
        alerts.forEach { Assert.assertEquals(it.state, Alert.State.AUDIT) }
        val alertIds = alerts.stream().map { it.id }.collect(Collectors.toList())
        val ack = client().execute(
            AlertingActions.ACKNOWLEDGE_ALERTS_ACTION_TYPE,
            AcknowledgeAlertRequest(monitorId, alertIds, WriteRequest.RefreshPolicy.IMMEDIATE)
        ).get()
        Assert.assertTrue(ack.acknowledged.isEmpty())
        Assert.assertTrue(ack.missing.containsAll(alertIds))
        Assert.assertTrue(ack.failed.isEmpty())
    }

    fun `test execute workflow with bucket-level and doc-level chained monitors`() {
        createTestIndex(TEST_HR_INDEX)

        val compositeSources = listOf(
            TermsValuesSourceBuilder("test_field_1").field("test_field_1")
        )
        val compositeAgg = CompositeAggregationBuilder("composite_agg", compositeSources)
        val input = SearchInput(
            indices = listOf(TEST_HR_INDEX),
            query = SearchSourceBuilder().size(0).query(QueryBuilders.matchAllQuery()).aggregation(compositeAgg)
        )
        val triggerScript = """
            params.docCount > 0
        """.trimIndent()

        var trigger = randomBucketLevelTrigger()
        trigger = trigger.copy(
            bucketSelector = BucketSelectorExtAggregationBuilder(
                name = trigger.id,
                bucketsPathsMap = mapOf("docCount" to "_count"),
                script = Script(triggerScript),
                parentBucketPath = "composite_agg",
                filter = null
            ),
            actions = listOf()
        )
        val bucketMonitor = createMonitor(
            randomBucketLevelMonitor(
                inputs = listOf(input),
                enabled = false,
                triggers = listOf(trigger)
            )
        )
        assertNotNull("The bucket monitor was not created", bucketMonitor)

        val docQuery1 = DocLevelQuery(query = "test_field_1:\"a\"", name = "3", fields = listOf())
        var monitor1 = randomDocumentLevelMonitor(
            inputs = listOf(DocLevelMonitorInput("description", listOf(TEST_HR_INDEX), listOf(docQuery1))),
            triggers = listOf(randomDocumentLevelTrigger(condition = ALWAYS_RUN))
        )
        val docMonitor = createMonitor(monitor1)!!
        assertNotNull("The doc level monitor was not created", docMonitor)

        val workflow = randomWorkflow(monitorIds = listOf(bucketMonitor!!.id, docMonitor.id))
        val workflowResponse = upsertWorkflow(workflow)
        assertNotNull("The workflow was not created", workflowResponse)

        // Add a doc that is accessible to the user
        indexDoc(
            TEST_HR_INDEX,
            "1",
            """
            {
              "test_field_1": "a",
              "accessible": true
            }
            """.trimIndent()
        )

        // Add a second doc that is not accessible to the user
        indexDoc(
            TEST_HR_INDEX,
            "2",
            """
            {
              "test_field_1": "b",
              "accessible": false
            }
            """.trimIndent()
        )

        indexDoc(
            TEST_HR_INDEX,
            "3",
            """
            {
              "test_field_1": "c",
              "accessible": true
            }
            """.trimIndent()
        )

        val executeResult = executeWorkflow(id = workflowResponse!!.id)
        assertNotNull(executeResult)
        assertEquals(2, executeResult!!.workflowRunResult.monitorRunResults.size)
    }

    fun `test chained alerts for AND OR and NOT conditions with custom alerts indices`() {
        val docQuery1 = DocLevelQuery(query = "test_field_1:\"us-west-2\"", name = "3", fields = listOf())
        val docLevelInput1 = DocLevelMonitorInput("description", listOf(index), listOf(docQuery1))
        val trigger1 = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val customFindingsIndex1 = "custom_findings_index"
        val customFindingsIndexPattern1 = "custom_findings_index-1"
        val customAlertsIndex = "custom_alerts_index"
        val customAlertsHistoryIndex = "custom_alerts_history_index"
        val customAlertsHistoryIndexPattern = "<custom_alerts_history_index-{now/d}-1>"
        var monitor1 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput1),
            triggers = listOf(trigger1),
            dataSources = DataSources(
                findingsIndex = customFindingsIndex1,
                findingsIndexPattern = customFindingsIndexPattern1,
                alertsIndex = customAlertsIndex,
                alertsHistoryIndex = customAlertsHistoryIndex,
                alertsHistoryIndexPattern = customAlertsHistoryIndexPattern
            )
        )
        val monitorResponse = createMonitor(monitor1)!!

        val docQuery2 = DocLevelQuery(query = "source.ip.v6.v2:16645", name = "4", fields = listOf())
        val docLevelInput2 = DocLevelMonitorInput("description", listOf(index), listOf(docQuery2))
        val trigger2 = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val customFindingsIndex2 = "custom_findings_index_2"
        val customFindingsIndexPattern2 = "custom_findings_index-2"
        var monitor2 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput2),
            triggers = listOf(trigger2),
            dataSources = DataSources(
                findingsIndex = customFindingsIndex2,
                findingsIndexPattern = customFindingsIndexPattern2,
                alertsIndex = customAlertsIndex,
                alertsHistoryIndex = customAlertsHistoryIndex,
                alertsHistoryIndexPattern = customAlertsHistoryIndexPattern
            )
        )

        val monitorResponse2 = createMonitor(monitor2)!!
        val andTrigger = randomChainedAlertTrigger(
            name = "1And2",
            condition = Script("monitor[id=${monitorResponse.id}] && monitor[id=${monitorResponse2.id}]")
        )
        val notTrigger = randomChainedAlertTrigger(
            name = "Not1OrNot2",
            condition = Script("!monitor[id=${monitorResponse.id}] || !monitor[id=${monitorResponse2.id}]")
        )
        var workflow = randomWorkflow(
            monitorIds = listOf(monitorResponse.id, monitorResponse2.id),
            triggers = listOf(andTrigger, notTrigger)
        )
        val workflowResponse = upsertWorkflow(workflow)!!
        val workflowById = searchWorkflow(workflowResponse.id)
        assertNotNull(workflowById)
        val workflowId = workflowResponse.id

        var executeWorkflowResponse = executeWorkflow(workflowById, workflowId, false)!!
        var triggerResults = executeWorkflowResponse.workflowRunResult.triggerResults
        Assert.assertEquals(triggerResults.size, 2)
        Assert.assertTrue(triggerResults.containsKey(andTrigger.id))
        Assert.assertTrue(triggerResults.containsKey(notTrigger.id))
        var andTriggerResult = triggerResults[andTrigger.id]
        var notTriggerResult = triggerResults[notTrigger.id]
        Assert.assertTrue(notTriggerResult!!.triggered)
        Assert.assertFalse(andTriggerResult!!.triggered)
        var res =
            getWorkflowAlerts(workflowId = workflowId, alertIndex = customAlertsIndex, associatedAlertsIndex = customAlertsHistoryIndex)
        var chainedAlerts = res.alerts
        Assert.assertTrue(chainedAlerts.size == 1)
        Assert.assertTrue(res.associatedAlerts.isEmpty())
        verifyAcknowledgeChainedAlerts(chainedAlerts, workflowId, 1)
        Assert.assertTrue(chainedAlerts[0].executionId == executeWorkflowResponse.workflowRunResult.executionId)
        Assert.assertTrue(chainedAlerts[0].monitorId == "")
        Assert.assertTrue(chainedAlerts[0].triggerId == notTrigger.id)
        var testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(ChronoUnit.MILLIS))
        // Matches monitor1
        val testDoc1 = """{
            "message" : "This is an error from IAD region",
            "source.ip.v6.v2" : 16644, 
            "test_strict_date_time" : "$testTime",
            "test_field_1" : "us-west-2"
        }"""
        indexDoc(index, "1", testDoc1)

        testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(ChronoUnit.MILLIS))
        // Matches monitor1 and monitor2
        val testDoc2 = """{
            "message" : "This is an error from IAD region",
            "source.ip.v6.v2" : 16645, 
            "test_strict_date_time" : "$testTime",
            "test_field_1" : "us-west-2"
        }"""
        indexDoc(index, "2", testDoc2)

        testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(ChronoUnit.MILLIS))
        // Doesn't match
        val testDoc3 = """{
            "message" : "This is an error from IAD region",
            "source.ip.v6.v2" : 16645, 
            "test_strict_date_time" : "$testTime",
            "test_field_1" : "us-east-1"
        }"""
        indexDoc(index, "3", testDoc3)
        executeWorkflowResponse = executeWorkflow(workflowById, workflowId, false)!!
        triggerResults = executeWorkflowResponse.workflowRunResult.triggerResults
        Assert.assertEquals(triggerResults.size, 2)
        Assert.assertTrue(triggerResults.containsKey(andTrigger.id))
        Assert.assertTrue(triggerResults.containsKey(notTrigger.id))
        andTriggerResult = triggerResults[andTrigger.id]
        notTriggerResult = triggerResults[notTrigger.id]
        Assert.assertFalse(notTriggerResult!!.triggered)
        Assert.assertTrue(andTriggerResult!!.triggered)
        res = getWorkflowAlerts(workflowId, alertIndex = customAlertsIndex, associatedAlertsIndex = customAlertsHistoryIndex)
        chainedAlerts = res.alerts
        val numChainedAlerts = 1
        Assert.assertTrue(res.associatedAlerts.isNotEmpty())
        Assert.assertTrue(chainedAlerts.size == numChainedAlerts)
        Assert.assertTrue(chainedAlerts[0].executionId == executeWorkflowResponse.workflowRunResult.executionId)
        Assert.assertTrue(chainedAlerts[0].monitorId == "")
        Assert.assertTrue(chainedAlerts[0].triggerId == andTrigger.id)
        val monitorsRunResults = executeWorkflowResponse.workflowRunResult.monitorRunResults
        assertEquals(2, monitorsRunResults.size)

        assertEquals(monitor1.name, monitorsRunResults[0].monitorName)
        assertEquals(1, monitorsRunResults[0].triggerResults.size)

        Assert.assertEquals(monitor2.name, monitorsRunResults[1].monitorName)
        Assert.assertEquals(1, monitorsRunResults[1].triggerResults.size)

        Assert.assertEquals(
            monitor1.dataSources.alertsHistoryIndex,
            CompositeWorkflowRunner.getDelegateMonitorAlertIndex(dataSources = monitor1.dataSources, workflow, true)
        )
        val alerts = getAuditStateAlerts(
            monitorId = monitorResponse.id, executionId = executeWorkflowResponse.workflowRunResult.executionId,
            alertsIndex = monitor1.dataSources.alertsHistoryIndex,
        )
        assertAuditStateAlerts(monitorResponse.id, alerts)
        assertFindings(monitorResponse.id, customFindingsIndex1, 2, 2, listOf("1", "2"))
        val associatedAlertIds = res.associatedAlerts.map { it.id }.toList()
        associatedAlertIds.containsAll(alerts.map { it.id }.toList())
        val alerts1 = getAuditStateAlerts(
            alertsIndex = monitor2.dataSources.alertsHistoryIndex, monitorId = monitorResponse2.id,
            executionId = executeWorkflowResponse.workflowRunResult.executionId,
        )
        assertAuditStateAlerts(monitorResponse2.id, alerts1)
        assertFindings(monitorResponse2.id, customFindingsIndex2, 1, 1, listOf("2"))
        associatedAlertIds.containsAll(alerts1.map { it.id }.toList())
        verifyAcknowledgeChainedAlerts(chainedAlerts, workflowId, numChainedAlerts)
    }

    fun `test chained alerts for AND OR and NOT conditions`() {
        val docQuery1 = DocLevelQuery(query = "test_field_1:\"us-west-2\"", name = "3", fields = listOf())
        val docLevelInput1 = DocLevelMonitorInput("description", listOf(index), listOf(docQuery1))
        val trigger1 = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val customFindingsIndex1 = "custom_findings_index"
        val customFindingsIndexPattern1 = "custom_findings_index-1"
        var monitor1 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput1),
            triggers = listOf(trigger1),
            dataSources = DataSources(
                findingsIndex = customFindingsIndex1,
                findingsIndexPattern = customFindingsIndexPattern1
            )
        )
        val monitorResponse = createMonitor(monitor1)!!

        val docQuery2 = DocLevelQuery(query = "source.ip.v6.v2:16645", name = "4", fields = listOf())
        val docLevelInput2 = DocLevelMonitorInput("description", listOf(index), listOf(docQuery2))
        val trigger2 = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val customFindingsIndex2 = "custom_findings_index_2"
        val customFindingsIndexPattern2 = "custom_findings_index-2"
        var monitor2 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput2),
            triggers = listOf(trigger2),
            dataSources = DataSources(
                findingsIndex = customFindingsIndex2,
                findingsIndexPattern = customFindingsIndexPattern2
            )
        )

        val monitorResponse2 = createMonitor(monitor2)!!
        val andTrigger = randomChainedAlertTrigger(
            name = "1And2",
            condition = Script("monitor[id=${monitorResponse.id}] && monitor[id=${monitorResponse2.id}]")
        )
        val notTrigger = randomChainedAlertTrigger(
            name = "Not1OrNot2",
            condition = Script("!monitor[id=${monitorResponse.id}] || !monitor[id=${monitorResponse2.id}]")
        )
        var workflow = randomWorkflow(
            monitorIds = listOf(monitorResponse.id, monitorResponse2.id),
            triggers = listOf(andTrigger, notTrigger)
        )
        val workflowResponse = upsertWorkflow(workflow)!!
        val workflowById = searchWorkflow(workflowResponse.id)
        assertNotNull(workflowById)
        val workflowId = workflowResponse.id

        var executeWorkflowResponse = executeWorkflow(workflowById, workflowId, false)!!
        var triggerResults = executeWorkflowResponse.workflowRunResult.triggerResults
        Assert.assertEquals(triggerResults.size, 2)
        Assert.assertTrue(triggerResults.containsKey(andTrigger.id))
        Assert.assertTrue(triggerResults.containsKey(notTrigger.id))
        var andTriggerResult = triggerResults[andTrigger.id]
        var notTriggerResult = triggerResults[notTrigger.id]
        Assert.assertTrue(notTriggerResult!!.triggered)
        Assert.assertFalse(andTriggerResult!!.triggered)
        var res = getWorkflowAlerts(
            workflowId,
        )
        var chainedAlerts = res.alerts
        Assert.assertTrue(chainedAlerts.size == 1)

        // verify get alerts api with defaults set in query params returns only chained alerts and not audit alerts
        val table = Table("asc", "id", null, 1, 0, "")
        val getAlertsDefaultParamsResponse = client().execute(
            AlertingActions.GET_ALERTS_ACTION_TYPE,
            GetAlertsRequest(
                table = table,
                severityLevel = "ALL",
                alertState = "ALL",
                monitorId = null,
                alertIndex = null,
                monitorIds = null,
                workflowIds = null,
                alertIds = null
            )
        ).get()
        Assert.assertEquals(getAlertsDefaultParamsResponse.alerts.size, 1)
        Assert.assertTrue(res.associatedAlerts.isEmpty())
        verifyAcknowledgeChainedAlerts(chainedAlerts, workflowId, 1)
        Assert.assertTrue(chainedAlerts[0].executionId == executeWorkflowResponse.workflowRunResult.executionId)
        Assert.assertTrue(chainedAlerts[0].monitorId == "")
        Assert.assertTrue(chainedAlerts[0].triggerId == notTrigger.id)
        var testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(ChronoUnit.MILLIS))
        // Matches monitor1
        val testDoc1 = """{
            "message" : "This is an error from IAD region",
            "source.ip.v6.v2" : 16644, 
            "test_strict_date_time" : "$testTime",
            "test_field_1" : "us-west-2"
        }"""
        indexDoc(index, "1", testDoc1)

        testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(ChronoUnit.MILLIS))
        // Matches monitor1 and monitor2
        val testDoc2 = """{
            "message" : "This is an error from IAD region",
            "source.ip.v6.v2" : 16645, 
            "test_strict_date_time" : "$testTime",
            "test_field_1" : "us-west-2"
        }"""
        indexDoc(index, "2", testDoc2)

        testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(ChronoUnit.MILLIS))
        // Doesn't match
        val testDoc3 = """{
            "message" : "This is an error from IAD region",
            "source.ip.v6.v2" : 16645, 
            "test_strict_date_time" : "$testTime",
            "test_field_1" : "us-east-1"
        }"""
        indexDoc(index, "3", testDoc3)
        executeWorkflowResponse = executeWorkflow(workflowById, workflowId, false)!!
        triggerResults = executeWorkflowResponse.workflowRunResult.triggerResults
        Assert.assertEquals(triggerResults.size, 2)
        Assert.assertTrue(triggerResults.containsKey(andTrigger.id))
        Assert.assertTrue(triggerResults.containsKey(notTrigger.id))
        andTriggerResult = triggerResults[andTrigger.id]
        notTriggerResult = triggerResults[notTrigger.id]
        Assert.assertFalse(notTriggerResult!!.triggered)
        Assert.assertTrue(andTriggerResult!!.triggered)
        val getAuditAlertsForMonitor1 = client().execute(
            AlertingActions.GET_ALERTS_ACTION_TYPE,
            GetAlertsRequest(
                table = table,
                severityLevel = "ALL",
                alertState = "AUDIT",
                monitorId = monitorResponse.id,
                alertIndex = null,
                monitorIds = null,
                workflowIds = listOf(workflowId),
                alertIds = null
            )
        ).get()
        Assert.assertEquals(getAuditAlertsForMonitor1.alerts.size, 1)
        res = getWorkflowAlerts(workflowId)
        chainedAlerts = res.alerts
        Assert.assertTrue(chainedAlerts.size == 1)
        Assert.assertTrue(res.associatedAlerts.isNotEmpty())
        Assert.assertTrue(chainedAlerts[0].executionId == executeWorkflowResponse.workflowRunResult.executionId)
        Assert.assertTrue(chainedAlerts[0].monitorId == "")
        Assert.assertTrue(chainedAlerts[0].triggerId == andTrigger.id)
        val monitorsRunResults = executeWorkflowResponse.workflowRunResult.monitorRunResults
        assertEquals(2, monitorsRunResults.size)

        assertEquals(monitor1.name, monitorsRunResults[0].monitorName)
        assertEquals(1, monitorsRunResults[0].triggerResults.size)

        Assert.assertEquals(monitor2.name, monitorsRunResults[1].monitorName)
        Assert.assertEquals(1, monitorsRunResults[1].triggerResults.size)

        Assert.assertEquals(
            monitor1.dataSources.alertsHistoryIndex,
            CompositeWorkflowRunner.getDelegateMonitorAlertIndex(dataSources = monitor1.dataSources, workflow, true)
        )
        val alerts = getAuditStateAlerts(
            alertsIndex = monitor1.dataSources.alertsHistoryIndex, monitorId = monitorResponse.id,
            executionId = executeWorkflowResponse.workflowRunResult.executionId
        )
        val associatedAlertIds = res.associatedAlerts.map { it.id }.toList()
        associatedAlertIds.containsAll(alerts.map { it.id }.toList())
        assertAuditStateAlerts(monitorResponse.id, alerts)
        assertFindings(monitorResponse.id, customFindingsIndex1, 2, 2, listOf("1", "2"))

        val alerts1 = getAuditStateAlerts(
            alertsIndex = monitor2.dataSources.alertsHistoryIndex, monitorId = monitorResponse2.id,
            executionId = executeWorkflowResponse.workflowRunResult.executionId
        )
        associatedAlertIds.containsAll(alerts1.map { it.id }.toList())
        assertAuditStateAlerts(monitorResponse2.id, alerts1)
        assertFindings(monitorResponse2.id, customFindingsIndex2, 1, 1, listOf("2"))
        verifyAcknowledgeChainedAlerts(chainedAlerts, workflowId, 1)
        // test redundant executions of workflow dont query old data again to verify metadata updation works fine
        val redundantExec = executeWorkflow(workflow)
        Assert.assertFalse(redundantExec?.workflowRunResult!!.triggerResults[andTrigger.id]!!.triggered)
        Assert.assertTrue(redundantExec.workflowRunResult.triggerResults[notTrigger.id]!!.triggered)
    }

    private fun getDelegateMonitorMetadataId(
        workflowMetadata: WorkflowMetadata?,
        monitorResponse: IndexMonitorResponse,
    ) = "${workflowMetadata!!.id}-${monitorResponse.id}-metadata"

    fun `test create workflow success`() {
        val docQuery1 = DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3", fields = listOf())
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(docQuery1)
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val customFindingsIndex = "custom_findings_index"
        val customFindingsIndexPattern = "custom_findings_index-1"
        val customQueryIndex = "custom_alerts_index"
        val monitor1 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
            dataSources = DataSources(
                queryIndex = customQueryIndex,
                findingsIndex = customFindingsIndex,
                findingsIndexPattern = customFindingsIndexPattern
            )
        )

        val monitor2 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
            dataSources = DataSources(
                queryIndex = customQueryIndex,
                findingsIndex = customFindingsIndex,
                findingsIndexPattern = customFindingsIndexPattern
            )
        )

        val monitorResponse1 = createMonitor(monitor1)!!
        val monitorResponse2 = createMonitor(monitor2)!!

        val workflow = randomWorkflow(
            monitorIds = listOf(monitorResponse1.id, monitorResponse2.id)
        )

        val workflowResponse = upsertWorkflow(workflow)!!
        assertNotNull("Workflow creation failed", workflowResponse)
        assertNotNull(workflowResponse.workflow)
        assertNotEquals("response is missing Id", Monitor.NO_ID, workflowResponse.id)
        assertTrue("incorrect version", workflowResponse.version > 0)

        val workflowById = searchWorkflow(workflowResponse.id)!!
        assertNotNull(workflowById)

        // Verify workflow
        assertNotEquals("response is missing Id", Monitor.NO_ID, workflowById.id)
        assertTrue("incorrect version", workflowById.version > 0)
        assertEquals("Workflow name not correct", workflow.name, workflowById.name)
        assertEquals("Workflow owner not correct", workflow.owner, workflowById.owner)
        assertEquals("Workflow input not correct", workflow.inputs, workflowById.inputs)

        // Delegate verification
        @Suppress("UNCHECKED_CAST")
        val delegates = (workflowById.inputs as List<CompositeInput>)[0].sequence.delegates.sortedBy { it.order }
        assertEquals("Delegates size not correct", 2, delegates.size)

        val delegate1 = delegates[0]
        assertNotNull(delegate1)
        assertEquals("Delegate1 order not correct", 1, delegate1.order)
        assertEquals("Delegate1 id not correct", monitorResponse1.id, delegate1.monitorId)

        val delegate2 = delegates[1]
        assertNotNull(delegate2)
        assertEquals("Delegate2 order not correct", 2, delegate2.order)
        assertEquals("Delegate2 id not correct", monitorResponse2.id, delegate2.monitorId)
        assertEquals(
            "Delegate2 Chained finding not correct", monitorResponse1.id, delegate2.chainedMonitorFindings!!.monitorId
        )
    }

    fun `test update workflow add monitor success`() {
        val docQuery1 = DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3", fields = listOf())
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(docQuery1)
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val customFindingsIndex = "custom_findings_index"
        val customFindingsIndexPattern = "custom_findings_index-1"
        val customQueryIndex = "custom_alerts_index"
        val monitor1 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
            dataSources = DataSources(
                queryIndex = customQueryIndex,
                findingsIndex = customFindingsIndex,
                findingsIndexPattern = customFindingsIndexPattern
            )
        )

        val monitor2 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
            dataSources = DataSources(
                queryIndex = customQueryIndex,
                findingsIndex = customFindingsIndex,
                findingsIndexPattern = customFindingsIndexPattern
            )
        )

        val monitorResponse1 = createMonitor(monitor1)!!
        val monitorResponse2 = createMonitor(monitor2)!!

        val workflow = randomWorkflow(
            monitorIds = listOf(monitorResponse1.id, monitorResponse2.id)
        )

        val workflowResponse = upsertWorkflow(workflow)!!
        assertNotNull("Workflow creation failed", workflowResponse)
        assertNotNull(workflowResponse.workflow)
        assertNotEquals("response is missing Id", Monitor.NO_ID, workflowResponse.id)
        assertTrue("incorrect version", workflowResponse.version > 0)

        var workflowById = searchWorkflow(workflowResponse.id)!!
        assertNotNull(workflowById)

        val monitor3 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
            dataSources = DataSources(
                queryIndex = customQueryIndex,
                findingsIndex = customFindingsIndex,
                findingsIndexPattern = customFindingsIndexPattern
            )
        )
        val monitorResponse3 = createMonitor(monitor3)!!

        val updatedWorkflowResponse = upsertWorkflow(
            randomWorkflow(
                monitorIds = listOf(monitorResponse1.id, monitorResponse2.id, monitorResponse3.id)
            ),
            workflowResponse.id,
            RestRequest.Method.PUT
        )!!

        assertNotNull("Workflow creation failed", updatedWorkflowResponse)
        assertNotNull(updatedWorkflowResponse.workflow)
        assertEquals("Workflow id changed", workflowResponse.id, updatedWorkflowResponse.id)
        assertTrue("incorrect version", updatedWorkflowResponse.version > 0)

        workflowById = searchWorkflow(updatedWorkflowResponse.id)!!

        // Verify workflow
        assertNotEquals("response is missing Id", Monitor.NO_ID, workflowById.id)
        assertTrue("incorrect version", workflowById.version > 0)
        assertEquals("Workflow name not correct", updatedWorkflowResponse.workflow.name, workflowById.name)
        assertEquals("Workflow owner not correct", updatedWorkflowResponse.workflow.owner, workflowById.owner)
        assertEquals("Workflow input not correct", updatedWorkflowResponse.workflow.inputs, workflowById.inputs)

        // Delegate verification
        @Suppress("UNCHECKED_CAST")
        val delegates = (workflowById.inputs as List<CompositeInput>)[0].sequence.delegates.sortedBy { it.order }
        assertEquals("Delegates size not correct", 3, delegates.size)

        val delegate1 = delegates[0]
        assertNotNull(delegate1)
        assertEquals("Delegate1 order not correct", 1, delegate1.order)
        assertEquals("Delegate1 id not correct", monitorResponse1.id, delegate1.monitorId)

        val delegate2 = delegates[1]
        assertNotNull(delegate2)
        assertEquals("Delegate2 order not correct", 2, delegate2.order)
        assertEquals("Delegate2 id not correct", monitorResponse2.id, delegate2.monitorId)
        assertEquals(
            "Delegate2 Chained finding not correct", monitorResponse1.id, delegate2.chainedMonitorFindings!!.monitorId
        )

        val delegate3 = delegates[2]
        assertNotNull(delegate3)
        assertEquals("Delegate3 order not correct", 3, delegate3.order)
        assertEquals("Delegate3 id not correct", monitorResponse3.id, delegate3.monitorId)
        assertEquals(
            "Delegate3 Chained finding not correct", monitorResponse2.id, delegate3.chainedMonitorFindings!!.monitorId
        )
    }

    fun `test update workflow change order of delegate monitors`() {
        val docQuery1 = DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3", fields = listOf())
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(docQuery1)
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val customFindingsIndex = "custom_findings_index"
        val customFindingsIndexPattern = "custom_findings_index-1"
        val customQueryIndex = "custom_alerts_index"
        val monitor1 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
            dataSources = DataSources(
                queryIndex = customQueryIndex,
                findingsIndex = customFindingsIndex,
                findingsIndexPattern = customFindingsIndexPattern
            )
        )

        val monitor2 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
            dataSources = DataSources(
                queryIndex = customQueryIndex,
                findingsIndex = customFindingsIndex,
                findingsIndexPattern = customFindingsIndexPattern
            )
        )

        val monitorResponse1 = createMonitor(monitor1)!!
        val monitorResponse2 = createMonitor(monitor2)!!

        val workflow = randomWorkflow(
            monitorIds = listOf(monitorResponse1.id, monitorResponse2.id)
        )

        val workflowResponse = upsertWorkflow(workflow)!!
        assertNotNull("Workflow creation failed", workflowResponse)
        assertNotNull(workflowResponse.workflow)
        assertNotEquals("response is missing Id", Monitor.NO_ID, workflowResponse.id)
        assertTrue("incorrect version", workflowResponse.version > 0)

        var workflowById = searchWorkflow(workflowResponse.id)!!
        assertNotNull(workflowById)

        val updatedWorkflowResponse = upsertWorkflow(
            randomWorkflow(
                monitorIds = listOf(monitorResponse2.id, monitorResponse1.id)
            ),
            workflowResponse.id,
            RestRequest.Method.PUT
        )!!

        assertNotNull("Workflow creation failed", updatedWorkflowResponse)
        assertNotNull(updatedWorkflowResponse.workflow)
        assertEquals("Workflow id changed", workflowResponse.id, updatedWorkflowResponse.id)
        assertTrue("incorrect version", updatedWorkflowResponse.version > 0)

        workflowById = searchWorkflow(updatedWorkflowResponse.id)!!

        // Verify workflow
        assertNotEquals("response is missing Id", Monitor.NO_ID, workflowById.id)
        assertTrue("incorrect version", workflowById.version > 0)
        assertEquals("Workflow name not correct", updatedWorkflowResponse.workflow.name, workflowById.name)
        assertEquals("Workflow owner not correct", updatedWorkflowResponse.workflow.owner, workflowById.owner)
        assertEquals("Workflow input not correct", updatedWorkflowResponse.workflow.inputs, workflowById.inputs)

        // Delegate verification
        @Suppress("UNCHECKED_CAST")
        val delegates = (workflowById.inputs as List<CompositeInput>)[0].sequence.delegates.sortedBy { it.order }
        assertEquals("Delegates size not correct", 2, delegates.size)

        val delegate1 = delegates[0]
        assertNotNull(delegate1)
        assertEquals("Delegate1 order not correct", 1, delegate1.order)
        assertEquals("Delegate1 id not correct", monitorResponse2.id, delegate1.monitorId)

        val delegate2 = delegates[1]
        assertNotNull(delegate2)
        assertEquals("Delegate2 order not correct", 2, delegate2.order)
        assertEquals("Delegate2 id not correct", monitorResponse1.id, delegate2.monitorId)
        assertEquals(
            "Delegate2 Chained finding not correct", monitorResponse2.id, delegate2.chainedMonitorFindings!!.monitorId
        )
    }

    fun `test update workflow remove monitor success`() {
        val docQuery1 = DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3", fields = listOf())
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(docQuery1)
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val customFindingsIndex = "custom_findings_index"
        val customFindingsIndexPattern = "custom_findings_index-1"
        val customQueryIndex = "custom_alerts_index"
        val monitor1 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
            dataSources = DataSources(
                queryIndex = customQueryIndex,
                findingsIndex = customFindingsIndex,
                findingsIndexPattern = customFindingsIndexPattern
            )
        )

        val monitor2 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
            dataSources = DataSources(
                queryIndex = customQueryIndex,
                findingsIndex = customFindingsIndex,
                findingsIndexPattern = customFindingsIndexPattern
            )
        )

        val monitorResponse1 = createMonitor(monitor1)!!
        val monitorResponse2 = createMonitor(monitor2)!!

        val workflow = randomWorkflow(
            monitorIds = listOf(monitorResponse1.id, monitorResponse2.id)
        )

        val workflowResponse = upsertWorkflow(workflow)!!
        assertNotNull("Workflow creation failed", workflowResponse)
        assertNotNull(workflowResponse.workflow)
        assertNotEquals("response is missing Id", Monitor.NO_ID, workflowResponse.id)
        assertTrue("incorrect version", workflowResponse.version > 0)

        var workflowById = searchWorkflow(workflowResponse.id)!!
        assertNotNull(workflowById)

        val updatedWorkflowResponse = upsertWorkflow(
            randomWorkflow(
                monitorIds = listOf(monitorResponse1.id)
            ),
            workflowResponse.id,
            RestRequest.Method.PUT
        )!!

        assertNotNull("Workflow creation failed", updatedWorkflowResponse)
        assertNotNull(updatedWorkflowResponse.workflow)
        assertEquals("Workflow id changed", workflowResponse.id, updatedWorkflowResponse.id)
        assertTrue("incorrect version", updatedWorkflowResponse.version > 0)

        workflowById = searchWorkflow(updatedWorkflowResponse.id)!!

        // Verify workflow
        assertNotEquals("response is missing Id", Monitor.NO_ID, workflowById.id)
        assertTrue("incorrect version", workflowById.version > 0)
        assertEquals("Workflow name not correct", updatedWorkflowResponse.workflow.name, workflowById.name)
        assertEquals("Workflow owner not correct", updatedWorkflowResponse.workflow.owner, workflowById.owner)
        assertEquals("Workflow input not correct", updatedWorkflowResponse.workflow.inputs, workflowById.inputs)

        // Delegate verification
        @Suppress("UNCHECKED_CAST")
        val delegates = (workflowById.inputs as List<CompositeInput>)[0].sequence.delegates.sortedBy { it.order }
        assertEquals("Delegates size not correct", 1, delegates.size)

        val delegate1 = delegates[0]
        assertNotNull(delegate1)
        assertEquals("Delegate1 order not correct", 1, delegate1.order)
        assertEquals("Delegate1 id not correct", monitorResponse1.id, delegate1.monitorId)
    }

    fun `test update workflow doesn't exist failure`() {
        val docQuery1 = DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3", fields = listOf())
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(docQuery1)
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val customFindingsIndex = "custom_findings_index"
        val customFindingsIndexPattern = "custom_findings_index-1"
        val customQueryIndex = "custom_alerts_index"
        val monitor1 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
            dataSources = DataSources(
                queryIndex = customQueryIndex,
                findingsIndex = customFindingsIndex,
                findingsIndexPattern = customFindingsIndexPattern
            )
        )

        val monitorResponse1 = createMonitor(monitor1)!!

        val workflow = randomWorkflow(
            monitorIds = listOf(monitorResponse1.id)
        )
        val workflowResponse = upsertWorkflow(workflow)!!
        assertNotNull("Workflow creation failed", workflowResponse)

        try {
            upsertWorkflow(workflow, "testId", RestRequest.Method.PUT)
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning GetWorkflow Action error ",
                    it.contains("Workflow with testId is not found")
                )
            }
        }
    }

    fun `test get workflow`() {
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3", fields = listOf()))
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
        )

        val monitorResponse = createMonitor(monitor)!!

        val workflowRequest = randomWorkflow(
            monitorIds = listOf(monitorResponse.id)
        )

        val workflowResponse = upsertWorkflow(workflowRequest)!!
        assertNotNull("Workflow creation failed", workflowResponse)
        assertNotNull(workflowResponse.workflow)
        assertNotEquals("response is missing Id", Monitor.NO_ID, workflowResponse.id)
        assertTrue("incorrect version", workflowResponse.version > 0)

        val getWorkflowResponse = getWorkflowById(id = workflowResponse.id)
        assertNotNull(getWorkflowResponse)

        val workflowById = getWorkflowResponse.workflow!!
        // Verify workflow
        assertNotEquals("response is missing Id", Monitor.NO_ID, getWorkflowResponse.id)
        assertTrue("incorrect version", getWorkflowResponse.version > 0)
        assertEquals("Workflow name not correct", workflowRequest.name, workflowById.name)
        assertEquals("Workflow owner not correct", workflowRequest.owner, workflowById.owner)
        assertEquals("Workflow input not correct", workflowRequest.inputs, workflowById.inputs)

        // Delegate verification
        @Suppress("UNCHECKED_CAST")
        val delegates = (workflowById.inputs as List<CompositeInput>)[0].sequence.delegates.sortedBy { it.order }
        assertEquals("Delegates size not correct", 1, delegates.size)

        val delegate = delegates[0]
        assertNotNull(delegate)
        assertEquals("Delegate order not correct", 1, delegate.order)
        assertEquals("Delegate id not correct", monitorResponse.id, delegate.monitorId)
    }

    fun `test get workflow for invalid id monitor index doesn't exist`() {
        // Get workflow for non existing workflow id
        try {
            getWorkflowById(id = "-1")
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning GetWorkflow Action error ",
                    it.contains("Workflow not found")
                )
            }
        }
    }

    fun `test get workflow for invalid id monitor index exists`() {
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3", fields = listOf()))
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
        )
        createMonitor(monitor)
        // Get workflow for non existing workflow id
        try {
            getWorkflowById(id = "-1")
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning GetWorkflow Action error ",
                    it.contains("Workflow not found")
                )
            }
        }
    }

    fun `test delete workflow keeping delegate monitor`() {
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3", fields = listOf()))
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)

        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )

        val monitorResponse = createMonitor(monitor)!!

        val workflowRequest = randomWorkflow(
            monitorIds = listOf(monitorResponse.id)
        )
        val workflowResponse = upsertWorkflow(workflowRequest)!!
        val workflowId = workflowResponse.id
        val getWorkflowResponse = getWorkflowById(id = workflowResponse.id)

        assertNotNull(getWorkflowResponse)
        assertEquals(workflowId, getWorkflowResponse.id)

        deleteWorkflow(workflowId, false)
        // Verify that the workflow is deleted
        try {
            getWorkflowById(workflowId)
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning GetWorkflow Action error ",
                    it.contains("Workflow not found.")
                )
            }
        }
        // Verify that the monitor is not deleted
        val existingDelegate = getMonitorResponse(monitorResponse.id)
        assertNotNull(existingDelegate)
    }

    fun `test delete workflow delegate monitor deleted`() {
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3", fields = listOf()))
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)

        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )

        val monitorResponse = createMonitor(monitor)!!

        val workflowRequest = randomWorkflow(
            monitorIds = listOf(monitorResponse.id)
        )
        val workflowResponse = upsertWorkflow(workflowRequest)!!
        val workflowId = workflowResponse.id
        val getWorkflowResponse = getWorkflowById(id = workflowResponse.id)

        assertNotNull(getWorkflowResponse)
        assertEquals(workflowId, getWorkflowResponse.id)

        deleteWorkflow(workflowId, true)
        // Verify that the workflow is deleted
        try {
            getWorkflowById(workflowId)
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning GetWorkflow Action error ",
                    it.contains("Workflow not found.")
                )
            }
        }
        // Verify that the monitor is deleted
        try {
            getMonitorResponse(monitorResponse.id)
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning GetMonitor Action error ",
                    it.contains("Monitor not found")
                )
            }
        }
    }

    fun `test delete executed workflow with metadata deleted`() {
        val docQuery1 = DocLevelQuery(query = "test_field_1:\"us-west-2\"", name = "3", fields = listOf())
        val docLevelInput1 = DocLevelMonitorInput("description", listOf(index), listOf(docQuery1))
        val trigger1 = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        var monitor1 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput1),
            triggers = listOf(trigger1)
        )
        val monitorResponse = createMonitor(monitor1)!!

        val docQuery2 = DocLevelQuery(query = "source.ip.v6.v2:16645", name = "4", fields = listOf())
        val docLevelInput2 = DocLevelMonitorInput("description", listOf(index), listOf(docQuery2))
        val trigger2 = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        var monitor2 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput2),
            triggers = listOf(trigger2),
        )

        val monitorResponse2 = createMonitor(monitor2)!!

        var workflow = randomWorkflow(
            monitorIds = listOf(monitorResponse.id, monitorResponse2.id)
        )
        val workflowResponse = upsertWorkflow(workflow)!!
        val workflowById = searchWorkflow(workflowResponse.id)
        assertNotNull(workflowById)

        var testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(ChronoUnit.MILLIS))
        // Matches monitor1
        val testDoc1 = """{
            "message" : "This is an error from IAD region",
            "source.ip.v6.v2" : 16644, 
            "test_strict_date_time" : "$testTime",
            "test_field_1" : "us-west-2"
        }"""
        indexDoc(index, "1", testDoc1)

        val workflowId = workflowResponse.id
        val executeWorkflowResponse = executeWorkflow(workflowById, workflowId, false)!!
        val monitorsRunResults = executeWorkflowResponse.workflowRunResult.monitorRunResults
        assertEquals(2, monitorsRunResults.size)

        val workflowMetadata = searchWorkflowMetadata(workflowId)
        assertNotNull(workflowMetadata)

        val monitorMetadataId1 = getDelegateMonitorMetadataId(workflowMetadata, monitorResponse)
        val monitorMetadata1 = searchMonitorMetadata(monitorMetadataId1)
        assertNotNull(monitorMetadata1)

        val monitorMetadataId2 = getDelegateMonitorMetadataId(workflowMetadata, monitorResponse2)
        val monitorMetadata2 = searchMonitorMetadata(monitorMetadataId2)
        assertNotNull(monitorMetadata2)

        assertFalse(monitorMetadata1!!.id == monitorMetadata2!!.id)

        deleteWorkflow(workflowId, true)
        // Verify that the workflow is deleted
        try {
            getWorkflowById(workflowId)
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning GetWorkflow Action error ",
                    it.contains("Workflow not found.")
                )
            }
        }
        // Verify that the workflow metadata is deleted
        try {
            searchWorkflowMetadata(workflowId)
            fail("expected searchWorkflowMetadata method to throw exception")
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning GetMonitor Action error ",
                    it.contains("List is empty")
                )
            }
        }
        // Verify that the monitors metadata are deleted
        try {
            searchMonitorMetadata(monitorMetadataId1)
            fail("expected searchMonitorMetadata method to throw exception")
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning GetMonitor Action error ",
                    it.contains("List is empty")
                )
            }
        }

        try {
            searchMonitorMetadata(monitorMetadataId2)
            fail("expected searchMonitorMetadata method to throw exception")
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning GetMonitor Action error ",
                    it.contains("List is empty")
                )
            }
        }
    }

    fun `test delete workflow delegate monitor part of another workflow not deleted`() {
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3", fields = listOf()))
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)

        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )

        val monitorResponse = createMonitor(monitor)!!

        val workflowRequest = randomWorkflow(
            monitorIds = listOf(monitorResponse.id)
        )
        val workflowResponse = upsertWorkflow(workflowRequest)!!
        val workflowId = workflowResponse.id
        val getWorkflowResponse = getWorkflowById(id = workflowResponse.id)

        assertNotNull(getWorkflowResponse)
        assertEquals(workflowId, getWorkflowResponse.id)

        val workflowRequest2 = randomWorkflow(
            monitorIds = listOf(monitorResponse.id)
        )
        val workflowResponse2 = upsertWorkflow(workflowRequest2)!!
        val workflowId2 = workflowResponse2.id
        val getWorkflowResponse2 = getWorkflowById(id = workflowResponse2.id)

        assertNotNull(getWorkflowResponse2)
        assertEquals(workflowId2, getWorkflowResponse2.id)

        try {
            deleteWorkflow(workflowId, true)
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning GetWorkflow Action error ",
                    it.contains("[Not allowed to delete ${monitorResponse.id} monitors")
                )
            }
        }
        val existingMonitor = getMonitorResponse(monitorResponse.id)
        assertNotNull(existingMonitor)
    }

    fun `test trying to delete monitor that is part of workflow sequence`() {
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3", fields = listOf()))
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)

        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )

        val monitorResponse = createMonitor(monitor)!!

        val workflowRequest = randomWorkflow(
            monitorIds = listOf(monitorResponse.id)
        )

        val workflowResponse = upsertWorkflow(workflowRequest)!!
        val workflowId = workflowResponse.id
        val getWorkflowResponse = getWorkflowById(id = workflowResponse.id)

        assertNotNull(getWorkflowResponse)
        assertEquals(workflowId, getWorkflowResponse.id)

        // Verify that the monitor can't be deleted because it's included in the workflow
        try {
            deleteMonitor(monitorResponse.id)
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning DeleteMonitor Action error ",
                    it.contains("Monitor can't be deleted because it is a part of workflow(s)")
                )
            }
        }
    }

    fun `test delete workflow for invalid id monitor index doesn't exists`() {
        // Try deleting non-existing workflow
        try {
            deleteWorkflow("-1")
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning DeleteWorkflow Action error ",
                    it.contains("Workflow not found.")
                )
            }
        }
    }

    fun `test delete workflow for invalid id monitor index exists`() {
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3", fields = listOf()))
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)

        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
        )
        createMonitor(monitor)
        // Try deleting non-existing workflow
        try {
            deleteWorkflow("-1")
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning DeleteWorkflow Action error ",
                    it.contains("Workflow not found.")
                )
            }
        }
    }

    fun `test create workflow without delegate failure`() {
        val workflow = randomWorkflow(
            monitorIds = Collections.emptyList()
        )
        try {
            upsertWorkflow(workflow)
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning IndexWorkflow Action error ",
                    it.contains("Delegates list can not be empty.")
                )
            }
        }
    }

    fun `test create workflow with 26 delegates failure`() {
        val monitorsIds = mutableListOf<String>()
        for (i in 0..25) {
            monitorsIds.add(UUID.randomUUID().toString())
        }
        val workflow = randomWorkflow(
            monitorIds = monitorsIds
        )
        try {
            upsertWorkflow(workflow)
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning IndexWorkflow Action error ",
                    it.contains("Delegates list can not be larger then 25.")
                )
            }
        }
    }

    fun `test update workflow without delegate failure`() {
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3", fields = listOf()))
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val monitor1 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )

        val monitor2 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
        )

        val monitorResponse1 = createMonitor(monitor1)!!
        val monitorResponse2 = createMonitor(monitor2)!!

        var workflow = randomWorkflow(
            monitorIds = listOf(monitorResponse1.id, monitorResponse2.id)
        )

        val workflowResponse = upsertWorkflow(workflow)!!
        assertNotNull("Workflow creation failed", workflowResponse)

        workflow = randomWorkflow(
            id = workflowResponse.id,
            monitorIds = Collections.emptyList()
        )
        try {
            upsertWorkflow(workflow)
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning IndexWorkflow Action error ",
                    it.contains("Delegates list can not be empty.")
                )
            }
        }
    }

    fun `test create workflow duplicate delegate failure`() {
        val workflow = randomWorkflow(
            monitorIds = listOf("1", "1", "2")
        )
        try {
            upsertWorkflow(workflow)
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning IndexWorkflow Action error ",
                    it.contains("Duplicate delegates not allowed")
                )
            }
        }
    }

    fun `test update workflow duplicate delegate failure`() {
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3", fields = listOf()))
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )

        val monitorResponse = createMonitor(monitor)!!

        var workflow = randomWorkflow(
            monitorIds = listOf(monitorResponse.id)
        )

        val workflowResponse = upsertWorkflow(workflow)!!
        assertNotNull("Workflow creation failed", workflowResponse)

        workflow = randomWorkflow(
            id = workflowResponse.id,
            monitorIds = listOf("1", "1", "2")
        )
        try {
            upsertWorkflow(workflow)
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning IndexWorkflow Action error ",
                    it.contains("Duplicate delegates not allowed")
                )
            }
        }
    }

    fun `test create workflow delegate monitor doesn't exist failure`() {
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3", fields = listOf()))
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)

        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )
        val monitorResponse = createMonitor(monitor)!!

        val workflow = randomWorkflow(
            monitorIds = listOf("-1", monitorResponse.id)
        )
        try {
            upsertWorkflow(workflow)
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning IndexWorkflow Action error ",
                    it.contains("are not valid monitor ids")
                )
            }
        }
    }

    fun `test update workflow delegate monitor doesn't exist failure`() {
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3", fields = listOf()))
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)

        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )
        val monitorResponse = createMonitor(monitor)!!

        var workflow = randomWorkflow(
            monitorIds = listOf(monitorResponse.id)
        )
        val workflowResponse = upsertWorkflow(workflow)!!
        assertNotNull("Workflow creation failed", workflowResponse)

        workflow = randomWorkflow(
            id = workflowResponse.id,
            monitorIds = listOf("-1", monitorResponse.id)
        )

        try {
            upsertWorkflow(workflow)
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning IndexWorkflow Action error ",
                    it.contains("are not valid monitor ids")
                )
            }
        }
    }

    fun `test create workflow sequence order not correct failure`() {
        val delegates = listOf(
            Delegate(1, "monitor-1"),
            Delegate(1, "monitor-2"),
            Delegate(2, "monitor-3")
        )
        val workflow = randomWorkflowWithDelegates(
            delegates = delegates
        )
        try {
            upsertWorkflow(workflow)
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning IndexWorkflow Action error ",
                    it.contains("Sequence ordering of delegate monitor shouldn't contain duplicate order values")
                )
            }
        }
    }

    fun `test update workflow sequence order not correct failure`() {
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3", fields = listOf()))
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)

        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )
        val monitorResponse = createMonitor(monitor)!!

        var workflow = randomWorkflow(
            monitorIds = listOf(monitorResponse.id)
        )
        val workflowResponse = upsertWorkflow(workflow)!!
        assertNotNull("Workflow creation failed", workflowResponse)

        val delegates = listOf(
            Delegate(1, "monitor-1"),
            Delegate(1, "monitor-2"),
            Delegate(2, "monitor-3")
        )
        workflow = randomWorkflowWithDelegates(
            id = workflowResponse.id,
            delegates = delegates
        )
        try {
            upsertWorkflow(workflow)
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning IndexWorkflow Action error ",
                    it.contains("Sequence ordering of delegate monitor shouldn't contain duplicate order values")
                )
            }
        }
    }

    fun `test create workflow chained findings monitor not in sequence failure`() {
        val delegates = listOf(
            Delegate(1, "monitor-1"),
            Delegate(2, "monitor-2", ChainedMonitorFindings("monitor-1")),
            Delegate(3, "monitor-3", ChainedMonitorFindings("monitor-x"))
        )
        val workflow = randomWorkflowWithDelegates(
            delegates = delegates
        )

        try {
            upsertWorkflow(workflow)
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning IndexWorkflow Action error ",
                    it.contains("Chained Findings Monitor monitor-x doesn't exist in sequence")
                )
            }
        }
    }

    fun `test create workflow query monitor chained findings monitor failure`() {
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3", fields = listOf()))
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)

        val docMonitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )
        val docMonitorResponse = createMonitor(docMonitor)!!

        val queryMonitor = randomQueryLevelMonitor()
        val queryMonitorResponse = createMonitor(queryMonitor)!!

        val workflow = randomWorkflow(
            monitorIds = listOf(queryMonitorResponse.id, docMonitorResponse.id)
        )
        try {
            upsertWorkflow(workflow)
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning IndexWorkflow Action error ",
                    it.contains("Query level monitor can't be part of chained findings")
                )
            }
        }
    }

    fun `test create workflow delegate and chained finding monitor different indices failure`() {
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3", fields = listOf()))
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)

        val docMonitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )
        val docMonitorResponse = createMonitor(docMonitor)!!

        val index1 = "$index-1"
        createTestIndex(index1)

        val docLevelInput1 = DocLevelMonitorInput(
            "description", listOf(index1), listOf(DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3", fields = listOf()))
        )

        val docMonitor1 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput1),
            triggers = listOf(trigger)
        )
        val docMonitorResponse1 = createMonitor(docMonitor1)!!

        val workflow = randomWorkflow(
            monitorIds = listOf(docMonitorResponse1.id, docMonitorResponse.id)
        )
        try {
            upsertWorkflow(workflow)
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning IndexWorkflow Action error ",
                    it.contains("doesn't query all of chained findings monitor's indices")
                )
            }
        }
    }

    fun `test create workflow when monitor index not initialized failure`() {
        val delegates = listOf(
            Delegate(1, "monitor-1")
        )
        val workflow = randomWorkflowWithDelegates(
            delegates = delegates
        )

        try {
            upsertWorkflow(workflow)
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning IndexWorkflow Action error ",
                    it.contains("Monitors not found")
                )
            }
        }
    }

    fun `test update workflow chained findings monitor not in sequence failure`() {
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3", fields = listOf()))
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)

        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )
        val monitorResponse = createMonitor(monitor)!!

        var workflow = randomWorkflow(
            monitorIds = listOf(monitorResponse.id)
        )
        val workflowResponse = upsertWorkflow(workflow)!!
        assertNotNull("Workflow creation failed", workflowResponse)

        val delegates = listOf(
            Delegate(1, "monitor-1"),
            Delegate(2, "monitor-2", ChainedMonitorFindings("monitor-1")),
            Delegate(3, "monitor-3", ChainedMonitorFindings("monitor-x"))
        )
        workflow = randomWorkflowWithDelegates(
            id = workflowResponse.id,
            delegates = delegates
        )

        try {
            upsertWorkflow(workflow)
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning IndexWorkflow Action error ",
                    it.contains("Chained Findings Monitor monitor-x doesn't exist in sequence")
                )
            }
        }
    }

    fun `test create workflow chained findings order not correct failure`() {
        val delegates = listOf(
            Delegate(1, "monitor-1"),
            Delegate(3, "monitor-2", ChainedMonitorFindings("monitor-1")),
            Delegate(2, "monitor-3", ChainedMonitorFindings("monitor-2"))
        )
        val workflow = randomWorkflowWithDelegates(
            delegates = delegates
        )

        try {
            upsertWorkflow(workflow)
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning IndexWorkflow Action error ",
                    it.contains("Chained Findings Monitor monitor-2 should be executed before monitor monitor-3")
                )
            }
        }
    }

    fun `test update workflow chained findings order not correct failure`() {
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3", fields = listOf()))
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)

        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger)
        )
        val monitorResponse = createMonitor(monitor)!!

        var workflow = randomWorkflow(
            monitorIds = listOf(monitorResponse.id)
        )
        val workflowResponse = upsertWorkflow(workflow)!!
        assertNotNull("Workflow creation failed", workflowResponse)

        val delegates = listOf(
            Delegate(1, "monitor-1"),
            Delegate(3, "monitor-2", ChainedMonitorFindings("monitor-1")),
            Delegate(2, "monitor-3", ChainedMonitorFindings("monitor-2"))
        )
        workflow = randomWorkflowWithDelegates(
            delegates = delegates
        )

        try {
            upsertWorkflow(workflow)
        } catch (e: Exception) {
            e.message?.let {
                assertTrue(
                    "Exception not returning IndexWorkflow Action error ",
                    it.contains("Chained Findings Monitor monitor-2 should be executed before monitor monitor-3")
                )
            }
        }
    }

    fun `test create workflow with chained alert triggers`() {
        val docQuery1 = DocLevelQuery(query = "source.ip.v6.v1:12345", name = "3", fields = listOf())
        val docLevelInput = DocLevelMonitorInput(
            "description", listOf(index), listOf(docQuery1)
        )
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        val customFindingsIndex = "custom_findings_index"
        val customFindingsIndexPattern = "custom_findings_index-1"
        val customQueryIndex = "custom_alerts_index"
        val monitor1 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
            dataSources = DataSources(
                queryIndex = customQueryIndex,
                findingsIndex = customFindingsIndex,
                findingsIndexPattern = customFindingsIndexPattern
            )
        )

        val monitor2 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput),
            triggers = listOf(trigger),
            dataSources = DataSources(
                queryIndex = customQueryIndex,
                findingsIndex = customFindingsIndex,
                findingsIndexPattern = customFindingsIndexPattern
            )
        )

        val monitorResponse1 = createMonitor(monitor1)!!
        val monitorResponse2 = createMonitor(monitor2)!!

        val chainedAlertTrigger1 = randomChainedAlertTrigger(
            condition = Script("monitor[id=${monitorResponse1.id}] && monitor[id=${monitorResponse2.id}")
        )
        val chainedAlertTrigger2 = randomChainedAlertTrigger(
            condition = Script("monitor[id=${monitorResponse1.id}] || monitor[id=${monitorResponse2.id}]")
        )
        val workflow = randomWorkflow(
            monitorIds = listOf(monitorResponse1.id, monitorResponse2.id),
            triggers = listOf(
                chainedAlertTrigger1,
                chainedAlertTrigger2
            )
        )
        val workflowResponse = upsertWorkflow(workflow)!!
        val workflowById = searchWorkflow(workflowResponse.id)!!

        assertEquals("Workflow input not correct", workflowById.triggers.size, 2)
        assertEquals("Workflow input not correct", workflowById.triggers.get(0).name, chainedAlertTrigger1.name)
        assertEquals("Workflow input not correct", workflowById.triggers.get(1).name, chainedAlertTrigger2.name)
        assertEquals("Workflow input not correct", workflowById.triggers.get(0).id, chainedAlertTrigger1.id)
        assertEquals("Workflow input not correct", workflowById.triggers.get(1).id, chainedAlertTrigger2.id)
        assertEquals(
            "Workflow input not correct",
            (workflowById.triggers.get(0) as ChainedAlertTrigger).condition.idOrCode,
            chainedAlertTrigger1.condition.idOrCode
        )
        assertEquals(
            "Workflow input not correct",
            (workflowById.triggers.get(1) as ChainedAlertTrigger).condition.idOrCode,
            chainedAlertTrigger2.condition.idOrCode
        )
    }

    fun `test postIndex on workflow update with trigger deletion`() {
        val docQuery1 = DocLevelQuery(query = "test_field_1:\"us-west-2\"", name = "3", fields = listOf())
        val docLevelInput1 = DocLevelMonitorInput("description", listOf(index), listOf(docQuery1))
        val trigger1 = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        var monitor1 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput1),
            triggers = listOf(trigger1)
        )
        var monitor2 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput1),
            triggers = listOf(trigger1)
        )
        val monitorResponse = createMonitor(monitor1)!!
        val monitorResponse2 = createMonitor(monitor2)!!

        val andTrigger = randomChainedAlertTrigger(
            name = "1And2",
            condition = Script("monitor[id=${monitorResponse.id}] && monitor[id=${monitorResponse2.id}]")
        )
        val notTrigger = randomChainedAlertTrigger(
            name = "Not1OrNot2",
            condition = Script("!monitor[id=${monitorResponse.id}] || !monitor[id=${monitorResponse2.id}]")
        )
        var workflow = randomWorkflow(
            monitorIds = listOf(monitorResponse.id, monitorResponse2.id),
            triggers = listOf(andTrigger)
        )
        val workflowResponse = upsertWorkflow(workflow)!!
        val workflowById = searchWorkflow(workflowResponse.id)
        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc1 = """{
            "message" : "This is an error from IAD region",
            "source.ip.v6.v2" : 16644, 
            "test_strict_date_time" : "$testTime",
            "test_field_1" : "us-west-2"
        }"""
        indexDoc(index, "1", testDoc1)
        val workflowId = workflowById!!.id
        var executeWorkflowResponse = executeWorkflow(workflowById, workflowId, false)!!
        var res = getWorkflowAlerts(
            workflowId,
        )
        var chainedAlerts = res.alerts
        Assert.assertTrue(chainedAlerts.size == 1)
        val updatedWorkflowResponse = upsertWorkflow(
            workflowById.copy(triggers = listOf(notTrigger)),
            workflowResponse.id,
            RestRequest.Method.PUT
        )!!
        val updatedWorkflow = searchWorkflow(workflowResponse.id)
        Assert.assertTrue(updatedWorkflow!!.triggers.size == 1)
        Assert.assertTrue(updatedWorkflow.triggers[0].id == notTrigger.id)
        OpenSearchTestCase.waitUntil({
            val searchRequest = SearchRequest(AlertIndices.ALERT_HISTORY_ALL)
            val sr = client().search(searchRequest).get()
            sr.hits.hits.size == 3
        }, 5, TimeUnit.MINUTES)
        val searchRequest = SearchRequest(AlertIndices.ALERT_HISTORY_ALL)
        val sr = client().search(searchRequest).get()
        Assert.assertTrue(sr.hits.hits.size == 3)
        val alerts = sr.hits.map { hit ->
            val xcp = XContentHelper.createParser(
                xContentRegistry(),
                LoggingDeprecationHandler.INSTANCE,
                hit.sourceRef,
                XContentType.JSON
            )
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
            val alert = Alert.parse(xcp, hit.id, hit.version)
            alert
        }
        Assert.assertTrue(alerts.stream().anyMatch { it.state == Alert.State.DELETED && chainedAlerts[0].id == it.id })
    }

    fun `test postDelete on workflow deletion`() {
        val docQuery1 = DocLevelQuery(query = "test_field_1:\"us-west-2\"", name = "3", fields = listOf())
        val docLevelInput1 = DocLevelMonitorInput("description", listOf(index), listOf(docQuery1))
        val trigger1 = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        var monitor1 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput1),
            triggers = listOf(trigger1)
        )
        var monitor2 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput1),
            triggers = listOf(trigger1)
        )
        val monitorResponse = createMonitor(monitor1)!!
        val monitorResponse2 = createMonitor(monitor2)!!

        val andTrigger = randomChainedAlertTrigger(
            name = "1And2",
            condition = Script("monitor[id=${monitorResponse.id}] && monitor[id=${monitorResponse2.id}]")
        )
        val notTrigger = randomChainedAlertTrigger(
            name = "Not1OrNot2",
            condition = Script("!monitor[id=${monitorResponse.id}] || !monitor[id=${monitorResponse2.id}]")
        )
        var workflow = randomWorkflow(
            monitorIds = listOf(monitorResponse.id, monitorResponse2.id),
            triggers = listOf(andTrigger)
        )
        val workflowResponse = upsertWorkflow(workflow)!!
        val workflowById = searchWorkflow(workflowResponse.id)
        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc1 = """{
            "message" : "This is an error from IAD region",
            "source.ip.v6.v2" : 16644, 
            "test_strict_date_time" : "$testTime",
            "test_field_1" : "us-west-2"
        }"""
        indexDoc(index, "1", testDoc1)
        val workflowId = workflowById!!.id
        var executeWorkflowResponse = executeWorkflow(workflowById, workflowId, false)!!
        var res = getWorkflowAlerts(
            workflowId,
        )
        var chainedAlerts = res.alerts
        Assert.assertTrue(chainedAlerts.size == 1)
        val deleteRes = deleteWorkflow(workflowId, false)
        logger.info(deleteRes)
        OpenSearchTestCase.waitUntil({
            val searchRequest = SearchRequest(AlertIndices.ALERT_HISTORY_ALL)
            val sr = client().search(searchRequest).get()
            sr.hits.hits.size == 3
        }, 5, TimeUnit.MINUTES)
        val searchRequest = SearchRequest(AlertIndices.ALERT_HISTORY_ALL)
        val sr = client().search(searchRequest).get()
        Assert.assertTrue(sr.hits.hits.size == 3)
        val alerts = sr.hits.map { hit ->
            val xcp = XContentHelper.createParser(
                xContentRegistry(),
                LoggingDeprecationHandler.INSTANCE,
                hit.sourceRef,
                XContentType.JSON
            )
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
            val alert = Alert.parse(xcp, hit.id, hit.version)
            alert
        }
        Assert.assertTrue(alerts.stream().anyMatch { it.state == Alert.State.DELETED && chainedAlerts[0].id == it.id })
    }

    fun `test get chained alerts with alertId paginating for associated alerts`() {
        val docQuery1 = DocLevelQuery(query = "test_field_1:\"us-west-2\"", name = "3", fields = listOf())
        val docLevelInput1 = DocLevelMonitorInput("description", listOf(index), listOf(docQuery1))
        val trigger1 = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        var monitor1 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput1),
            triggers = listOf(trigger1)
        )
        var monitor2 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput1),
            triggers = listOf(trigger1)
        )
        val monitorResponse = createMonitor(monitor1)!!
        val monitorResponse2 = createMonitor(monitor2)!!

        val andTrigger = randomChainedAlertTrigger(
            name = "1And2",
            condition = Script("monitor[id=${monitorResponse.id}] && monitor[id=${monitorResponse2.id}]")
        )
        var workflow = randomWorkflow(
            monitorIds = listOf(monitorResponse.id, monitorResponse2.id),
            triggers = listOf(andTrigger)
        )
        val workflowResponse = upsertWorkflow(workflow)!!
        val workflowById = searchWorkflow(workflowResponse.id)
        val workflowId = workflowById!!.id
        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc1 = """{
            "message" : "This is an error from IAD region",
            "source.ip.v6.v2" : 16644, 
            "test_strict_date_time" : "$testTime",
            "test_field_1" : "us-west-2"
        }"""
        var i = 1
        val indexRequests = mutableListOf<IndexRequest>()
        while (i++ < 300) {
            indexRequests += IndexRequest(index).source(testDoc1, XContentType.JSON).id("$i").opType(DocWriteRequest.OpType.INDEX)
        }
        val bulkResponse: BulkResponse =
            client().bulk(BulkRequest().add(indexRequests).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)).get()
        if (bulkResponse.hasFailures()) {
            fail("Bulk request to index to test index has failed")
        }
        var executeWorkflowResponse = executeWorkflow(workflowById, workflowId, false)!!
        var res = getWorkflowAlerts(
            workflowId = workflowId
        )
        Assert.assertTrue(executeWorkflowResponse.workflowRunResult.triggerResults[andTrigger.id]!!.triggered)
        var chainedAlerts = res.alerts
        Assert.assertTrue(chainedAlerts.size == 1)
        Assert.assertEquals(res.associatedAlerts.size, 10)
        var res100to200 = getWorkflowAlerts(
            workflowId = workflowId,
            alertIds = listOf(res.alerts[0].id),
            table = Table("asc", "monitor_id", null, 100, 100, null)
        )
        Assert.assertEquals(res100to200.associatedAlerts.size, 100)
        var res200to300 = getWorkflowAlerts(
            workflowId = workflowId,
            alertIds = listOf(res.alerts[0].id),
            table = Table("asc", "monitor_id", null, 100, 201, null)
        )
        Assert.assertEquals(res200to300.associatedAlerts.size, 100)
        var res0to99 = getWorkflowAlerts(
            workflowId = workflowId,
            alertIds = listOf(res.alerts[0].id),
            table = Table("asc", "monitor_id", null, 100, 0, null)
        )
        Assert.assertEquals(res0to99.associatedAlerts.size, 100)

        val ids100to200 = res100to200.associatedAlerts.stream().map { it.id }.collect(Collectors.toSet())
        val idsSet0to99 = res0to99.associatedAlerts.stream().map { it.id }.collect(Collectors.toSet())
        val idsSet200to300 = res200to300.associatedAlerts.stream().map { it.id }.collect(Collectors.toSet())

        Assert.assertTrue(idsSet0to99.all { it !in ids100to200 })
        Assert.assertTrue(idsSet0to99.all { it !in idsSet200to300 })
        Assert.assertTrue(ids100to200.all { it !in idsSet200to300 })
    }

    fun `test existing chained alert active alert is updated on consequtive trigger condition match`() {
        val docQuery1 = DocLevelQuery(query = "test_field_1:\"us-west-2\"", name = "3", fields = listOf())
        val docLevelInput1 = DocLevelMonitorInput("description", listOf(index), listOf(docQuery1))
        val trigger1 = randomDocumentLevelTrigger(condition = ALWAYS_RUN)
        var monitor1 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput1),
            triggers = listOf(trigger1)
        )
        var monitor2 = randomDocumentLevelMonitor(
            inputs = listOf(docLevelInput1),
            triggers = listOf(trigger1)
        )
        val monitorResponse = createMonitor(monitor1)!!
        val monitorResponse2 = createMonitor(monitor2)!!
        val notTrigger = randomChainedAlertTrigger(
            name = "Not1OrNot2",
            condition = Script("!monitor[id=${monitorResponse.id}] || !monitor[id=${monitorResponse2.id}]")
        )
        var workflow = randomWorkflow(
            monitorIds = listOf(monitorResponse.id, monitorResponse2.id),
            triggers = listOf(notTrigger)
        )
        val workflowResponse = upsertWorkflow(workflow)!!
        val workflowById = searchWorkflow(workflowResponse.id)
        val workflowId = workflowById!!.id

        /** no ACTIVE alert exists and chained alert trigger matches. Expect: new ACTIVE alert created**/
        var executeWorkflowResponse = executeWorkflow(workflowById, workflowId, false)!!
        assertTrue(executeWorkflowResponse.workflowRunResult.triggerResults[notTrigger.id]!!.triggered)
        val workflowAlerts = getWorkflowAlerts(workflowId)
        Assert.assertTrue(workflowAlerts.alerts.size == 1)
        Assert.assertEquals(workflowAlerts.alerts[0].state, Alert.State.ACTIVE)
        /** ACTIVE alert exists and chained alert trigger matched again. Expect: existing alert updated and remains in ACTIVE*/
        var executeWorkflowResponse1 = executeWorkflow(workflowById, workflowId, false)!!
        assertTrue(executeWorkflowResponse1.workflowRunResult.triggerResults[notTrigger.id]!!.triggered)
        val udpdatedActiveAlerts = getWorkflowAlerts(workflowId)
        Assert.assertTrue(udpdatedActiveAlerts.alerts.size == 1)
        Assert.assertEquals(udpdatedActiveAlerts.alerts[0].state, Alert.State.ACTIVE)
        Assert.assertTrue(udpdatedActiveAlerts.alerts[0].lastNotificationTime!! > workflowAlerts.alerts[0].lastNotificationTime!!)

        /** Acknowledge ACTIVE alert*/
        val ackChainedAlerts = ackChainedAlerts(udpdatedActiveAlerts.alerts.stream().map { it.id }.collect(Collectors.toList()), workflowId)
        Assert.assertTrue(ackChainedAlerts.acknowledged.size == 1)
        Assert.assertTrue(ackChainedAlerts.missing.size == 0)
        Assert.assertTrue(ackChainedAlerts.failed.size == 0)

        /** ACKNOWLEDGED alert exists and chained alert trigger matched again. Expect: existing alert updated and remains ACKNOWLEDGED*/
        var executeWorkflowResponse2 = executeWorkflow(workflowById, workflowId, false)!!
        assertTrue(executeWorkflowResponse2.workflowRunResult.triggerResults[notTrigger.id]!!.triggered)
        val acknowledgedAlert = getWorkflowAlerts(workflowId, alertState = Alert.State.ACKNOWLEDGED)
        Assert.assertTrue(acknowledgedAlert.alerts.size == 1)
        Assert.assertEquals(acknowledgedAlert.alerts[0].state, Alert.State.ACKNOWLEDGED)
        Assert.assertTrue(acknowledgedAlert.alerts[0].lastNotificationTime!! == udpdatedActiveAlerts.alerts[0].lastNotificationTime!!)

        /** ACKNOWLEDGED alert exists and chained alert trigger NOT matched. Expect: ACKNOWLEDGD alert marked as COMPLETED**/
        val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(MILLIS))
        val testDoc1 = """{
            "message" : "This is an error from IAD region",
            "source.ip.v6.v2" : 16644, 
            "test_strict_date_time" : "$testTime",
            "test_field_1" : "us-west-2"
        }"""
        indexDoc(index, "1", testDoc1)
        var executeWorkflowResponse3 = executeWorkflow(workflowById, workflowId, false)!!
        assertFalse(executeWorkflowResponse3.workflowRunResult.triggerResults[notTrigger.id]!!.triggered)
        val completedAlert = getWorkflowAlerts(workflowId, alertState = Alert.State.COMPLETED)
        Assert.assertTrue(completedAlert.alerts.size == 1)
        Assert.assertEquals(completedAlert.alerts[0].state, Alert.State.COMPLETED)
        Assert.assertTrue(completedAlert.alerts[0].endTime!! > acknowledgedAlert.alerts[0].lastNotificationTime!!)

        /** COMPLETED state alert exists and trigger matches. Expect: new ACTIVE state chaiend alert created*/
        var executeWorkflowResponse4 = executeWorkflow(workflowById, workflowId, false)!!
        assertTrue(executeWorkflowResponse4.workflowRunResult.triggerResults[notTrigger.id]!!.triggered)
        val newActiveAlert = getWorkflowAlerts(workflowId, alertState = Alert.State.ACTIVE)
        Assert.assertTrue(newActiveAlert.alerts.size == 1)
        Assert.assertEquals(newActiveAlert.alerts[0].state, Alert.State.ACTIVE)
        Assert.assertTrue(newActiveAlert.alerts[0].lastNotificationTime!! > acknowledgedAlert.alerts[0].lastNotificationTime!!)
        val completedAlert1 = getWorkflowAlerts(workflowId, alertState = Alert.State.COMPLETED)
        Assert.assertTrue(completedAlert1.alerts.size == 1)
        Assert.assertEquals(completedAlert1.alerts[0].state, Alert.State.COMPLETED)
        Assert.assertTrue(completedAlert1.alerts[0].endTime!! > acknowledgedAlert.alerts[0].lastNotificationTime!!)
    }
}

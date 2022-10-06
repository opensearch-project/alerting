package org.opensearch.alerting

import org.junit.Assert
import org.opensearch.alerting.action.GetFindingsAction
import org.opensearch.alerting.action.GetFindingsRequest
import org.opensearch.alerting.core.model.DocLevelMonitorInput
import org.opensearch.alerting.core.model.DocLevelQuery
import org.opensearch.alerting.model.DataSources
import org.opensearch.alerting.model.Table
import org.opensearch.alerting.transport.AlertingSingleNodeTestCase
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit.MILLIS

class GetFindingsIT : AlertingSingleNodeTestCase() {

    fun `test get all findings`() {
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

        monitor = monitorResponse!!.monitor
        indexDoc(index, "1", testDoc)
        val id = monitorResponse.id
        executeMonitor(monitor, id, false)

        val findings = searchFindings(id, customFindingsIndex)
        assertEquals("Findings saved for test monitor", 1, findings.size)
        assertTrue("Findings saved for test monitor", findings[0].relatedDocIds.contains("1"))
        val table = Table("asc", "id", null, 1, 0, "")
        var getFindingsResponse = client()
            .execute(GetFindingsAction.INSTANCE, GetFindingsRequest(null, table, null, customFindingsIndex))
            .get()
        Assert.assertTrue(getFindingsResponse != null)
        Assert.assertTrue(getFindingsResponse.totalFindings == 1)
        var getFindingsResponse1 = client()
            .execute(GetFindingsAction.INSTANCE, GetFindingsRequest(null, table, null, customFindingsIndex, listOf(id)))
            .get()
        Assert.assertTrue(getFindingsResponse1 != null)
        Assert.assertTrue(getFindingsResponse1.totalFindings == 1)
        var getFindingsResponse2 = client()
            .execute(GetFindingsAction.INSTANCE, GetFindingsRequest(null, table, id, customFindingsIndex))
            .get()
        Assert.assertTrue(getFindingsResponse2 != null)
        Assert.assertTrue(getFindingsResponse2.totalFindings == 1)
    }

    fun `test get findings by list of monitors containing both existent and non-existent ids`() {
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
        val findings = searchFindings(id)
        assertEquals("Findings saved for test monitor", 1, findings.size)
        assertTrue("Findings saved for test monitor", findings[0].relatedDocIds.contains("1"))
        val findings1 = searchFindings(id)
        assertEquals("Findings saved for test monitor", 1, findings1.size)
        assertTrue("Findings saved for test monitor", findings1[0].relatedDocIds.contains("1"))
        val table = Table("asc", "id", null, 2, 0, "")
        var getFindingsResponse = client()
            .execute(GetFindingsAction.INSTANCE, GetFindingsRequest(null, table, null))
            .get()
        Assert.assertTrue(getFindingsResponse != null)
        Assert.assertTrue(getFindingsResponse.totalFindings == 2)

        var getFindingsResponse1 = client()
            .execute(GetFindingsAction.INSTANCE, GetFindingsRequest(null, table, null, null, listOf(id, id1, "1", "2")))
            .get()
        Assert.assertTrue(getFindingsResponse1 != null)
        Assert.assertTrue(getFindingsResponse1.totalFindings == 2)
    }
}

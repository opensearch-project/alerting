package org.opensearch.alerting

import org.junit.Assert
import org.opensearch.alerting.action.GetAlertsAction
import org.opensearch.alerting.action.GetAlertsRequest
import org.opensearch.alerting.core.model.DocLevelMonitorInput
import org.opensearch.alerting.core.model.DocLevelQuery
import org.opensearch.alerting.model.DataSources
import org.opensearch.alerting.model.Table
import org.opensearch.alerting.transport.AlertingSingleNodeTestCase
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit.MILLIS

class GetAlertsIT : AlertingSingleNodeTestCase() {

    fun `test get all alerts`() {
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

        monitor = monitorResponse!!.monitor
        indexDoc(index, "1", testDoc)
        val id = monitorResponse.id
        executeMonitor(monitor, id, false)

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
                GetAlertsRequest(table, "ALL", "ALL", null, null)
            )
            .get()

        Assert.assertTrue(getAlertsResponse != null)
        Assert.assertTrue(getAlertsResponse.alerts.size == 2)

        var alertsResponseForRequestWithoutCustomIndex = client()
            .execute(
                GetAlertsAction.INSTANCE,
                GetAlertsRequest(table, "ALL", "ALL", null, null, listOf(id, id1, "1", "2"))
            )
            .get()
        Assert.assertTrue(alertsResponseForRequestWithoutCustomIndex != null)
        Assert.assertTrue(alertsResponseForRequestWithoutCustomIndex.alerts.size == 2)
    }
}

package org.opensearch.alerting.resthandler

import org.apache.http.HttpHeaders
import org.apache.http.message.BasicHeader
import org.opensearch.alerting.*
import org.opensearch.alerting.core.model.Input
import org.opensearch.alerting.core.model.IntervalSchedule
import org.opensearch.alerting.core.model.Schedule
import org.opensearch.alerting.core.model.SearchInput
import org.opensearch.alerting.model.Monitor
import org.opensearch.alerting.model.QueryLevelTrigger
import org.opensearch.alerting.model.Trigger
import org.opensearch.commons.authuser.User
import org.opensearch.index.query.QueryBuilders
import org.opensearch.rest.RestStatus
import org.opensearch.script.Script
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.test.rest.OpenSearchRestTestCase
import java.time.Instant
import java.time.temporal.ChronoUnit

class MonitorRestApilTForDocument: AlertingRestTestCase() {


    @Throws(Exception::class)
    fun `test creating a document monitor`() {
        val monitor = randomDocumentLevelMonitor()

        val createResponse = client().makeRequest("POST", ALERTING_BASE_URI, emptyMap(), monitor.toHttpEntity())

        assertEquals("Create monitor failed", RestStatus.CREATED, createResponse.restStatus())
        val responseBody = createResponse.asMap()
        val createdId = responseBody["_id"] as String
        val createdVersion = responseBody["_version"] as Int
        assertNotEquals("response is missing Id", Monitor.NO_ID, createdId)
        assertTrue("incorrect version", createdVersion > 0)
        assertEquals("Incorrect Location header", "$ALERTING_BASE_URI/$createdId", createResponse.getHeader("Location"))
    }

    @Throws(Exception::class)
    fun `test getting a monitor`() {
        val monitor = createRandomDocumentMonitor()

        val storedMonitor = getMonitor(monitor.id)

        assertEquals("Indexed and retrieved monitor differ", monitor, storedMonitor)
    }



    protected fun createRandomDocumentMonitor(refresh: Boolean = false, withMetadata: Boolean = false): Monitor {
        val monitor = randomDocumentLevelMonitor(withMetadata = withMetadata)
        val monitorId = createMonitor(monitor, refresh).id
        if (withMetadata) {
            return getMonitor(monitorId = monitorId, header = BasicHeader(HttpHeaders.USER_AGENT, "OpenSearch-Dashboards"))
        }
        return getMonitor(monitorId = monitorId)
    }


    @Throws(Exception::class)
    fun `test updating conditions for a monitor`() {
        val monitor = createRandomDocumentMonitor()

        val updatedTriggers = listOf(
            QueryLevelTrigger(
                name = "foo",
                severity = "1",
                condition = Script("return true"),
                actions = emptyList()
            )
        )
        val updateResponse = OpenSearchRestTestCase.client().makeRequest(
            "PUT", monitor.relativeUrl(),
            emptyMap(), monitor.copy(triggers = updatedTriggers).toHttpEntity()
        )

        OpenSearchRestTestCase.assertEquals("Update monitor failed", RestStatus.OK, updateResponse.restStatus())
        val responseBody = updateResponse.asMap()
        OpenSearchRestTestCase.assertEquals("Updated monitor id doesn't match", monitor.id, responseBody["_id"] as String)
        OpenSearchRestTestCase.assertEquals("Version not incremented", (monitor.version + 1).toInt(), responseBody["_version"] as Int)

        val updatedMonitor = getMonitor(monitor.id)
        OpenSearchRestTestCase.assertEquals("Monitor trigger not updated", updatedTriggers, updatedMonitor.triggers)
    }

    @Throws(Exception::class)
    fun `test deleting a monitor`() {
        val monitor = createRandomDocumentMonitor()

        val deleteResponse = client().makeRequest("DELETE", monitor.relativeUrl())
        assertEquals("Delete failed", RestStatus.OK, deleteResponse.restStatus())

        val getResponse = client().makeRequest("HEAD", monitor.relativeUrl())
        assertEquals("Deleted monitor still exists", RestStatus.NOT_FOUND, getResponse.restStatus())
    }

    fun randomDocumentLevelMonitor(
        name: String = OpenSearchRestTestCase.randomAlphaOfLength(10),
        user: User = randomUser(),
        inputs: List<Input> = listOf(
            SearchInput(
                emptyList(),
                SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).aggregation(TermsAggregationBuilder("test_agg"))
            )
        ),
        schedule: Schedule = IntervalSchedule(interval = 5, unit = ChronoUnit.MINUTES),
        enabled: Boolean = randomBoolean(),
        triggers: List<Trigger> = (1..randomInt(10)).map { randomBucketLevelTrigger() },
        enabledTime: Instant? = if (enabled) Instant.now().truncatedTo(ChronoUnit.MILLIS) else null,
        lastUpdateTime: Instant = Instant.now().truncatedTo(ChronoUnit.MILLIS),
        withMetadata: Boolean = false
    ): Monitor {
        return Monitor(
            name = name, monitorType = Monitor.MonitorType.DOC_LEVEL_MONITOR, enabled = enabled, inputs = inputs,
            schedule = schedule, triggers = triggers, enabledTime = enabledTime, lastUpdateTime = lastUpdateTime, user = user,
            lastRunContext = mapOf(), uiMetadata = if (withMetadata) mapOf("foo" to "bar") else mapOf()
        )
    }
}

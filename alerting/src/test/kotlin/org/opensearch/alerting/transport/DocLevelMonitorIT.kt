package org.opensearch.alerting.transport

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest
import org.opensearch.action.support.WriteRequest
import org.opensearch.alerting.ALWAYS_RUN
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.action.ExecuteMonitorAction
import org.opensearch.alerting.action.ExecuteMonitorRequest
import org.opensearch.alerting.randomDocumentLevelMonitor
import org.opensearch.alerting.randomDocumentLevelTrigger
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentType
import org.opensearch.common.xcontent.json.JsonXContent
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.action.IndexMonitorRequest
import org.opensearch.commons.alerting.model.DocLevelMonitorInput
import org.opensearch.commons.alerting.model.DocLevelQuery
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.MonitorMetadata
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.index.query.TermQueryBuilder
import org.opensearch.index.reindex.ReindexModulePlugin
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.join.ParentJoinModulePlugin
import org.opensearch.painless.PainlessModulePlugin
import org.opensearch.plugins.Plugin
import org.opensearch.rest.RestRequest
import org.opensearch.script.mustache.MustacheModulePlugin
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.test.OpenSearchIntegTestCase
import java.time.Instant
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.Locale

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 5)
@ThreadLeakScope(value = ThreadLeakScope.Scope.NONE)
class DocLevelMonitorIT : OpenSearchIntegTestCase() {

    fun `test execute doc level monitor with monitor timebox breached`() {
        val indexName = randomAlphaOfLength(20).lowercase(Locale.ROOT)

        val indexSettings = Settings.builder()
            .put("index.number_of_shards", 4)
            .put("index.number_of_replicas", 0)
            .build()

        val mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("test_strict_date_time")
            .field("type", "date")
            .field("format", "strict_date_time")
            .endObject()
            .startObject("test_field")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()

        client().admin().indices().prepareCreate(indexName)
            .setSettings(indexSettings)
            .setMapping(mapping)
            .get()

        val docQuery = DocLevelQuery(
            query = "test_field:\"us-west-2\"",
            name = "3",
            id = "random",
            tags = listOf(),
            fields = listOf()
        )
        val docInput = DocLevelMonitorInput("description", listOf(indexName), listOf(docQuery))
        val trigger = randomDocumentLevelTrigger(condition = ALWAYS_RUN)

        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(docInput),
            triggers = listOf(trigger)
        )

        val monitorResponse = client().execute(
            AlertingActions.INDEX_MONITOR_ACTION_TYPE,
            IndexMonitorRequest(
                monitorId = Monitor.NO_ID,
                seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO,
                primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
                refreshPolicy = WriteRequest.RefreshPolicy.IMMEDIATE,
                method = RestRequest.Method.POST,
                monitor = monitor
            )
        ).get()

        val createdMonitor = monitorResponse.monitor
        val monitorId = monitorResponse.id
        assertFalse(monitorId.isNullOrEmpty())

        val now = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now().truncatedTo(ChronoUnit.MILLIS))
        val doc = """
            {
              "message" : "This is an error from IAD region",
              "test_strict_date_time" : "$now",
              "test_field" : "us-west-2"
            }
        """.trimIndent()

        client().prepareIndex(indexName).setId("1")
            .setSource(doc, XContentType.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get()

        val executeResponse = client().execute(
            ExecuteMonitorAction.INSTANCE,
            ExecuteMonitorRequest(false, TimeValue(Instant.now().toEpochMilli()), monitorId, createdMonitor)
        ).get()

        assertEquals(createdMonitor.name, executeResponse.monitorRunResult.monitorName)
        assertEquals(1, executeResponse.monitorRunResult.triggerResults.size)

        val metadata1 = searchMonitorMetadata("$monitorId-metadata")
        assertNotNull(metadata1)

        // Update cluster setting to breach timebox
        val settings = Settings.builder()
            .put(AlertingSettings.DOC_LEVEL_MONITOR_EXECUTION_MAX_DURATION.key, TimeValue.timeValueMillis(1))
        client().admin().cluster().updateSettings(
            ClusterUpdateSettingsRequest().persistentSettings(settings)
        ).actionGet()

        val executeResponse2 = client().execute(
            ExecuteMonitorAction.INSTANCE,
            ExecuteMonitorRequest(false, TimeValue(Instant.now().toEpochMilli()), monitorId, createdMonitor)
        ).get()

        val metadata2 = searchMonitorMetadata("$monitorId-metadata")
        assertNotNull(metadata2)
        assertEquals(metadata1, metadata2)

        // Revert setting
        val revertSettings = Settings.builder()
            .putNull(AlertingSettings.DOC_LEVEL_MONITOR_EXECUTION_MAX_DURATION.key)
        client().admin().cluster().updateSettings(
            ClusterUpdateSettingsRequest().persistentSettings(revertSettings)
        ).actionGet()
    }

    private fun searchMonitorMetadata(id: String): MonitorMetadata? {
        val ssb = SearchSourceBuilder().query(TermQueryBuilder("_id", id)).version(true)
        val response = client().prepareSearch(ScheduledJob.SCHEDULED_JOBS_INDEX)
            .setRouting(id)
            .setSource(ssb)
            .get()

        return response.hits.hits.mapNotNull {
            val parser = createParser(JsonXContent.jsonXContent, it.sourceRef).also { p -> p.nextToken() }
            var metadata: MonitorMetadata? = null
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                parser.nextToken()
                if (parser.currentName() == "metadata") {
                    metadata = MonitorMetadata.parse(parser).copy(id = it.id)
                }
            }
            metadata
        }.firstOrNull()
    }

    override fun nodePlugins(): MutableCollection<Class<out Plugin>> {
        return mutableListOf(
            AlertingPlugin::class.java,
            MustacheModulePlugin::class.java,
            ReindexModulePlugin::class.java,
            PainlessModulePlugin::class.java,
            ParentJoinModulePlugin::class.java
        )
    }

    override fun nodeSettings(nodeOrdinal: Int): Settings {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put("opendistro.scheduled_jobs.sweeper.period", TimeValue.timeValueSeconds(5))
            .put("opendistro.scheduled_jobs.enabled", true)
            .build()
    }
}

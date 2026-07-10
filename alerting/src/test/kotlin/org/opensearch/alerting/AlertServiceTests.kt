/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import kotlinx.coroutines.runBlocking
import org.apache.lucene.search.TotalHits
import org.junit.Before
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.mockito.Mockito.verify
import org.opensearch.Version
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.search.SearchResponseSections
import org.opensearch.action.search.ShardSearchFailure
import org.opensearch.alerting.alerts.AlertIndices
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.util.getBucketKeysHash
import org.opensearch.cluster.node.DiscoveryNode
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.ClusterSettings
import org.opensearch.common.settings.Setting
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.commons.alerting.model.AggregationResultBucket
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.commons.alerting.model.BucketLevelTrigger
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.action.AlertCategory
import org.opensearch.core.common.bytes.BytesReference
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.remote.metadata.client.SdkClient
import org.opensearch.remote.metadata.client.SearchDataObjectRequest
import org.opensearch.remote.metadata.client.SearchDataObjectResponse
import org.opensearch.search.SearchHit
import org.opensearch.search.SearchHits
import org.opensearch.test.ClusterServiceUtils
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.client.Client
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.CompletableFuture
import org.mockito.Mockito.`when` as whenever
class AlertServiceTests : OpenSearchTestCase() {

    private lateinit var client: Client
    private lateinit var xContentRegistry: NamedXContentRegistry
    private lateinit var settings: Settings
    private lateinit var threadPool: ThreadPool
    private lateinit var clusterService: ClusterService

    private lateinit var alertIndices: AlertIndices
    private lateinit var alertService: AlertService
    private lateinit var sdkClient: SdkClient

    @Before
    fun setup() {
        // TODO: If more *Service unit tests are added, this configuration can be moved to some base class for each service test class to use
        client = Mockito.mock(Client::class.java)
        xContentRegistry = Mockito.mock(NamedXContentRegistry::class.java)
        threadPool = Mockito.mock(ThreadPool::class.java)
        clusterService = Mockito.mock(ClusterService::class.java)
        settings = Settings.builder().build()
        val settingSet = hashSetOf<Setting<*>>()
        settingSet.addAll(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        settingSet.add(AlertingSettings.ALERT_HISTORY_ENABLED)
        settingSet.add(AlertingSettings.ALERT_HISTORY_MAX_DOCS)
        settingSet.add(AlertingSettings.ALERT_HISTORY_INDEX_MAX_AGE)
        settingSet.add(AlertingSettings.ALERT_HISTORY_ROLLOVER_PERIOD)
        settingSet.add(AlertingSettings.ALERT_HISTORY_RETENTION_PERIOD)
        settingSet.add(AlertingSettings.REQUEST_TIMEOUT)
        settingSet.add(AlertingSettings.FINDING_HISTORY_ENABLED)
        settingSet.add(AlertingSettings.FINDING_HISTORY_MAX_DOCS)
        settingSet.add(AlertingSettings.FINDING_HISTORY_INDEX_MAX_AGE)
        settingSet.add(AlertingSettings.FINDING_HISTORY_ROLLOVER_PERIOD)
        settingSet.add(AlertingSettings.FINDING_HISTORY_RETENTION_PERIOD)
        val discoveryNode = DiscoveryNode("node", buildNewFakeTransportAddress(), Version.CURRENT)
        val clusterSettings = ClusterSettings(settings, settingSet)
        val testClusterService = ClusterServiceUtils.createClusterService(threadPool, discoveryNode, clusterSettings)
        clusterService = Mockito.spy(testClusterService)

        alertIndices = AlertIndices(settings, client, threadPool, clusterService)
        sdkClient = Mockito.mock(SdkClient::class.java)
        alertService = AlertService(client, xContentRegistry, alertIndices, sdkClient)
    }

    fun `test getting categorized alerts for bucket-level monitor with no current alerts`() {
        val trigger = randomBucketLevelTrigger()
        val monitor = randomBucketLevelMonitor(triggers = listOf(trigger))

        val currentAlerts = mutableMapOf<String, Alert>()
        val aggResultBuckets = createAggregationResultBucketsFromBucketKeys(
            listOf(
                listOf("a"),
                listOf("b")
            )
        )

        val categorizedAlerts = alertService.getCategorizedAlertsForBucketLevelMonitor(
            monitor, trigger, currentAlerts, aggResultBuckets, emptyList(), "", null
        )
        // Completed Alerts are what remains in currentAlerts after categorization
        val completedAlerts = currentAlerts.values.toList()
        assertEquals(listOf<Alert>(), categorizedAlerts[AlertCategory.DEDUPED])
        assertAlertsExistForBucketKeys(
            listOf(
                listOf("a"),
                listOf("b")
            ),
            categorizedAlerts[AlertCategory.NEW] ?: error("New alerts not found")
        )
        assertEquals(listOf<Alert>(), completedAlerts)
    }

    fun `test getting categorized alerts for bucket-level monitor with de-duped alerts`() {
        val trigger = randomBucketLevelTrigger()
        val monitor = randomBucketLevelMonitor(triggers = listOf(trigger))

        val currentAlerts = createCurrentAlertsFromBucketKeys(
            monitor, trigger,
            listOf(
                listOf("a"),
                listOf("b")
            )
        )
        val aggResultBuckets = createAggregationResultBucketsFromBucketKeys(
            listOf(
                listOf("a"),
                listOf("b")
            )
        )

        val categorizedAlerts = alertService.getCategorizedAlertsForBucketLevelMonitor(
            monitor, trigger, currentAlerts, aggResultBuckets, emptyList(), "", null
        )
        // Completed Alerts are what remains in currentAlerts after categorization
        val completedAlerts = currentAlerts.values.toList()
        assertAlertsExistForBucketKeys(
            listOf(
                listOf("a"),
                listOf("b")
            ),
            categorizedAlerts[AlertCategory.DEDUPED] ?: error("Deduped alerts not found")
        )
        assertEquals(listOf<Alert>(), categorizedAlerts[AlertCategory.NEW])
        assertEquals(listOf<Alert>(), completedAlerts)
    }

    fun `test getting categorized alerts for bucket-level monitor with completed alerts`() {
        val trigger = randomBucketLevelTrigger()
        val monitor = randomBucketLevelMonitor(triggers = listOf(trigger))

        val currentAlerts = createCurrentAlertsFromBucketKeys(
            monitor, trigger,
            listOf(
                listOf("a"),
                listOf("b")
            )
        )
        val aggResultBuckets = listOf<AggregationResultBucket>()

        val categorizedAlerts = alertService.getCategorizedAlertsForBucketLevelMonitor(
            monitor, trigger, currentAlerts, aggResultBuckets, emptyList(), "", null
        )
        // Completed Alerts are what remains in currentAlerts after categorization
        val completedAlerts = currentAlerts.values.toList()
        assertEquals(listOf<Alert>(), categorizedAlerts[AlertCategory.DEDUPED])
        assertEquals(listOf<Alert>(), categorizedAlerts[AlertCategory.NEW])
        assertAlertsExistForBucketKeys(
            listOf(
                listOf("a"),
                listOf("b")
            ),
            completedAlerts
        )
    }

    fun `test getting categorized alerts for bucket-level monitor with de-duped and completed alerts`() {
        val trigger = randomBucketLevelTrigger()
        val monitor = randomBucketLevelMonitor(triggers = listOf(trigger))

        val currentAlerts = createCurrentAlertsFromBucketKeys(
            monitor, trigger,
            listOf(
                listOf("a"),
                listOf("b")
            )
        )
        val aggResultBuckets = createAggregationResultBucketsFromBucketKeys(
            listOf(
                listOf("b"),
                listOf("c")
            )
        )

        val categorizedAlerts = alertService.getCategorizedAlertsForBucketLevelMonitor(
            monitor, trigger, currentAlerts, aggResultBuckets, emptyList(), "", null
        )
        // Completed Alerts are what remains in currentAlerts after categorization
        val completedAlerts = currentAlerts.values.toList()
        assertAlertsExistForBucketKeys(listOf(listOf("b")), categorizedAlerts[AlertCategory.DEDUPED] ?: error("Deduped alerts not found"))
        assertAlertsExistForBucketKeys(listOf(listOf("c")), categorizedAlerts[AlertCategory.NEW] ?: error("New alerts not found"))
        assertAlertsExistForBucketKeys(listOf(listOf("a")), completedAlerts)
    }

    fun `test getting categorized alerts for bucket-level monitor with de-duped alerts size 1`() {
        val trigger = randomBucketLevelTrigger()
        val monitor = randomBucketLevelMonitor(triggers = listOf(trigger))

        val currentAlerts = createCurrentAlertsFromBucketKeys(
            monitor, trigger,
            listOf(
                listOf("a")
            )
        )
        val aggResultBuckets = createAggregationResultBucketsFromBucketKeys(
            listOf(
                listOf("a"),
            )
        )

        val categorizedAlerts = alertService.getCategorizedAlertsForBucketLevelMonitor(
            monitor, trigger, currentAlerts, aggResultBuckets, emptyList(), "", null
        )
        // Completed Alerts are what remains in currentAlerts after categorization
        val completedAlerts = currentAlerts.values.toList()
        assertAlertsExistForBucketKeys(listOf(listOf("a")), categorizedAlerts[AlertCategory.DEDUPED] ?: error("Deduped alerts not found"))
        assertAlertsExistForBucketKeys(emptyList(), categorizedAlerts[AlertCategory.NEW] ?: error("New alerts found"))
        assertAlertsExistForBucketKeys(emptyList(), completedAlerts)
    }

    fun `test loadCurrentAlertsForQueryLevelMonitor returns empty alerts for dry-run monitor with blank id`() {
        val trigger = randomQueryLevelTrigger()
        val monitor = randomQueryLevelMonitor(triggers = listOf(trigger)).copy(id = Monitor.NO_ID)

        val result = runBlocking {
            alertService.loadCurrentAlertsForQueryLevelMonitor(monitor, null)
        }

        assertEquals(1, result.size)
        assertTrue(result.containsKey(trigger))
        assertNull(result[trigger])
    }

    fun `test loadCurrentAlertsForBucketLevelMonitor returns empty alerts for dry-run monitor with blank id`() {
        val trigger = randomBucketLevelTrigger()
        val monitor = randomBucketLevelMonitor(triggers = listOf(trigger)).copy(id = Monitor.NO_ID)

        val result = runBlocking {
            alertService.loadCurrentAlertsForBucketLevelMonitor(monitor, null)
        }

        assertEquals(1, result.size)
        assertTrue(result.containsKey(trigger))
        assertTrue(result[trigger]!!.isEmpty())
    }

    fun `test loadCurrentAlertsForQueryLevelMonitor excludes terminal-state alerts`() {
        val trigger = randomQueryLevelTrigger()
        val monitor = randomQueryLevelMonitor(triggers = listOf(trigger))

        val active = randomAlert(monitor).copy(triggerId = trigger.id, triggerName = trigger.name, state = Alert.State.ACTIVE)
        val completed = randomAlert(monitor).copy(triggerId = trigger.id, triggerName = trigger.name, state = Alert.State.COMPLETED)
        stubSearchAlerts(listOf(completed, active))

        val result = runBlocking { alertService.loadCurrentAlertsForQueryLevelMonitor(monitor, null) }

        assertEquals(1, result.size)
        val loaded = result[trigger]
        assertNotNull("Active alert should be loaded", loaded)
        assertEquals(Alert.State.ACTIVE, loaded!!.state)
        assertEquals(active.id, loaded.id)
    }

    fun `test loadCurrentAlertsForQueryLevelMonitor returns null when only terminal alerts present`() {
        val trigger = randomQueryLevelTrigger()
        val monitor = randomQueryLevelMonitor(triggers = listOf(trigger))

        val completed = randomAlert(monitor).copy(triggerId = trigger.id, triggerName = trigger.name, state = Alert.State.COMPLETED)
        val deleted = randomAlert(monitor).copy(triggerId = trigger.id, triggerName = trigger.name, state = Alert.State.DELETED)
        stubSearchAlerts(listOf(completed, deleted))

        val result = runBlocking { alertService.loadCurrentAlertsForQueryLevelMonitor(monitor, null) }

        assertEquals(1, result.size)
        assertTrue(result.containsKey(trigger))
        assertNull("Terminal-state alerts must be excluded from current alerts", result[trigger])
    }

    fun `test searchAlerts emits server-side must_not excluding terminal states`() {
        val trigger = randomQueryLevelTrigger()
        val monitor = randomQueryLevelMonitor(triggers = listOf(trigger))
        stubSearchAlerts(emptyList())

        runBlocking { alertService.loadCurrentAlertsForQueryLevelMonitor(monitor, null) }

        val captor = ArgumentCaptor.forClass(SearchDataObjectRequest::class.java)
        verify(sdkClient).searchDataObjectAsync(captor.capture())
        val query = captor.value.searchSourceBuilder().query().toString()
        assertTrue("query should exclude terminal states via must_not", query.contains("must_not"))
        assertTrue("must_not should target the state field", query.contains(Alert.STATE_FIELD))
        assertTrue("COMPLETED should be excluded server-side", query.contains(Alert.State.COMPLETED.name))
        assertTrue("DELETED should be excluded server-side", query.contains(Alert.State.DELETED.name))
    }

    private fun stubSearchAlerts(alerts: List<Alert>) {
        val hits = alerts.map { alert ->
            val builder = XContentFactory.jsonBuilder()
            alert.toXContentWithUser(builder)
            SearchHit(0, alert.id, emptyMap(), emptyMap()).sourceRef(BytesReference.bytes(builder))
        }.toTypedArray()
        val searchHits = SearchHits(hits, TotalHits(hits.size.toLong(), TotalHits.Relation.EQUAL_TO), 1.0f)
        val sections = SearchResponseSections(searchHits, null, null, false, false, null, 1)
        val searchResponse = SearchResponse(sections, null, 1, 1, 0, 1L, arrayOf<ShardSearchFailure>(), SearchResponse.Clusters.EMPTY)
        whenever(sdkClient.searchDataObjectAsync(any(SearchDataObjectRequest::class.java)))
            .thenReturn(CompletableFuture.completedFuture(SearchDataObjectResponse(searchResponse)))
    }

    private fun createCurrentAlertsFromBucketKeys(
        monitor: Monitor,
        trigger: BucketLevelTrigger,
        bucketKeysList: List<List<String>>
    ): MutableMap<String, Alert> {
        return bucketKeysList.map { bucketKeys ->
            val aggResultBucket = AggregationResultBucket("parent_bucket_path", bucketKeys, mapOf())
            val alert = Alert(
                monitor, trigger, Instant.now().truncatedTo(ChronoUnit.MILLIS), null,
                actionExecutionResults = listOf(randomActionExecutionResult()), aggregationResultBucket = aggResultBucket
            )
            aggResultBucket.getBucketKeysHash() to alert
        }.toMap().toMutableMap()
    }

    private fun createAggregationResultBucketsFromBucketKeys(bucketKeysList: List<List<String>>): List<AggregationResultBucket> {
        return bucketKeysList.map { AggregationResultBucket("parent_bucket_path", it, mapOf()) }
    }

    private fun assertAlertsExistForBucketKeys(bucketKeysList: List<List<String>>, alerts: List<Alert>) {
        // Check if size is equals first for sanity and since bucketKeysList should have unique entries,
        // this ensures there shouldn't be duplicates in the alerts
        assertEquals(bucketKeysList.size, alerts.size)
        val expectedBucketKeyHashes = bucketKeysList.map { it.joinToString(separator = "#") }.toSet()
        alerts.forEach { alert ->
            assertNotNull(alert.aggregationResultBucket)
            assertTrue(expectedBucketKeyHashes.contains(alert.aggregationResultBucket!!.getBucketKeysHash()))
        }
    }
}

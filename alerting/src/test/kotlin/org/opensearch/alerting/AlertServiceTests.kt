/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.junit.Before
import org.mockito.Mockito
import org.opensearch.Version
import org.opensearch.alerting.alerts.AlertIndices
import org.opensearch.alerting.model.AggregationResultBucket
import org.opensearch.alerting.model.Alert
import org.opensearch.alerting.model.BucketLevelTrigger
import org.opensearch.alerting.model.Monitor
import org.opensearch.alerting.model.action.AlertCategory
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.util.getBucketKeysHash
import org.opensearch.client.Client
import org.opensearch.cluster.node.DiscoveryNode
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.ClusterSettings
import org.opensearch.common.settings.Setting
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.test.ClusterServiceUtils
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.threadpool.ThreadPool
import java.time.Instant
import java.time.temporal.ChronoUnit

class AlertServiceTests : OpenSearchTestCase() {

    private lateinit var client: Client
    private lateinit var xContentRegistry: NamedXContentRegistry
    private lateinit var settings: Settings
    private lateinit var threadPool: ThreadPool
    private lateinit var clusterService: ClusterService

    private lateinit var alertIndices: AlertIndices
    private lateinit var alertService: AlertService

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
        alertService = AlertService(client, xContentRegistry, alertIndices)
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

        val categorizedAlerts = alertService.getCategorizedAlertsForBucketLevelMonitor(monitor, trigger, currentAlerts, aggResultBuckets)
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

        val categorizedAlerts = alertService.getCategorizedAlertsForBucketLevelMonitor(monitor, trigger, currentAlerts, aggResultBuckets)
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

        val categorizedAlerts = alertService.getCategorizedAlertsForBucketLevelMonitor(monitor, trigger, currentAlerts, aggResultBuckets)
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

        val categorizedAlerts = alertService.getCategorizedAlertsForBucketLevelMonitor(monitor, trigger, currentAlerts, aggResultBuckets)
        // Completed Alerts are what remains in currentAlerts after categorization
        val completedAlerts = currentAlerts.values.toList()
        assertAlertsExistForBucketKeys(listOf(listOf("b")), categorizedAlerts[AlertCategory.DEDUPED] ?: error("Deduped alerts not found"))
        assertAlertsExistForBucketKeys(listOf(listOf("c")), categorizedAlerts[AlertCategory.NEW] ?: error("New alerts not found"))
        assertAlertsExistForBucketKeys(listOf(listOf("a")), completedAlerts)
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
        }.toMap() as MutableMap<String, Alert>
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

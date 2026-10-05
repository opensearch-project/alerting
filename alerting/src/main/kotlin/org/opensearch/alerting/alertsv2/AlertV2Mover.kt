package org.opensearch.alerting.alertsv2

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.action.bulk.BulkRequest
import org.opensearch.action.bulk.BulkResponse
import org.opensearch.action.delete.DeleteRequest
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.alerting.core.modelv2.AlertV2
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.settings.AlertingSettings.Companion.ALERT_V2_HISTORY_ENABLED
import org.opensearch.cluster.ClusterChangedEvent
import org.opensearch.cluster.ClusterStateListener
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.core.common.bytes.BytesReference
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParserUtils
import org.opensearch.index.VersionType
import org.opensearch.index.query.QueryBuilders
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.threadpool.Scheduler
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.client.Client
import java.time.Instant
import java.util.concurrent.TimeUnit

private val scope: CoroutineScope = CoroutineScope(Dispatchers.IO)
private val logger = LogManager.getLogger(AlertV2Mover::class.java)

class AlertV2Mover(
    settings: Settings,
    private val client: Client,
    private val threadPool: ThreadPool,
    private val clusterService: ClusterService,
) : ClusterStateListener {
    init {
        clusterService.addListener(this)
        clusterService.clusterSettings.addSettingsUpdateConsumer(ALERT_V2_HISTORY_ENABLED) { alertV2HistoryEnabled = it }
    }

    @Volatile private var isClusterManager = false

    private var alertV2IndexInitialized = false

    private var alertV2HistoryIndexInitialized = false

    private var alertV2HistoryEnabled = ALERT_V2_HISTORY_ENABLED.get(settings)

    private var scheduledAlertsV2CheckAndExpire: Scheduler.Cancellable? = null

    private val executorName = ThreadPool.Names.MANAGEMENT

    private val checkForExpirationInterval = TimeValue(1L, TimeUnit.MINUTES)

    override fun clusterChanged(event: ClusterChangedEvent) {
        if (this.isClusterManager != event.localNodeClusterManager()) {
            this.isClusterManager = event.localNodeClusterManager()
            if (this.isClusterManager) {
                onManager()
            } else {
                offManager()
            }
        }

        alertV2IndexInitialized = event.state().routingTable().hasIndex(AlertV2Indices.ALERT_V2_INDEX)
        alertV2HistoryIndexInitialized = event.state().metadata().hasAlias(AlertV2Indices.ALERT_V2_HISTORY_WRITE_INDEX)
    }

    fun onManager() {
        try {
            // try to sweep current AlertV2s for expiration immediately as we might be restarting the cluster
            moveOrDeleteAlertV2s()
            // schedule expiration checks and expirations to happen repeatedly at some interval
            scheduledAlertsV2CheckAndExpire = threadPool
                .scheduleWithFixedDelay({ moveOrDeleteAlertV2s() }, checkForExpirationInterval, executorName)
        } catch (e: Exception) {
            // This should be run on cluster startup
            logger.error(
                "Error sweeping AlertV2s for expiration. This cannot be done until clustermanager node is restarted.",
                e
            )
        }
    }

    fun offManager() {
        scheduledAlertsV2CheckAndExpire?.cancel()
    }

    // if alertV2 history is enabled, move expired alerts to alertV2 history indices
    // if alertV2 history is disabled, permanently delete expired alerts
    private fun moveOrDeleteAlertV2s() {
        if (!areAlertV2IndicesPresent()) {
            return
        }

        scope.launch {
            val expiredAlertsSearchResponse = searchForExpiredAlerts()

            var copyResponse: BulkResponse? = null
            val deleteResponse: BulkResponse?
            if (!alertV2HistoryEnabled) {
                deleteResponse = deleteExpiredAlerts(expiredAlertsSearchResponse)
            } else {
                copyResponse = copyExpiredAlerts(expiredAlertsSearchResponse)
                deleteResponse = deleteExpiredAlertsThatWereCopied(copyResponse)
            }
            checkForFailures(copyResponse)
            checkForFailures(deleteResponse)
        }
    }

    private suspend fun searchForExpiredAlerts(): SearchResponse {
        val now = Instant.now().toEpochMilli()
        val expiredAlertsQuery = QueryBuilders.rangeQuery(AlertV2.EXPIRATION_TIME_FIELD).lte(now)

        val expiredAlertsSearchQuery = SearchSourceBuilder.searchSource()
            .query(expiredAlertsQuery)
            .version(true)

        val activeAlertsRequest = SearchRequest(AlertV2Indices.ALERT_V2_INDEX)
            .source(expiredAlertsSearchQuery)
        val searchResponse: SearchResponse = client.suspendUntil { search(activeAlertsRequest, it) }
        return searchResponse
    }

    private suspend fun copyExpiredAlerts(expiredAlertsSearchResponse: SearchResponse): BulkResponse? {
        // If no alerts are found, simply return
        if (expiredAlertsSearchResponse.hits.totalHits?.value == 0L) {
            return null
        }

        val indexRequests = expiredAlertsSearchResponse.hits.map { hit ->
            IndexRequest(AlertV2Indices.ALERT_V2_HISTORY_WRITE_INDEX)
                .source(
                    AlertV2.parse(alertV2ContentParser(hit.sourceRef), hit.id, hit.version)
                        .toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS)
                )
                .version(hit.version)
                .versionType(VersionType.EXTERNAL_GTE)
                .id(hit.id)
        }

        val copyRequest = BulkRequest().add(indexRequests)
        val copyResponse: BulkResponse = client.suspendUntil { bulk(copyRequest, it) }

        return copyResponse
    }

    private suspend fun deleteExpiredAlerts(expiredAlertsSearchResponse: SearchResponse): BulkResponse {
        val deleteRequests = expiredAlertsSearchResponse.hits.map {
            DeleteRequest(AlertV2Indices.ALERT_V2_INDEX, it.id)
                .version(it.version)
                .versionType(VersionType.EXTERNAL_GTE)
        }

        val deleteRequest = BulkRequest().add(deleteRequests)
        val deleteResponse: BulkResponse = client.suspendUntil { bulk(deleteRequest, it) }

        return deleteResponse
    }

    private suspend fun deleteExpiredAlertsThatWereCopied(copyResponse: BulkResponse?): BulkResponse? {
        // if there were no expired alerts to copy, skip deleting anything
        if (copyResponse == null) {
            return null
        }

        val deleteRequests = copyResponse.items.filterNot { it.isFailed }.map {
            DeleteRequest(AlertV2Indices.ALERT_V2_INDEX, it.id)
                .version(it.version)
                .versionType(VersionType.EXTERNAL_GTE)
        }
        val deleteResponse: BulkResponse = client.suspendUntil { bulk(BulkRequest().add(deleteRequests), it) }

        return deleteResponse
    }

    private fun checkForFailures(bulkResponse: BulkResponse?) {
        bulkResponse?.let {
            if (bulkResponse.hasFailures()) {
                val retryCause = bulkResponse.items.filter { it.isFailed }
                    .firstOrNull { it.status() == RestStatus.TOO_MANY_REQUESTS }
                    ?.failure?.cause
                throw RuntimeException(
                    "Failed to move or delete alert v2s: " +
                        bulkResponse.buildFailureMessage(),
                    retryCause
                )
            }
        }
    }

    private fun alertV2ContentParser(bytesReference: BytesReference): XContentParser {
        val xcp = XContentHelper.createParser(
            NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE,
            bytesReference, XContentType.JSON
        )
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
        return xcp
    }

    private fun areAlertV2IndicesPresent(): Boolean {
        return alertV2IndexInitialized && alertV2HistoryIndexInitialized
    }
}

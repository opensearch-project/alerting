/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

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
import org.opensearch.alerting.MonitorRunnerExecutionContext
import org.opensearch.alerting.alertsv2.AlertV2Indices.Companion.ALERT_V2_HISTORY_WRITE_INDEX
import org.opensearch.alerting.alertsv2.AlertV2Indices.Companion.ALERT_V2_INDEX
import org.opensearch.alerting.modelv2.AlertV2
import org.opensearch.alerting.modelv2.MonitorV2
import org.opensearch.alerting.modelv2.MonitorV2.Companion.MONITOR_V2_TYPE
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
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.alerting.model.ScheduledJob.Companion.SCHEDULED_JOBS_INDEX
import org.opensearch.core.common.bytes.BytesReference
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentParser
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
    private val xContentRegistry: NamedXContentRegistry,
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

        alertV2IndexInitialized = event.state().routingTable().hasIndex(ALERT_V2_INDEX)
        alertV2HistoryIndexInitialized = event.state().metadata().hasAlias(ALERT_V2_HISTORY_WRITE_INDEX)
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
        logger.info("in move or delete alert v2")
        if (!areAlertV2IndicesPresent()) {
            return
        }

        scope.launch {
            val expiredAlerts = searchForExpiredAlerts()

            var copyResponse: BulkResponse? = null
            val deleteResponse: BulkResponse?
            if (!alertV2HistoryEnabled) {
                deleteResponse = deleteExpiredAlerts(expiredAlerts)
            } else {
                copyResponse = copyExpiredAlerts(expiredAlerts)
                deleteResponse = deleteExpiredAlertsThatWereCopied(copyResponse)
            }
            checkForFailures(copyResponse)
            checkForFailures(deleteResponse)
        }
    }

    private suspend fun searchForExpiredAlerts(): List<AlertV2> {
        // first collect all active alerts
        val allAlertsSearchQuery = SearchSourceBuilder.searchSource()
            .query(QueryBuilders.matchAllQuery())
            .size(10000)
            .version(true)
        val activeAlertsRequest = SearchRequest(ALERT_V2_INDEX)
            .source(allAlertsSearchQuery)
        val searchAlertsResponse: SearchResponse = client.suspendUntil { search(activeAlertsRequest, it) }

        val allAlertV2s = mutableListOf<AlertV2>()
        searchAlertsResponse.hits.forEach { hit ->
            allAlertV2s.add(
                AlertV2.parse(alertV2ContentParser(hit.sourceRef), hit.id, hit.version)
            )
        }

        // now collect all triggers and their expire durations
        val monitorV2sSearchQuery = SearchSourceBuilder.searchSource()
            .query(QueryBuilders.existsQuery(MONITOR_V2_TYPE))
            .size(10000)
            .version(true)
        val monitorV2sRequest = SearchRequest(SCHEDULED_JOBS_INDEX)
            .source(monitorV2sSearchQuery)
        val searchMonitorV2sResponse: SearchResponse = client.suspendUntil { search(monitorV2sRequest, it) }

        val triggerToExpireDuration = mutableMapOf<String, Long>()
        searchMonitorV2sResponse.hits.forEach { hit ->
            val monitorV2 = ScheduledJob.parse(scheduledJobContentParser(hit.sourceRef), hit.id, hit.version) as MonitorV2
            monitorV2.triggers.forEach { trigger ->
                triggerToExpireDuration.put(trigger.id, trigger.expireDuration)
            }
        }

        val now = Instant.now().toEpochMilli()

        // collect all alerts that are now expired
        val expiredAlerts = mutableListOf<AlertV2>()
        for (alertV2 in allAlertV2s) {
            val triggerV2Id = alertV2.triggerId
            val triggeredTime = alertV2.triggeredTime.toEpochMilli()

            val expireDuration = triggerToExpireDuration[triggerV2Id]

            // if the ID of the trigger that generated this alert can't
            // be found from the search monitor response, it means one of two things:
            // 1) the monitor was deleted
            // 2) the trigger ID (and possibly more) was edited
            // in both cases, we want to expire this alert to retain
            // only alerts that were generated by triggers
            // who currently exist and retain their config from
            // when they generated the alert
            // note: this is a redundancy with MonitorRunnerService's
            // postIndex and postDelete, which handles moving alerts in response
            // to a monitor update or delete event. this cleanly handles the case
            // that even with those measures in place, the trigger that generated
            // this alert somehow couldn't be found
            if (expireDuration == null) {
                expiredAlerts.add(alertV2)
                continue
            }

            val expireDurationMillis = expireDuration * 60 * 1000
            if (now - triggeredTime >= expireDurationMillis) {
                expiredAlerts.add(alertV2)
            }
        }

        return expiredAlerts
    }

    private suspend fun deleteExpiredAlerts(expiredAlerts: List<AlertV2>): BulkResponse? {
        // If no expired alerts are found, simply return
        if (expiredAlerts.isEmpty()) {
            return null
        }

        val deleteRequests = expiredAlerts.map {
            DeleteRequest(ALERT_V2_INDEX, it.id)
                .version(it.version)
                .versionType(VersionType.EXTERNAL_GTE)
        }

        val deleteRequest = BulkRequest().add(deleteRequests)
        val deleteResponse: BulkResponse = client.suspendUntil { bulk(deleteRequest, it) }

        return deleteResponse
    }

    private suspend fun copyExpiredAlerts(expiredAlerts: List<AlertV2>): BulkResponse? {
        // If no expired alerts are found, simply return
        if (expiredAlerts.isEmpty()) {
            return null
        }

        val indexRequests = expiredAlerts.map {
            IndexRequest(ALERT_V2_HISTORY_WRITE_INDEX)
                .source(it.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
                .version(it.version)
                .versionType(VersionType.EXTERNAL_GTE)
                .id(it.id)
        }

        val copyRequest = BulkRequest().add(indexRequests)
        val copyResponse: BulkResponse = client.suspendUntil { bulk(copyRequest, it) }

        return copyResponse
    }

    private suspend fun deleteExpiredAlertsThatWereCopied(copyResponse: BulkResponse?): BulkResponse? {
        // if there were no expired alerts to copy, skip deleting anything
        if (copyResponse == null) {
            return null
        }

        val deleteRequests = copyResponse.items.filterNot { it.isFailed }.map {
            DeleteRequest(ALERT_V2_INDEX, it.id)
                .version(it.version)
                .versionType(VersionType.EXTERNAL_GTE)
        }
        val deleteRequest = BulkRequest().add(deleteRequests)
        val deleteResponse: BulkResponse = client.suspendUntil { bulk(deleteRequest, it) }

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
        return XContentHelper.createParser(
            NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE,
            bytesReference, XContentType.JSON
        )
    }
    private fun scheduledJobContentParser(bytesReference: BytesReference): XContentParser {
        return XContentHelper.createParser(
            xContentRegistry, LoggingDeprecationHandler.INSTANCE,
            bytesReference, XContentType.JSON
        )
    }

    private fun areAlertV2IndicesPresent(): Boolean {
        return alertV2IndexInitialized && alertV2HistoryIndexInitialized
    }

    companion object {
        // this method is used by MonitorRunnerService's postIndex and postDelete
        // functions to move (in the case of alert v2 history enabled) or delete
        // (in the case of alert v2 history disabled) the alerts generated by
        // a monitor in response to the event that the monitor gets updated
        // or deleted
        suspend fun moveAlertV2s(monitorV2Id: String, monitorV2: MonitorV2?, monitorCtx: MonitorRunnerExecutionContext) {
            val client = monitorCtx.client!!

            // first collect all alerts that came from this updated or deleted monitor
            val boolQuery = QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery(AlertV2.MONITOR_V2_ID_FIELD, monitorV2Id))

            /*
             this monitorV2 != null case happens when this function is called by postIndex. if the monitor is updated,
             we don't want to expire alerts that were generated by triggers that still exist
             in the updated monitor, so filter those out. only expire alerts from triggers in
             this monitor that may no longer exist in the updated version of the monitor.
             edge case: user can edit the trigger itself while explicitly keeping the ID the same,
             which means alerts generated by that trigger will (incorrectly) not be filtered out by this logic
             even though it was edited. to mitigate this, recall that callers of the update monitor API
             must supply the full MonitorV2 object of the updated monitor config. this is important
             because it means they don't have to reference the triggers by ID when updating the triggers,
             they simply declare a whole new monitor with whatever new triggers they want it to have, and when doing this,
             likely won't explicitly pass in trigger IDs for their updated triggers that exactly match
             the IDs of the old triggers. this means Alerting will generate a new ID for the updated triggers by default,
             meaning this logic will pick up those updated triggers and correctly move/delete the alerts
            */
            if (monitorV2 != null) {
                boolQuery.mustNot(QueryBuilders.termsQuery(AlertV2.TRIGGER_V2_ID_FIELD, monitorV2.triggers.map { it.id }))
            }

            val alertsSearchQuery = SearchSourceBuilder.searchSource()
                .query(boolQuery)
                .size(10000)
                .version(true)
            val activeAlertsRequest = SearchRequest(ALERT_V2_INDEX)
                .source(alertsSearchQuery)
            val searchAlertsResponse: SearchResponse = client.suspendUntil { search(activeAlertsRequest, it) }

            // If no alerts are found, simply return
            if (searchAlertsResponse.hits.totalHits?.value == 0L) return

            val alertV2HistoryEnabled = monitorCtx.clusterService!!.clusterSettings.get(ALERT_V2_HISTORY_ENABLED)

            // if alert v2 history is enabled, migrate the relevant alerts
            // to the alert v2 history index pattern instead of hard deleting them
            var copyResponse: BulkResponse? = null
            if (alertV2HistoryEnabled) {
                val indexRequests = searchAlertsResponse.hits.map { hit ->
                    val xcp = XContentHelper.createParser(
                        NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE,
                        hit.sourceRef, XContentType.JSON
                    )

                    IndexRequest(ALERT_V2_HISTORY_WRITE_INDEX)
                        .routing(monitorV2Id)
                        .source(
                            AlertV2.parse(xcp, hit.id, hit.version)
                                .toXContentWithUser(XContentFactory.jsonBuilder())
                        )
                        .version(hit.version)
                        .versionType(VersionType.EXTERNAL_GTE)
                        .id(hit.id)
                }
                val copyRequest = BulkRequest().add(indexRequests)
                copyResponse = client.suspendUntil { bulk(copyRequest, it) }

                // retry any likely transient failures
                if (copyResponse!!.hasFailures()) {
                    val retryCause = copyResponse.items.filter { it.isFailed }
                        .firstOrNull { it.status() == RestStatus.TOO_MANY_REQUESTS }
                        ?.failure?.cause
                    throw RuntimeException(
                        "Failed to copy alertV2s for [$monitorV2Id, ${monitorV2?.triggers?.map { it.id }}]: " +
                            copyResponse.buildFailureMessage(),
                        retryCause
                    )
                }
            }

            // prepare deletion request
            val deleteRequests = if (alertV2HistoryEnabled) {
                // if alerts were to be migrated, delete only the ones
                // that were successfully copied over
                copyResponse!!.items.filterNot { it.isFailed }.map {
                    DeleteRequest(ALERT_V2_INDEX, it.id)
                        .version(it.version)
                        .versionType(VersionType.EXTERNAL_GTE)
                }
            } else {
                // otherwise just directly get the original
                // set of alerts
                searchAlertsResponse.hits.map { hit ->
                    DeleteRequest(ALERT_V2_INDEX, hit.id)
                        .version(hit.version)
                        .versionType(VersionType.EXTERNAL_GTE)
                }
            }

            // execute delete request
            val deleteRequest = BulkRequest().add(deleteRequests)
            val deleteResponse: BulkResponse = client.suspendUntil { bulk(deleteRequest, it) }

            // retry any likely transient failures
            if (deleteResponse.hasFailures()) {
                val retryCause = deleteResponse.items.filter { it.isFailed }
                    .firstOrNull { it.status() == RestStatus.TOO_MANY_REQUESTS }
                    ?.failure?.cause
                throw RuntimeException(
                    "Failed to delete alertV2s for [$monitorV2Id, ${monitorV2?.triggers?.map { it.id }}]: " +
                        deleteResponse.buildFailureMessage(),
                    retryCause
                )
            }
        }
    }
}

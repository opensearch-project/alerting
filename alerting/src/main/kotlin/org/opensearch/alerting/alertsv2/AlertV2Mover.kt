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
import org.opensearch.alerting.modelv2.AlertV2.Companion.TRIGGERED_TIME_FIELD
import org.opensearch.alerting.modelv2.AlertV2.Companion.TRIGGER_V2_ID_FIELD
import org.opensearch.alerting.modelv2.MonitorV2
import org.opensearch.alerting.modelv2.MonitorV2.Companion.MONITOR_V2_TYPE
import org.opensearch.alerting.modelv2.MonitorV2.Companion.TRIGGERS_FIELD
import org.opensearch.alerting.modelv2.PPLSQLMonitor.Companion.PPL_SQL_MONITOR_TYPE
import org.opensearch.alerting.modelv2.TriggerV2.Companion.EXPIRE_FIELD
import org.opensearch.alerting.modelv2.TriggerV2.Companion.ID_FIELD
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.settings.AlertingSettings.Companion.ALERT_V2_HISTORY_ENABLED
import org.opensearch.alerting.util.MAX_SEARCH_SIZE
import org.opensearch.client.Client
import org.opensearch.cluster.ClusterChangedEvent
import org.opensearch.cluster.ClusterStateListener
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
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
import java.time.Instant
import java.util.concurrent.TimeUnit

private val scope: CoroutineScope = CoroutineScope(Dispatchers.IO)
private val logger = LogManager.getLogger(AlertV2Mover::class.java)

/**
 * This class handles sweeping the active v2 alerts index for expired alerts, and
 * either moving them to v2 alerts history index (if alert v2 history enabled) or
 * permanently deleting them (if alert v2 history disabled). It also contains the
 * logic for moving alerts in response to a monitor update or deletion.
 *
 * @opensearch.experimental
 */
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
                deleteResponse = deleteExpiredAlertsThatWereCopied(copyResponse, expiredAlerts)
            }
            checkForFailures(copyResponse)
            checkForFailures(deleteResponse)
        }
    }

    private suspend fun searchForExpiredAlerts(): List<AlertV2> {
        logger.debug("beginning search for expired alerts")
        /* first collect all triggers and their expire durations */
        // when searching the alerting-config index, only trigger IDs and their expire durations are needed
        val monitorV2sSearchQuery = SearchSourceBuilder.searchSource()
            .query(QueryBuilders.existsQuery(MONITOR_V2_TYPE))
            .fetchSource(
                arrayOf(
                    "$MONITOR_V2_TYPE.$PPL_SQL_MONITOR_TYPE.$TRIGGERS_FIELD.$ID_FIELD",
                    "$MONITOR_V2_TYPE.$PPL_SQL_MONITOR_TYPE.$TRIGGERS_FIELD.$EXPIRE_FIELD"
                ),
                null
            )
            .size(MAX_SEARCH_SIZE)
            .version(true)
        val monitorV2sRequest = SearchRequest(SCHEDULED_JOBS_INDEX)
            .source(monitorV2sSearchQuery)
        val searchMonitorV2sResponse: SearchResponse = client.suspendUntil { search(monitorV2sRequest, it) }

        logger.debug("searching triggers for their expire durations")
        // construct a map that stores each trigger's expiration time
        // TODO: create XContent parser specifically for responses to the above search to avoid casting
        val triggerToExpireDuration = mutableMapOf<String, Long>()
        searchMonitorV2sResponse.hits.forEach { hit ->
            val monitorV2Obj = hit.sourceAsMap[MONITOR_V2_TYPE] as Map<String, Any>
            val pplMonitorObj = monitorV2Obj[PPL_SQL_MONITOR_TYPE] as Map<String, Any>
            val triggers = pplMonitorObj[TRIGGERS_FIELD] as List<Map<String, Any>>
            for (trigger in triggers) {
                val triggerId = trigger[ID_FIELD] as String
                val expireDuration = (trigger[EXPIRE_FIELD] as Int).toLong()
                logger.debug("triggerId: $triggerId")
                logger.debug("triggerExpires: $expireDuration")
                triggerToExpireDuration[triggerId] = expireDuration
            }
        }

        logger.debug("trigger to expire duration map: $triggerToExpireDuration")

        /* now collect all expired alerts */
        logger.debug("searching active alerts index for expired alerts")

        val now = Instant.now().toEpochMilli()

        val expiredAlertsBoolQuery = QueryBuilders.boolQuery()

        // collect, in an overarching should clause, each trigger and its expiration time.
        // any alert that matches both the trigger ID and the expiration time check should
        // be returned by the search query
        triggerToExpireDuration.forEach { (triggerId, expireDuration) ->
            val expireDurationMillis = expireDuration * 60 * 1000
            val maxValidTime = now - expireDurationMillis

            expiredAlertsBoolQuery.should(
                QueryBuilders.boolQuery()
                    .must(QueryBuilders.termQuery(TRIGGER_V2_ID_FIELD, triggerId))
                    .must(QueryBuilders.rangeQuery(TRIGGERED_TIME_FIELD).lte(maxValidTime))
            )
        }

        // add orphaned alerts to should clause as well (i.e. alerts whose trigger IDs cannot
        // be found in the list of currently existent triggers), since orphaned alerts should be expired.
        // note: this is a redundancy with MonitorRunnerService's
        // postIndex and postDelete, which handles moving alerts in response
        // to a monitor update or delete event. this cleanly handles the case
        // that even with those measures in place, an alert that came from a
        // now nonexistent trigger was somehow found
        expiredAlertsBoolQuery.should(
            QueryBuilders.boolQuery()
                .mustNot(QueryBuilders.termsQuery(TRIGGER_V2_ID_FIELD, triggerToExpireDuration.keys.toList()))
        )

        // Explicitly specify that at least one should clause must match
        expiredAlertsBoolQuery.minimumShouldMatch(1)

        // search for the expired alerts
        val expiredAlertsSearchQuery = SearchSourceBuilder.searchSource()
            .query(expiredAlertsBoolQuery)
            .size(MAX_SEARCH_SIZE)
            .version(true)
        val expiredAlertsRequest = SearchRequest(ALERT_V2_INDEX)
            .source(expiredAlertsSearchQuery)
        val expiredAlertsResponse: SearchResponse = client.suspendUntil { search(expiredAlertsRequest, it) }

        // parse the search results into full alert docs, as they will need to be
        // indexed into alert history indices
        val expiredAlertV2s = mutableListOf<AlertV2>()
        expiredAlertsResponse.hits.forEach { hit ->
            expiredAlertV2s.add(
                AlertV2.parse(alertV2ContentParser(hit.sourceRef), hit.id, hit.version)
            )
        }

        logger.debug("expired alerts: $expiredAlertV2s")

        return expiredAlertV2s
    }

    private suspend fun deleteExpiredAlerts(expiredAlerts: List<AlertV2>): BulkResponse? {
        logger.debug("beginning to hard delete expired alerts permanently")
        // If no expired alerts are found, simply return
        if (expiredAlerts.isEmpty()) {
            return null
        }

        val deleteRequests = expiredAlerts.map {
            DeleteRequest(ALERT_V2_INDEX, it.id)
                .routing(it.monitorId)
                .version(it.version)
                .versionType(VersionType.EXTERNAL_GTE)
        }

        val deleteRequest = BulkRequest().add(deleteRequests)
        val deleteResponse: BulkResponse = client.suspendUntil { bulk(deleteRequest, it) }

        return deleteResponse
    }

    private suspend fun copyExpiredAlerts(expiredAlerts: List<AlertV2>): BulkResponse? {
        logger.debug("beginning to copy expired alerts to history write index")
        // If no expired alerts are found, simply return
        if (expiredAlerts.isEmpty()) {
            return null
        }

        val indexRequests = expiredAlerts.map {
            IndexRequest(ALERT_V2_HISTORY_WRITE_INDEX)
                .routing(it.monitorId)
                .source(it.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
                .version(it.version)
                .versionType(VersionType.EXTERNAL_GTE)
                .id(it.id)
        }

        val copyRequest = BulkRequest().add(indexRequests)
        val copyResponse: BulkResponse = client.suspendUntil { bulk(copyRequest, it) }

        return copyResponse
    }

    private suspend fun deleteExpiredAlertsThatWereCopied(copyResponse: BulkResponse?, expiredAlerts: List<AlertV2>): BulkResponse? {
        logger.debug("beginning to delete expired alerts that were copied to history write index")
        // if there were no expired alerts to copy, skip deleting anything
        if (copyResponse == null) {
            return null
        }

        // pre-index the alerts so retrieving their
        // monitor IDs for routing is easier
        val alertsById: Map<String, AlertV2> = expiredAlerts.associateBy { it.id }

        val deleteRequests = copyResponse.items.filterNot { it.isFailed }.map {
            DeleteRequest(ALERT_V2_INDEX, it.id)
                .routing(alertsById[it.id]!!.monitorId)
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
                logger.error(
                    "Failed to move or delete alert v2s: ${bulkResponse.buildFailureMessage()}",
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
            logger.debug("beginning to move alerts for postIndex or postDelete of monitor: $monitorV2Id")
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
                boolQuery.mustNot(QueryBuilders.termsQuery(TRIGGER_V2_ID_FIELD, monitorV2.triggers.map { it.id }))
            }

            val alertsSearchQuery = SearchSourceBuilder.searchSource()
                .query(boolQuery)
                .size(MAX_SEARCH_SIZE)
                .version(true)
            val activeAlertsRequest = SearchRequest(ALERT_V2_INDEX)
                .source(alertsSearchQuery)
            val searchAlertsResponse: SearchResponse = client.suspendUntil { search(activeAlertsRequest, it) }

            // If no alerts are found, simply return
            if (searchAlertsResponse.hits.totalHits?.value == 0L) return

            val activeAlerts = mutableListOf<AlertV2>()
            searchAlertsResponse.hits.forEach { hit ->
                activeAlerts.add(
                    AlertV2.parse(
                        XContentHelper.createParser(
                            NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE,
                            hit.sourceRef, XContentType.JSON
                        ),
                        hit.id,
                        hit.version
                    )
                )
            }

            // pre-index the alerts so retrieving their
            // monitor IDs for routing is easier
            val alertsById: Map<String, AlertV2> = activeAlerts.associateBy { it.id }

            val alertV2HistoryEnabled = monitorCtx.clusterService!!.clusterSettings.get(ALERT_V2_HISTORY_ENABLED)

            // if alert v2 history is enabled, migrate the relevant alerts
            // to the alert v2 history index pattern instead of hard deleting them
            var copyResponse: BulkResponse? = null
            if (alertV2HistoryEnabled) {
                logger.debug("alert v2 history enabled, copying alerts to history write index")
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

            logger.debug("deleting alerts related to monitor: $monitorV2Id")

            // prepare deletion request
            val deleteRequests = if (alertV2HistoryEnabled) {
                // if alerts were to be migrated, delete only the ones
                // that were successfully copied over
                copyResponse!!.items.filterNot { it.isFailed }.map {
                    DeleteRequest(ALERT_V2_INDEX, it.id)
                        .routing(alertsById[it.id]!!.monitorId)
                        .version(it.version)
                        .versionType(VersionType.EXTERNAL_GTE)
                }
            } else {
                // otherwise just directly get the original
                // set of alerts
                searchAlertsResponse.hits.map { hit ->
                    DeleteRequest(ALERT_V2_INDEX, hit.id)
                        .routing(alertsById[hit.id]!!.monitorId)
                        .version(hit.version)
                        .versionType(VersionType.EXTERNAL_GTE)
                }
            }

            // execute delete request
            val deleteRequest = BulkRequest().add(deleteRequests)
            val deleteResponse: BulkResponse = client.suspendUntil { bulk(deleteRequest, it) }

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

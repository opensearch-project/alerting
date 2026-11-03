/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util.destinationmigration

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.alerting.core.ScheduledJobIndices
import org.opensearch.cluster.ClusterChangedEvent
import org.opensearch.cluster.ClusterStateListener
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.lifecycle.LifecycleListener
import org.opensearch.common.unit.TimeValue
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.threadpool.Scheduler
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.client.Client
import org.opensearch.transport.client.node.NodeClient
import kotlin.coroutines.CoroutineContext

class DestinationMigrationCoordinator(
    private val client: Client,
    private val clusterService: ClusterService,
    private val threadPool: ThreadPool,
    private val scheduledJobIndices: ScheduledJobIndices
) : ClusterStateListener, CoroutineScope, LifecycleListener() {

    private val logger = LogManager.getLogger(javaClass)

    override val coroutineContext: CoroutineContext
        get() = Dispatchers.Default + CoroutineName("DestinationMigrationCoordinator")

    private var scheduledMigration: Scheduler.Cancellable? = null

    @Volatile
    private var runningLock = false

    init {
        clusterService.addListener(this)
        clusterService.addLifecycleListener(this)
    }

    override fun clusterChanged(event: ClusterChangedEvent) {
        // Cancellation on demotion must always run regardless of index changes —
        // a node losing cluster-manager status is a metadata event, not a routing
        // table change, so it must be handled before the routing-table guard.
        if (!event.localNodeClusterManager()) {
            if (scheduledMigration != null && !scheduledMigration!!.isCancelled) {
                logger.info("Cancelling the migration process.")
                scheduledMigration?.cancel()
            }
            return
        }

        // Proceed with migration initiation when ANY of:
        //   (a) the scheduled-jobs index routing table changed, OR
        //   (b) the index does not exist yet (bootstrap / initial creation path) — when the index
        //       is absent this guard passes on every cluster event until the index appears; the
        //       inner idempotency checks in initMigrateDestinations() prevent double-scheduling, OR
        //   (c) this node just became cluster manager (promotion on an already-created index;
        //       promotion fires a nodes-change event, not an index-routing change).
        // Skip all other cluster events to avoid unnecessary work.
        // Note: event.localNodeClusterManager() == true is already proven by the demotion guard
        // above, so the second half of becameClusterManager is omitted — it is always true here.
        val indexExists = event.state().routingTable().hasIndex(ScheduledJob.SCHEDULED_JOBS_INDEX)
        val indexChanged = event.indexRoutingTableChanged(ScheduledJob.SCHEDULED_JOBS_INDEX)
        val becameClusterManager = !event.previousState().nodes().isLocalNodeElectedClusterManager
        if (indexExists && !indexChanged && !becameClusterManager) return

        if (DestinationMigrationUtilService.finishFlag) {
            logger.info("Reset destination migration process.")
            scheduledMigration?.cancel()
            DestinationMigrationUtilService.finishFlag = false
        }
        if (!runningLock && (scheduledMigration == null || scheduledMigration!!.isCancelled)) {
            try {
                runningLock = true
                initMigrateDestinations()
            } finally {
                runningLock = false
            }
        }
    }

    private fun initMigrateDestinations() {
        if (!scheduledJobIndices.scheduledJobIndexExists()) {
            logger.debug("Alerting config index is not initialized")
            scheduledMigration?.cancel()
            return
        }

        if (!clusterService.state().nodes().isLocalNodeElectedClusterManager) {
            scheduledMigration?.cancel()
            return
        }

        if (DestinationMigrationUtilService.finishFlag) {
            logger.info("Destination migration is already complete, cancelling migration process.")
            scheduledMigration?.cancel()
            return
        }

        val scheduledJob = Runnable {
            launch {
                try {
                    if (DestinationMigrationUtilService.finishFlag) {
                        logger.info("Cancel background destination migration process.")
                        scheduledMigration?.cancel()
                    }

                    logger.info("Performing migration of destination data.")
                    DestinationMigrationUtilService.migrateDestinations(client as NodeClient)
                } catch (e: Exception) {
                    logger.error("Failed to migrate destination data", e)
                }
            }
        }

        scheduledMigration = threadPool.scheduleWithFixedDelay(scheduledJob, TimeValue.timeValueMinutes(1), ThreadPool.Names.MANAGEMENT)
    }
}

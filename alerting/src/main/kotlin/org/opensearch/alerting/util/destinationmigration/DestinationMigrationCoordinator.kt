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
import org.opensearch.client.Client
import org.opensearch.client.node.NodeClient
import org.opensearch.cluster.ClusterChangedEvent
import org.opensearch.cluster.ClusterStateListener
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.component.LifecycleListener
import org.opensearch.common.unit.TimeValue
import org.opensearch.threadpool.Scheduler
import org.opensearch.threadpool.ThreadPool
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
        logger.info("Detected cluster change event for destination migration")
        if (DestinationMigrationUtilService.finishFlag) {
            logger.info("Reset destination migration process.")
            scheduledMigration?.cancel()
            DestinationMigrationUtilService.finishFlag = false
        }
        if (
            event.localNodeClusterManager() &&
            !runningLock &&
            (scheduledMigration == null || scheduledMigration!!.isCancelled)
        ) {
            try {
                runningLock = true
                initMigrateDestinations()
            } finally {
                runningLock = false
            }
        } else if (!event.localNodeClusterManager()) {
            scheduledMigration?.cancel()
        }
    }

    private fun initMigrateDestinations() {
        if (!scheduledJobIndices.scheduledJobIndexExists()) {
            logger.debug("Alerting config index is not initialized")
            scheduledMigration?.cancel()
            return
        }

        if (!clusterService.state().nodes().isLocalNodeElectedMaster) {
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

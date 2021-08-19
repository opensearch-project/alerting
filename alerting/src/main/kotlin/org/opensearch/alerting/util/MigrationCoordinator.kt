package org.opensearch.alerting.util

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
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

class MigrationCoordinator(
    private val client: Client,
    private val clusterService: ClusterService,
    private val threadPool: ThreadPool
) : ClusterStateListener, CoroutineScope, LifecycleListener() {

    private val logger = LogManager.getLogger(javaClass)

    override val coroutineContext: CoroutineContext
        get() = Dispatchers.Default + CoroutineName("MigrationCoordinator")

    private var scheduledMigration: Scheduler.Cancellable? = null

    @Volatile
    private var runningLock = false

    init {
        clusterService.addListener(this)
        clusterService.addLifecycleListener(this)
    }

    override fun clusterChanged(event: ClusterChangedEvent) {
        logger.info("Detected cluster change event for migration")
        if (MigrationUtilService.finishFlag) {
            logger.info("Reset migration process.")
            scheduledMigration?.cancel()
            MigrationUtilService.finishFlag = false
        }
        if (
            event.localNodeMaster() &&
            !runningLock &&
            (scheduledMigration == null || scheduledMigration!!.isCancelled)
        ) {
            try {
                runningLock = true
                initMigrateDestinations()
            } finally {
                runningLock = false
            }
        } else if (!event.localNodeMaster()) {
            scheduledMigration?.cancel()
        }
    }

    private fun initMigrateDestinations() {
        if (!clusterService.state().nodes().isLocalNodeElectedMaster) {
            scheduledMigration?.cancel()
            return
        }

        if (MigrationUtilService.finishFlag) {
            logger.info("Migration is already complete, cancel migration process.")
            scheduledMigration?.cancel()
            return
        }

        val scheduledJob = Runnable {
            launch {
                try {
                    if (MigrationUtilService.finishFlag) {
                        logger.info("Cancel background migration process.")
                        scheduledMigration?.cancel()
                    }

                    logger.info("Performing migration of destination data.")
                    MigrationUtilService.migrateDestinations(client as NodeClient)
                } catch (e: Exception) {
                    logger.error("Failed to migrate destination data", e)
                }
            }
        }

        scheduledMigration = threadPool.scheduleWithFixedDelay(scheduledJob, TimeValue.timeValueMinutes(1), ThreadPool.Names.MANAGEMENT)
    }
}

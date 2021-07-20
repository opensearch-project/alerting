package org.opensearch.alerting.util

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.action.ActionListener
import org.opensearch.action.admin.cluster.node.info.NodesInfoAction
import org.opensearch.action.admin.cluster.node.info.NodesInfoRequest
import org.opensearch.action.admin.cluster.node.info.NodesInfoResponse
import org.opensearch.action.admin.cluster.node.info.PluginsAndModules
import org.opensearch.client.Client
import org.opensearch.client.node.NodeClient
import org.opensearch.cluster.ClusterChangedEvent
import org.opensearch.cluster.ClusterStateListener
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.component.LifecycleListener
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.threadpool.Scheduler
import org.opensearch.threadpool.ThreadPool
import kotlin.coroutines.CoroutineContext

class MigrationCoordinator(
    private val client: Client,
    private val clusterService: ClusterService,
    private val threadPool: ThreadPool,
//    private val migrationService: MigrationUtilService
) : ClusterStateListener, CoroutineScope, LifecycleListener() {

    private val logger = LogManager.getLogger(javaClass)

    override val coroutineContext: CoroutineContext
        get() = Dispatchers.Default + CoroutineName("MigrationCoordinator")

    private var scheduledFullSweep: Scheduler.Cancellable? = null
    private var scheduledMigration: Scheduler.Cancellable? = null


    @Volatile
    private var runningLock = false


    @Volatile private var isMaster = false

    @Volatile final var flag: Boolean = false
        private set
    // To track if there are any legacy Alerting plugin nodes part of the cluster
    @Volatile final var hasLegacyPlugin: Boolean = false
        private set

    init {
        clusterService.addListener(this)
        clusterService.addLifecycleListener(this)
    }

    override fun clusterChanged(event: ClusterChangedEvent) {
        logger.info("Detected cluster change event for migration")
        if (event.localNodeMaster() && !MigrationUtilService.finishFlag && !runningLock && (scheduledMigration == null || scheduledMigration!!.isCancelled)) { //&& (event.nodesChanged() || event.isNewCluster) && sweepPluginVersion()) {
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

    fun sweepPluginVersion(): Boolean {
        val request = NodesInfoRequest().clear().addMetric("plugins")
        var executeMigration = false
        client.execute(
            NodesInfoAction.INSTANCE, request,
            object : ActionListener<NodesInfoResponse> {
                override fun onResponse(response: NodesInfoResponse) {
//                    val versionSet = mutableSetOf<String>()
//                    val legacyVersionSet = mutableSetOf<String>()

                    response.nodes.map { it.getInfo(PluginsAndModules::class.java).pluginInfos }
                        .forEach {
                            it.forEach { nodePlugin ->
                                if (nodePlugin.name == "opensearch-alerting" ||
                                    nodePlugin.name == "opensearch_alerting"
                                ) {
                                    logger.info("This is the OS plugin version: ${nodePlugin.version}")
                                    if (nodePlugin.version == "1.0.0.0") executeMigration = true
//                                    versionSet.add(nodePlugin.version)
                                }

                                if (nodePlugin.name == "opendistro-alerting" ||
                                    nodePlugin.name == "opendistro_alerting"
                                ) {
                                    logger.info("This is the odfe plugin version: ${nodePlugin.version}")
                                    executeMigration = true
//                                    legacyVersionSet.add(nodePlugin.version)
                                }
                            }
                        }
                }

                override fun onFailure(e: Exception) {
                    logger.error("Failed sweeping nodes for Alerting plugin versions: $e")
//                    flag = false
                }
            }
        )
        return executeMigration
    }

    private fun initMigrateDestinations() {
        if (!clusterService.state().nodes().isLocalNodeElectedMaster) {
            scheduledMigration?.cancel()
            return
        }

        if (MigrationUtilService.finishFlag) {
            logger.info("Re-enable Migration Service.")
//            migrationService.reenableMigrationService()
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

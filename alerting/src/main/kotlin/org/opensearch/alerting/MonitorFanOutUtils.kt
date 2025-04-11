package org.opensearch.alerting

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.opensearch.Version
import org.opensearch.cluster.node.DiscoveryNode

private val logger: Logger = LogManager.getLogger("FanOutEligibility")

fun isNodeEligibleForFanOut(
    candidateNode: DiscoveryNode,
    coordinatorNode: DiscoveryNode,
): Boolean {
    try {
        val candidateNodeAttributes: Map<String, Any> = candidateNode.attributes
        val coordinatorNodeAttributes: Map<String, Any> = coordinatorNode.attributes
        if (candidateNodeAttributes.containsKey("di_number") && candidateNodeAttributes["di_number"] != null &&
            coordinatorNodeAttributes.containsKey("di_number") && candidateNodeAttributes["di_number"] != null
        ) {
            return candidateNodeAttributes["di_number"] as Int >= coordinatorNodeAttributes["di_number"] as Int
        }
    } catch (e: Exception) {
        logger.error("Error in isNodeEligibleForFanOut criteria evaluation", e)
        return true
    }
    return true
}

fun getNodes(monitorCtx: MonitorRunnerExecutionContext): Map<String, DiscoveryNode> {
    val clusterService = monitorCtx.clusterService!!
    return clusterService.state().nodes.dataNodes.filter {
        it.value.version >= Version.CURRENT &&
            isNodeEligibleForFanOut(it.value, clusterService.localNode())
    }
}

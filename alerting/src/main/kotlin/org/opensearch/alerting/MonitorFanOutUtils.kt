/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.alerting

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.opensearch.core.index.Index
import org.opensearch.core.index.shard.ShardId
import org.opensearch.index.seqno.SequenceNumbers

private val logger: Logger = LogManager.getLogger("FanOutEligibility")

fun distributeShards(
    maxFanoutNodes: Int,
    allNodes: List<String>,
    shards: List<String>,
    index: Index,
): Map<String, MutableSet<ShardId>> {
    val totalShards = shards.size
    val numFanOutNodes = allNodes.size.coerceAtMost(totalShards)
    val totalNodes = maxFanoutNodes.coerceAtMost(numFanOutNodes)

    val shardIdList =
        shards.map {
            ShardId(index, it.toInt())
        }
    val shuffledNodes = allNodes.shuffled()
    val nodes = shuffledNodes.subList(0, totalNodes)

    val nodeShardAssignments = nodes.associateWith { mutableSetOf<ShardId>() }

    if (nodeShardAssignments.isEmpty()) {
        logger.error("No nodes eligible for fanout")
        return nodeShardAssignments
    }

    shardIdList.forEachIndexed { idx, shardId ->
        val nodeIdx = idx % nodes.size
        val node = nodes[nodeIdx]
        nodeShardAssignments[node]!!.add(shardId)
    }

    return nodeShardAssignments
}

/**
 * Initializes the last run context for a given index.
 *
 * This method prepares the context structure to be updated later by fan-out operations.
 * It preserves existing sequence numbers and initializes new shards with UNASSIGNED_SEQ_NO.
 *
 * @param lastRunContext The previous run context from monitor metadata
 * @param monitorCtx The execution context containing cluster service
 * @param index The name of the index for which the context is being initialized
 * @return A map containing the initialized last run context
 */
fun initializeNewLastRunContext(
    lastRunContext: Map<String, Any>,
    index: String,
    shardCount: Int,
): Map<String, Any> {
    val updatedLastRunContext = lastRunContext.toMutableMap()

    // Only initialize shards that don't already have a sequence number
    for (i: Int in 0 until shardCount) {
        val shard = i.toString()
        // Preserve existing sequence numbers instead of resetting to UNASSIGNED
        if (!updatedLastRunContext.containsKey(shard)) {
            updatedLastRunContext[shard] = SequenceNumbers.UNASSIGNED_SEQ_NO
        }
    }

    // Metadata fields
    updatedLastRunContext["shards_count"] = shardCount
    updatedLastRunContext["index"] = index

    return updatedLastRunContext
}

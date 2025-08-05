/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.alerting

import org.opensearch.core.index.Index
import org.opensearch.core.index.shard.ShardId
import org.opensearch.test.OpenSearchTestCase

class MonitorFanOutUtilsTests : OpenSearchTestCase() {
    fun `test distribute few shards many nodes`() {
        val result = distributeShards(
            1000,
            listOf("nodeA", "nodeB", "nodeC", "nodeD", "nodeE"),
            listOf("0", "1"),
            Index("index1", "id1")
        )

        validateDistribution(result, 2, listOf(1), 2)
    }

    fun `test distribute randomizes the assigned node`() {
        val nodes = mutableSetOf<String>()

        // Picking a node to distribute to is random. To reduce test flakiness, we run this 100 times to give a (1/5)^99 chance
        // that the same node is picked every time
        repeat(100) {
            val result = distributeShards(
                1000,
                listOf("nodeA", "nodeB", "nodeC", "nodeD", "nodeE"),
                listOf("0"),
                Index("index1", "id1")
            )

            validateDistribution(result, 1, listOf(1), 1)
            nodes.addAll(result.keys)
        }

        assertTrue(nodes.size > 1)
    }

    fun `test distribute many shards few nodes`() {
        val result = distributeShards(
            1000,
            listOf("nodeA", "nodeB", "nodeC"),
            listOf("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"),
            Index("index1", "id1")
        )

        validateDistribution(result, 3, listOf(3, 4), 10)
    }

    fun `test distribute max nodes limits`() {
        val result = distributeShards(
            2,
            listOf("nodeA", "nodeB", "nodeC"),
            listOf("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"),
            Index("index1", "id1")
        )

        validateDistribution(result, 2, listOf(5), 10)
    }

    fun `test distribute edge case 1 shard`() {
        val result = distributeShards(
            1000,
            listOf("nodeA", "nodeB", "nodeC"),
            listOf("0"),
            Index("index1", "id1")
        )

        validateDistribution(result, 1, listOf(1), 1)
    }

    fun `test distribute edge case 1 node`() {
        val result = distributeShards(
            1000,
            listOf("nodeA"),
            listOf("0", "1", "2"),
            Index("index1", "id1")
        )

        validateDistribution(result, 1, listOf(3), 3)
    }

    fun `test distribute edge case 1 shard 1 node`() {
        val result = distributeShards(
            1000,
            listOf("nodeA"),
            listOf("0"),
            Index("index1", "id1")
        )

        validateDistribution(result, 1, listOf(1), 1)
    }

    fun `test distribute edge case no nodes does not throw`() {
        val result = distributeShards(
            1000,
            listOf(),
            listOf("0"),
            Index("index1", "id1")
        )

        validateDistribution(result, 0, listOf(), 0)
    }

    fun `test distribute edge case no shards does not throw`() {
        val result = distributeShards(
            1000,
            listOf("nodeA"),
            listOf(),
            Index("index1", "id1")
        )

        validateDistribution(result, 0, listOf(), 0)
    }

    private fun validateDistribution(
        result: Map<String, MutableSet<ShardId>>,
        expectedNodeCount: Int,
        expectedShardsPerNode: List<Int>,
        expectedTotalShardCount: Int
    ) {
        assertEquals(expectedNodeCount, result.keys.size)
        var shardCount = 0
        result.forEach { (_, shards) ->
            assertTrue(expectedShardsPerNode.contains(shards.size))
            shardCount += shards.size
        }
        assertEquals(expectedTotalShardCount, shardCount)
    }
}

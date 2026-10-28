/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.alerting

import org.opensearch.core.index.Index
import org.opensearch.core.index.shard.ShardId
import org.opensearch.index.seqno.SequenceNumbers
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

    fun `test initializeNewLastRunContext returns mutable map`() {
        val result = initializeNewLastRunContext(emptyMap(), "test-index", 3)

        assertTrue("result must be a MutableMap", result is MutableMap)
        assertTrue("result must contain shard key '0'", result.containsKey("0"))
        assertTrue("result must contain shard key '1'", result.containsKey("1"))
        assertTrue("result must contain shard key '2'", result.containsKey("2"))
        assertEquals("shards_count must equal 3", 3, result["shards_count"])
        assertEquals("index must equal 'test-index'", "test-index", result["index"])

        // Verify it is truly mutable — must not throw UnsupportedOperationException
        result["extra_key"] = "extra_value"
        assertEquals("extra_value", result["extra_key"])
    }

    fun `test initializeNewLastRunContext preserves existing seq numbers`() {
        val existingContext = mapOf<String, Any>("0" to 42L)

        val result = initializeNewLastRunContext(existingContext, "preserve-index", 2)

        assertEquals("Existing sequence number for shard 0 must be preserved", 42L, result["0"])
        assertEquals(
            "New shard 1 must be initialized to UNASSIGNED_SEQ_NO",
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            result["1"]
        )
        assertEquals(2, result["shards_count"])
    }

    fun `test initializeNewLastRunContext with Collections emptyMap does not throw`() {
        val emptyJavaMap = java.util.Collections.emptyMap<String, Any>()

        val result = initializeNewLastRunContext(emptyJavaMap, "empty-map-index", 2)

        assertNotNull("result must not be null", result)
        assertTrue("result must be mutable", result is MutableMap)
        // Verify mutability by inserting a new entry
        result["probe"] = "ok"
        assertEquals("ok", result["probe"])
    }
}

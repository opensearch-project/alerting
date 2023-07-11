/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util.clusterMetricsMonitorHelpers

import org.opensearch.action.support.WriteRequest
import org.opensearch.alerting.randomClusterMetricsInput
import org.opensearch.alerting.util.clusterMetricsMonitorHelpers.CatShardsResponseWrapper.Companion.WRAPPER_FIELD
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.model.ClusterMetricsInput
import org.opensearch.core.common.Strings
import org.opensearch.test.OpenSearchSingleNodeTestCase

class CatShardsWrappersIT : OpenSearchSingleNodeTestCase() {
    private val path = ClusterMetricsInput.ClusterMetricType.CAT_SHARDS.defaultPath

    fun `test CatShardsRequestWrapper validate valid pathParams`() {
        // GIVEN
        val pathParams = "index1,index_2,index-3"

        // WHEN
        val requestWrapper = CatShardsRequestWrapper(pathParams = pathParams)

        // THEN
        assertEquals(3, requestWrapper.clusterStateRequest.indices().size)
        assertEquals(3, requestWrapper.indicesStatsRequest.indices().size)
    }

    fun `test CatShardsRequestWrapper validate without providing pathParams`() {
        // GIVEN & WHEN
        val requestWrapper = CatShardsRequestWrapper()

        // THEN
        assertEquals(Strings.EMPTY_ARRAY, requestWrapper.clusterStateRequest.indices())
        assertNull(requestWrapper.indicesStatsRequest.indices())
    }

    fun `test CatShardsRequestWrapper validate blank pathParams`() {
        // GIVEN
        val pathParams = "    "

        // WHEN
        val requestWrapper = CatShardsRequestWrapper(pathParams = pathParams)

        // THEN
        assertEquals(Strings.EMPTY_ARRAY, requestWrapper.clusterStateRequest.indices())
        assertNull(requestWrapper.indicesStatsRequest.indices())
    }

    fun `test CatShardsRequestWrapper validate empty pathParams`() {
        // GIVEN
        val pathParams = ""

        // WHEN
        val requestWrapper = CatShardsRequestWrapper(pathParams = pathParams)

        // THEN
        assertEquals(Strings.EMPTY_ARRAY, requestWrapper.clusterStateRequest.indices())
        assertNull(requestWrapper.indicesStatsRequest.indices())
    }

    fun `test CatShardsRequestWrapper validate invalid pathParams`() {
        // GIVEN
        val pathParams = "_index1,index^2"

        // WHEN & THEN
        assertThrows(IllegalArgumentException::class.java) { CatShardsRequestWrapper(pathParams = pathParams) }
    }

    suspend fun `test CatShardsResponseWrapper returns with only indices in pathParams`() {
        // GIVEN
        val testIndices = (1..5).map {
            "test-index${randomAlphaOfLength(10).lowercase()}" to randomIntBetween(1, 10)
        }.toMap()

        testIndices.forEach { (indexName, docCount) ->
            repeat(docCount) {
                val docId = (it + 1).toString()
                val docMessage = """
                    { 
                        "message": "$indexName doc num $docId" 
                    }
                """.trimIndent()
                indexDoc(indexName, docId, docMessage)
            }
        }

        /*
        Creating a subset of indices to use for the pathParams to test that all indices on the cluster ARE NOT returned.
        */
        val pathParamsIndices = testIndices.keys.toList().subList(1, testIndices.size - 1)
        val pathParams = pathParamsIndices.joinToString(",")
        val input = randomClusterMetricsInput(path = path, pathParams = pathParams)

        // WHEN
        val responseMap = (executeTransportAction(input, client())).toMap()

        // THEN
        val shards = responseMap[WRAPPER_FIELD] as List<HashMap<String, String>>
        val returnedIndices =
            shards.map { (it[CatShardsResponseWrapper.ShardInfo.INDEX_FIELD] as String) to it }.toMap()

        assertEquals(pathParamsIndices.size, returnedIndices.keys.size)
        testIndices.forEach { (indexName, docCount) ->
            if (pathParamsIndices.contains(indexName)) {
                assertEquals(
                    indexName,
                    returnedIndices[indexName]?.get(CatShardsResponseWrapper.ShardInfo.INDEX_FIELD) as String
                )
                assertEquals(
                    docCount.toString(),
                    returnedIndices[indexName]?.get(CatShardsResponseWrapper.ShardInfo.DOCS_FIELD) as String
                )
            }
        }
    }

    suspend fun `test CatShardsResponseWrapper returns with all indices when empty pathParams`() {
        // GIVEN
        val testIndices = (1..5).map {
            "test-index${randomAlphaOfLength(10).lowercase()}" to randomIntBetween(1, 10)
        }.toMap()

        testIndices.forEach { (indexName, docCount) ->
            repeat(docCount) {
                val docId = (it + 1).toString()
                val docMessage = """
                    { 
                        "message": "$indexName doc num $docId" 
                    }
                """.trimIndent()
                indexDoc(indexName, docId, docMessage)
            }
        }

        val input = randomClusterMetricsInput(path = path)

        // WHEN
        val responseMap = (executeTransportAction(input, client())).toMap()

        // THEN
        val shards = responseMap[WRAPPER_FIELD] as List<HashMap<String, String>>
        val returnedIndices =
            shards.map { (it[CatShardsResponseWrapper.ShardInfo.INDEX_FIELD] as String) to it }.toMap()

        assertEquals(testIndices.size, returnedIndices.keys.size)
        testIndices.forEach { (indexName, docCount) ->
            assertEquals(
                indexName,
                returnedIndices[indexName]?.get(CatShardsResponseWrapper.ShardInfo.INDEX_FIELD) as String
            )
            assertEquals(
                docCount.toString(),
                returnedIndices[indexName]?.get(CatShardsResponseWrapper.ShardInfo.DOCS_FIELD) as String
            )
        }
    }

    private fun indexDoc(index: String, id: String, doc: String) {
        client().prepareIndex(index).setId(id)
            .setSource(doc, XContentType.JSON).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get()
    }
}

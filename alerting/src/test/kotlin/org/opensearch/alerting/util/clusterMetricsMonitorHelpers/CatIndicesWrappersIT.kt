/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util.clusterMetricsMonitorHelpers

import org.opensearch.action.support.WriteRequest
import org.opensearch.alerting.randomClusterMetricsInput
import org.opensearch.alerting.util.clusterMetricsMonitorHelpers.CatIndicesResponseWrapper.Companion.WRAPPER_FIELD
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.model.ClusterMetricsInput
import org.opensearch.core.common.Strings
import org.opensearch.test.OpenSearchSingleNodeTestCase

class CatIndicesWrappersIT : OpenSearchSingleNodeTestCase() {
    private val path = ClusterMetricsInput.ClusterMetricType.CAT_INDICES.defaultPath

    fun `test CatIndicesRequestWrapper validate valid pathParams`() {
        // GIVEN
        val pathParams = "index1,index-name-2,index-3"

        // WHEN
        val requestWrapper = CatIndicesRequestWrapper(pathParams = pathParams)

        // THEN
        assertEquals(3, requestWrapper.clusterHealthRequest.indices().size)
        assertEquals(3, requestWrapper.clusterStateRequest.indices().size)
        assertEquals(3, requestWrapper.indexSettingsRequest.indices().size)
        assertEquals(3, requestWrapper.indicesStatsRequest.indices().size)
    }

    fun `test CatIndicesRequestWrapper validate without providing pathParams`() {
        // GIVEN & WHEN
        val requestWrapper = CatIndicesRequestWrapper()

        // THEN
        assertNull(requestWrapper.clusterHealthRequest.indices())
        assertEquals(Strings.EMPTY_ARRAY, requestWrapper.clusterStateRequest.indices())
        assertEquals(Strings.EMPTY_ARRAY, requestWrapper.indexSettingsRequest.indices())
        assertNull(requestWrapper.indicesStatsRequest.indices())
    }

    fun `test CatIndicesRequestWrapper validate blank pathParams`() {
        // GIVEN
        val pathParams = "    "

        // WHEN
        val requestWrapper = CatIndicesRequestWrapper(pathParams = pathParams)

        // THEN
        assertNull(requestWrapper.clusterHealthRequest.indices())
        assertEquals(Strings.EMPTY_ARRAY, requestWrapper.clusterStateRequest.indices())
        assertEquals(Strings.EMPTY_ARRAY, requestWrapper.indexSettingsRequest.indices())
        assertNull(requestWrapper.indicesStatsRequest.indices())
    }

    fun `test CatIndicesRequestWrapper validate empty pathParams`() {
        // GIVEN
        val pathParams = ""

        // WHEN
        val requestWrapper = CatIndicesRequestWrapper(pathParams = pathParams)

        // THEN
        assertNull(requestWrapper.clusterHealthRequest.indices())
        assertEquals(Strings.EMPTY_ARRAY, requestWrapper.clusterStateRequest.indices())
        assertEquals(Strings.EMPTY_ARRAY, requestWrapper.indexSettingsRequest.indices())
        assertNull(requestWrapper.indicesStatsRequest.indices())
    }

    fun `test CatIndicesRequestWrapper validate invalid pathParams`() {
        // GIVEN
        val pathParams = "_index1,index^2"

        // WHEN & THEN
        assertThrows(IllegalArgumentException::class.java) { CatIndicesRequestWrapper(pathParams = pathParams) }
    }

    fun `test CatIndicesResponseWrapper returns with only indices in pathParams`() {
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
            shards.map { (it[CatIndicesResponseWrapper.IndexInfo.INDEX_FIELD] as String) to it }.toMap()

        assertEquals(pathParamsIndices.size, returnedIndices.keys.size)
        testIndices.forEach { (indexName, docCount) ->
            if (pathParamsIndices.contains(indexName)) {
                assertEquals(
                    indexName,
                    returnedIndices[indexName]?.get(CatIndicesResponseWrapper.IndexInfo.INDEX_FIELD) as String
                )
                assertEquals(
                    docCount.toString(),
                    returnedIndices[indexName]?.get(CatIndicesResponseWrapper.IndexInfo.DOCS_COUNT_FIELD) as String
                )
            }
        }
    }

    fun `test CatIndicesResponseWrapper returns with all indices when empty pathParams`() {
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
            shards.map { (it[CatIndicesResponseWrapper.IndexInfo.INDEX_FIELD] as String) to it }.toMap()

        assertEquals(testIndices.size, returnedIndices.keys.size)
        testIndices.forEach { (indexName, docCount) ->
            assertEquals(
                indexName,
                returnedIndices[indexName]?.get(CatIndicesResponseWrapper.IndexInfo.INDEX_FIELD) as String
            )
            assertEquals(
                docCount.toString(),
                returnedIndices[indexName]?.get(CatIndicesResponseWrapper.IndexInfo.DOCS_COUNT_FIELD) as String
            )
        }
    }

    private fun indexDoc(index: String, id: String, doc: String) {
        client().prepareIndex(index).setId(id)
            .setSource(doc, XContentType.JSON).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get()
    }
}

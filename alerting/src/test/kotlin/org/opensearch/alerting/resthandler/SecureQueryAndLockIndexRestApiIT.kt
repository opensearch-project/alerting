/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.apache.hc.core5.http.ContentType.APPLICATION_JSON
import org.apache.hc.core5.http.io.entity.StringEntity
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.opensearch.alerting.AlertingRestTestCase
import org.opensearch.alerting.core.lock.LockService.Companion.LOCK_INDEX_NAME
import org.opensearch.alerting.makeRequest
import org.opensearch.alerting.randomDocumentLevelMonitor
import org.opensearch.client.ResponseException
import org.opensearch.client.RestClient
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.model.DocLevelMonitorInput
import org.opensearch.commons.alerting.model.DocLevelQuery
import org.opensearch.commons.alerting.model.ScheduledJob.Companion.DOC_LEVEL_QUERIES_INDEX
import org.opensearch.commons.rest.SecureRestClientBuilder
import org.opensearch.core.rest.RestStatus
import org.opensearch.index.query.QueryBuilders
import org.opensearch.search.builder.SearchSourceBuilder

/**
 * The doc-level query indices and the job lock index are system indices, so a user can't read or write them directly, even
 * with index permissions on every index.
 */
class SecureQueryAndLockIndexRestApiIT : AlertingRestTestCase() {

    companion object {
        @BeforeClass
        @JvmStatic fun setup() {
            org.junit.Assume.assumeTrue(System.getProperty("security", "false")!!.toBoolean())
        }
    }

    private val user = "queryLockUser"
    private val role = "query_lock_all_index_role"
    private var userClient: RestClient? = null

    @Before
    fun create() {
        createUser(user, arrayOf())
        // crud on every index plus the bulk permission a document write needs, so only the system index protection keeps the
        // user out of these indices
        createCustomIndexRole(role, "*", "indices:data/write/bulk")
        createUserRolesMapping(role, arrayOf(user))
        userClient = SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), user, password)
            .setSocketTimeout(60000)
            .build()
    }

    @After
    fun cleanup() {
        userClient?.close()
        deleteRoleAndRoleMapping(role)
        deleteUser(user)
    }

    fun `test user cannot directly search doc-level query system index`() {
        createDocLevelMonitorWithQuery()
        assertSearchHits(adminClient(), "$DOC_LEVEL_QUERIES_INDEX*", expectAny = true)

        // The security plugin leaves the system index out of the user's search, so it succeeds with no hits rather than failing
        assertSearchHits(userClient!!, "$DOC_LEVEL_QUERIES_INDEX*", expectAny = false)
    }

    fun `test user cannot directly write to doc-level query system index`() {
        createDocLevelMonitorWithQuery()

        assertWriteForbidden(DOC_LEVEL_QUERIES_INDEX)
    }

    fun `test user cannot directly search job lock system index`() {
        createLock()
        assertSearchHits(adminClient(), LOCK_INDEX_NAME, expectAny = true)

        assertSearchHits(userClient!!, LOCK_INDEX_NAME, expectAny = false)
    }

    fun `test user cannot directly write to job lock system index`() {
        createLock()

        assertWriteForbidden(LOCK_INDEX_NAME)
    }

    private fun createDocLevelMonitorWithQuery() {
        val testIndex = createTestIndex()
        val docQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "3", fields = listOf())
        createMonitor(
            randomDocumentLevelMonitor(
                inputs = listOf(DocLevelMonitorInput("description", listOf(testIndex), listOf(docQuery))),
                triggers = listOf(),
                enabled = false
            )
        )
    }

    private fun createLock() {
        indexDocWithAdminClient(LOCK_INDEX_NAME, "test-lock", """{"scheduled_job_id": "test", "lock_time": 1, "released": true}""")
    }

    private fun assertSearchHits(client: RestClient, index: String, expectAny: Boolean) {
        adminClient().makeRequest("POST", "/$index/_refresh?expand_wildcards=all")
        val query = SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).toString()
        val response = client.makeRequest("GET", "$index/_search", StringEntity(query, APPLICATION_JSON))
        assertEquals("Unexpected status", RestStatus.OK, response.restStatus())
        val hits = createParser(XContentType.JSON.xContent(), response.entity.content).map()["hits"] as Map<*, *>
        val numberDocsFound = (hits["total"] as Map<*, *>)["value"] as Int
        assertEquals("Docs found in [$index]: $numberDocsFound", expectAny, numberDocsFound > 0)
    }

    private fun assertWriteForbidden(index: String) {
        val e = expectThrows(ResponseException::class.java) {
            userClient!!.makeRequest("POST", "$index/_doc", StringEntity("""{"monitor_id": "fake"}""", APPLICATION_JSON))
        }
        assertEquals("Unexpected status", RestStatus.FORBIDDEN, e.response.restStatus())
    }
}

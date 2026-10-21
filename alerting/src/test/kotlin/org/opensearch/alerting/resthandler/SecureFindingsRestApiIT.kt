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
import org.opensearch.alerting.alerts.AlertIndices.Companion.ALL_FINDING_INDEX_PATTERN
import org.opensearch.alerting.alerts.AlertIndices.Companion.FINDING_HISTORY_WRITE_INDEX
import org.opensearch.alerting.makeRequest
import org.opensearch.client.ResponseException
import org.opensearch.client.RestClient
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.rest.SecureRestClientBuilder
import org.opensearch.core.rest.RestStatus
import org.opensearch.index.query.QueryBuilders
import org.opensearch.search.builder.SearchSourceBuilder

/**
 * The findings indices are system indices, so a user can't read or write them directly, even with index permissions on every
 * index. Findings are read through the Get Findings API instead.
 */
class SecureFindingsRestApiIT : AlertingRestTestCase() {

    companion object {
        @BeforeClass
        @JvmStatic fun setup() {
            org.junit.Assume.assumeTrue(System.getProperty("security", "false")!!.toBoolean())
        }
    }

    private val user = "findingsUser"
    private val role = "findings_all_index_role"
    private var userClient: RestClient? = null

    @Before
    fun create() {
        createUser(user, arrayOf())
        // crud on every index plus the bulk permission a document write needs, so only the system index protection keeps the
        // user out of the findings indices
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

    fun `test user cannot directly search findings system index`() {
        createFinding(matchingDocIds = listOf("1"))

        // The security plugin leaves the system index out of the user's search, so it succeeds with no hits rather than failing
        val query = SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).toString()
        val response = userClient!!.makeRequest("GET", "$ALL_FINDING_INDEX_PATTERN/_search", StringEntity(query, APPLICATION_JSON))
        assertEquals("Unexpected status", RestStatus.OK, response.restStatus())
        val hits = createParser(XContentType.JSON.xContent(), response.entity.content).map()["hits"] as Map<*, *>
        val numberDocsFound = (hits["total"] as Map<*, *>)["value"]
        assertEquals("User was able to directly search findings system index docs", 0, numberDocsFound)
    }

    fun `test user cannot directly write to findings system index`() {
        createFinding(matchingDocIds = listOf("1"))

        val e = expectThrows(ResponseException::class.java) {
            userClient!!.makeRequest(
                "POST",
                "$FINDING_HISTORY_WRITE_INDEX/_doc",
                StringEntity("""{"monitor_id": "fake"}""", APPLICATION_JSON)
            )
        }
        assertEquals("Unexpected status", RestStatus.FORBIDDEN, e.response.restStatus())
    }
}

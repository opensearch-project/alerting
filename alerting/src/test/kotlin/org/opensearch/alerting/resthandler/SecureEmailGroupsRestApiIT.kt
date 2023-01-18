/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.apache.hc.core5.http.ContentType
import org.apache.hc.core5.http.HttpHeaders
import org.apache.hc.core5.http.io.entity.StringEntity
import org.apache.hc.core5.http.message.BasicHeader
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.opensearch.alerting.ALERTING_GET_EMAIL_GROUP_ACCESS
import org.opensearch.alerting.ALERTING_SEARCH_EMAIL_GROUP_ACCESS
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.AlertingRestTestCase
import org.opensearch.alerting.TEST_HR_BACKEND_ROLE
import org.opensearch.alerting.TEST_HR_INDEX
import org.opensearch.alerting.TEST_HR_ROLE
import org.opensearch.alerting.makeRequest
import org.opensearch.client.RestClient
import org.opensearch.commons.rest.SecureRestClientBuilder
import org.opensearch.rest.RestStatus
import org.opensearch.test.junit.annotations.TestLogging

val SEARCH_EMAIL_GROUP_DSL = """
        {
          "from": 0,
          "size": 20,
          "sort": { "email_group.name.keyword": "desc" },
          "query": {
            "bool": {
              "must": {
                "match_all": {}
              }
            }
          }
        }
""".trimIndent()

@TestLogging("level:DEBUG", reason = "Debug for tests.")
@Suppress("UNCHECKED_CAST")
class SecureEmailGroupsRestApiIT : AlertingRestTestCase() {
    companion object {

        @BeforeClass
        @JvmStatic fun setup() {
            // things to execute once and keep around for the class
            org.junit.Assume.assumeTrue(System.getProperty("security", "false")!!.toBoolean())
        }
    }

    val user = "userOne"
    var userClient: RestClient? = null

    @Before
    fun create() {

        if (userClient == null) {
            createUser(user, user, arrayOf())
            userClient = SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), user, user).setSocketTimeout(60000).build()
        }
    }

    @After
    fun cleanup() {

        userClient?.close()
        deleteUser(user)
    }

    // Email groups related tests.

    fun `test get email groups with an user with get email group role`() {
        createUserWithTestDataAndCustomRole(
            user,
            TEST_HR_INDEX,
            TEST_HR_ROLE,
            listOf(TEST_HR_BACKEND_ROLE),
            getClusterPermissionsFromCustomRole(ALERTING_GET_EMAIL_GROUP_ACCESS)
        )

        val emailGroup = createRandomEmailGroupWithGivenName(true, randomAlphaOfLength(5))

        try {
            val getEmailGroupResponse = userClient?.makeRequest(
                "GET",
                "${AlertingPlugin.EMAIL_GROUP_BASE_URI}/${emailGroup.id}",
                StringEntity(
                    emailGroup.toJsonString(),
                    ContentType.APPLICATION_JSON
                ),
                BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
            )
            assertEquals("Get Email Group failed", RestStatus.OK, getEmailGroupResponse?.restStatus())
        } finally {
            deleteRoleAndRoleMapping(TEST_HR_ROLE)
        }
    }

    fun `test search email groups with an user with search email group role`() {
        createUserWithTestDataAndCustomRole(
            user,
            TEST_HR_INDEX,
            TEST_HR_ROLE,
            listOf(TEST_HR_BACKEND_ROLE),
            getClusterPermissionsFromCustomRole(ALERTING_SEARCH_EMAIL_GROUP_ACCESS)
        )

        createRandomEmailGroupWithGivenName(true, randomAlphaOfLength(10))

        try {
            val searchEmailGroupsResponse = userClient?.makeRequest(
                "POST",
                "${AlertingPlugin.EMAIL_GROUP_BASE_URI}/_search",
                StringEntity(
                    SEARCH_EMAIL_GROUP_DSL,
                    ContentType.APPLICATION_JSON
                ),
                BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
            )
            assertEquals("Search Email Group failed", RestStatus.OK, searchEmailGroupsResponse?.restStatus())
        } finally {
            deleteRoleAndRoleMapping(TEST_HR_ROLE)
        }
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.apache.http.HttpHeaders
import org.apache.http.entity.ContentType
import org.apache.http.entity.StringEntity
import org.apache.http.message.BasicHeader
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.opensearch.alerting.ALERTING_GET_EMAIL_ACCOUNT_ACCESS
import org.opensearch.alerting.ALERTING_SEARCH_EMAIL_ACCOUNT_ACCESS
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.AlertingRestTestCase
import org.opensearch.alerting.TEST_HR_BACKEND_ROLE
import org.opensearch.alerting.TEST_HR_INDEX
import org.opensearch.alerting.TEST_HR_ROLE
import org.opensearch.alerting.makeRequest
import org.opensearch.client.RestClient
import org.opensearch.commons.rest.SecureRestClientBuilder
import org.opensearch.rest.RestStatus

val SEARCH_EMAIL_ACCOUNT_DSL = """
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

class SecureEmailAccountRestApiIT : AlertingRestTestCase() {

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

    // Email account related tests.

    fun `test get email accounts with an user with get email account role`() {
        createUserWithTestDataAndCustomRole(
            user,
            TEST_HR_INDEX,
            TEST_HR_ROLE,
            TEST_HR_BACKEND_ROLE,
            getClusterPermissionsFromCustomRole(ALERTING_GET_EMAIL_ACCOUNT_ACCESS)
        )

        val emailAccount = createRandomEmailAccountWithGivenName(true, randomAlphaOfLength(5))

        try {
            val emailAccountResponse = userClient?.makeRequest(
                "GET",
                "${AlertingPlugin.EMAIL_ACCOUNT_BASE_URI}/${emailAccount.id}",
                StringEntity(
                    emailAccount.toJsonString(),
                    ContentType.APPLICATION_JSON
                ),
                BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
            )

            assertEquals("Get Email failed", RestStatus.OK, emailAccountResponse?.restStatus())
        } finally {
            deleteRoleAndRoleMapping(TEST_HR_ROLE)
        }
    }

    fun `test search email accounts with an user with search email account role`() {

        createUserWithTestDataAndCustomRole(
            user,
            TEST_HR_INDEX,
            TEST_HR_ROLE,
            TEST_HR_BACKEND_ROLE,
            getClusterPermissionsFromCustomRole(ALERTING_SEARCH_EMAIL_ACCOUNT_ACCESS)
        )

        createRandomEmailAccountWithGivenName(true, randomAlphaOfLength(10))

        try {
            val searchEmailAccountResponse = userClient?.makeRequest(
                "POST",
                "${AlertingPlugin.EMAIL_ACCOUNT_BASE_URI}/_search",
                StringEntity(SEARCH_EMAIL_ACCOUNT_DSL, ContentType.APPLICATION_JSON),
                BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
            )
            assertEquals("Search Email failed", RestStatus.OK, searchEmailAccountResponse?.restStatus())
        } finally {
            deleteRoleAndRoleMapping(TEST_HR_ROLE)
        }
    }

    /*
    TODO: https://github.com/opensearch-project/alerting/issues/300

    fun `test get email accounts with an user without get email account role`() {
        createUserWithTestDataAndCustomRole(
            user,
            TEST_HR_INDEX,
            TEST_HR_ROLE,
            TEST_HR_BACKEND_ROLE,
            getClusterPermissionsFromCustomRole(ALERTING_NO_ACCESS_ROLE)
        )

        val emailAccount = createRandomEmailAccountWithGivenName(true, randomAlphaOfLength(5))

        try {
            userClient?.makeRequest(
                "GET",
                "${AlertingPlugin.EMAIL_ACCOUNT_BASE_URI}/${emailAccount.id}",
                StringEntity(
                    emailAccount.toJsonString(),
                    ContentType.APPLICATION_JSON
                ),
                BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
            )
            fail("Expected 403 Method FORBIDDEN response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.FORBIDDEN, e.response.restStatus())
        } finally {
            deleteRoleAndRoleMapping(TEST_HR_ROLE)
        }
    }

    fun `test search email accounts with an user without search email account role`() {

        createUserWithTestDataAndCustomRole(
            user,
            TEST_HR_INDEX,
            TEST_HR_ROLE,
            TEST_HR_BACKEND_ROLE,
            getClusterPermissionsFromCustomRole(ALERTING_NO_ACCESS_ROLE)
        )

        createRandomEmailAccountWithGivenName(true, randomAlphaOfLength(5))

        try {
            userClient?.makeRequest(
                "POST",
                "${AlertingPlugin.EMAIL_ACCOUNT_BASE_URI}/_search",
                StringEntity(SEARCH_EMAIL_ACCOUNT_DSL, ContentType.APPLICATION_JSON),
                BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
            )
            fail("Expected 403 Method FORBIDDEN response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.FORBIDDEN, e.response.restStatus())
        } finally {
            deleteRoleAndRoleMapping(TEST_HR_ROLE)
        }
    }

     */
}

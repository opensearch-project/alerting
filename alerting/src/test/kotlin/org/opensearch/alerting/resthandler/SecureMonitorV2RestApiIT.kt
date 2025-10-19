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
import org.opensearch.alerting.AlertingPlugin.Companion.MONITOR_V2_BASE_URI
import org.opensearch.alerting.AlertingRestTestCase
import org.opensearch.alerting.makeRequest
import org.opensearch.alerting.randomPPLMonitor
import org.opensearch.client.ResponseException
import org.opensearch.client.RestClient
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.rest.SecureRestClientBuilder
import org.opensearch.core.rest.RestStatus
import org.opensearch.index.query.QueryBuilders
import org.opensearch.search.builder.SearchSourceBuilder

/***
 * Tests Alerting V2 CRUD with role-based access control
 *
 * Gradle command to run this suite:
 * ./gradlew :alerting:integTest -Dhttps=true -Dsecurity=true -Duser=admin -Dpassword=admin \
 * --tests "org.opensearch.alerting.resthandler.SecureMonitorV2RestApiIT"
 */
class SecureMonitorV2RestApiIT : AlertingRestTestCase() {

    companion object {
        @BeforeClass
        @JvmStatic fun setup() {
            // things to execute once and keep around for the class
            org.junit.Assume.assumeTrue(System.getProperty("security", "false")!!.toBoolean())
        }
    }

    val user = "userD"
    var userClient: RestClient? = null

    @Before
    fun create() {
        if (userClient == null) {
            createUser(user, arrayOf())
            userClient = SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), user, password)
                .setSocketTimeout(60000)
                .setConnectionRequestTimeout(180000)
                .build()
        }
    }

    @After
    fun cleanup() {
        userClient?.close()
        deleteUser(user)
    }

    fun `test create monitor that queries index user doesn't have access to fails`() {
        if (!isHttps()) {
            return
        }

        createIndex("some_index", Settings.EMPTY)
        createIndex("other_index", Settings.EMPTY)

        val pplMonitorConfig = randomPPLMonitor(
            query = "source = some_index | head 10"
        )

        val userCustomRole = "custom_role_1"
        createUserWithAdminLevelCustomRole(
            user,
            listOf(),
            userCustomRole,
            "other_index"
        )

        try {
            createMonitorV2WithClient(
                userClient!!,
                monitorV2 = pplMonitorConfig
            )
            fail("Expected create monitor to fail as user does not have permissions to index that monitor queries")
        } catch (e: ResponseException) {
            assertEquals("Unexpected error status", RestStatus.BAD_REQUEST.status, e.response.statusLine.statusCode)
        }

        ensureNumMonitorV2s(0)
    }

    fun `test RBAC create monitor with backend roles user has access to succeeds`() {
        enableFilterBy()
        if (!isHttps()) {
            return
        }

        val pplMonitorConfig = randomPPLMonitor(enabled = true)

        val userCustomRole = "custom_role_1"
        createUserWithAdminLevelCustomRole(
            user,
            listOf("backend_role_a", "backend_role_b"),
            userCustomRole
        )

        createMonitorV2WithClient(userClient!!, monitorV2 = pplMonitorConfig, listOf("backend_role_a"))

        ensureNumMonitorV2s(1)
    }

    fun `test RBAC create monitor with backend roles user has no access to fails`() {
        enableFilterBy()
        if (!isHttps()) {
            return
        }

        val pplMonitorConfig = randomPPLMonitor(enabled = true)

        val userCustomRole = "custom_role_1"
        createUserWithAdminLevelCustomRole(
            user,
            listOf("backend_role_a", "backend_role_b"),
            userCustomRole
        )

        try {
            createMonitorV2WithClient(
                userClient!!,
                monitorV2 = pplMonitorConfig,
                listOf("backend_role_a", "backend_role_b", "backend_role_c")
            )
            fail("Expected create monitor to fail as user does not have backend_role_c backend role")
        } catch (e: ResponseException) {
            assertEquals("Unexpected error status", RestStatus.FORBIDDEN.status, e.response.statusLine.statusCode)
        }

        ensureNumMonitorV2s(0)
    }

    fun `test RBAC update monitorV2 as user with correct backend roles succeeds`() {
        enableFilterBy()
        if (!isHttps()) {
            return
        }
        val pplMonitorConfig = randomPPLMonitor(enabled = true)

        val userCustomRole = "custom_role_1"
        createUserWithAdminLevelCustomRole(
            user,
            listOf("backend_role_a", "backend_role_b"),
            userCustomRole
        )

        val pplMonitor = createMonitorV2WithClient(userClient!!, pplMonitorConfig, listOf("backend_role_a", "backend_role_b"))

        // getUser should have access to the monitor above created by user
        val getUser = "getUser"
        val getUserCustomRole = "custom_role_2" // different role from the one created before to avoid conflicts
        createUserWithAdminLevelCustomRole(
            getUser,
            listOf("backend_role_a"),
            getUserCustomRole
        )

        val getUserClient = SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), getUser, password)
            .setSocketTimeout(60000)
            .setConnectionRequestTimeout(180000)
            .build()

        val newMonitor = randomPPLMonitor()
        val updateMonitorResponse = getUserClient!!.makeRequest(
            "PUT",
            "$MONITOR_V2_BASE_URI/${pplMonitor.id}",
            newMonitor.toHttpEntity(),
            BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
        )
        assertEquals("Update monitorV2 failed", RestStatus.OK, updateMonitorResponse.restStatus())

        // cleanup
        getUserClient.close()
    }

    fun `test RBAC update monitorV2 as user without correct backend roles fails`() {
        enableFilterBy()
        if (!isHttps()) {
            return
        }
        val pplMonitorConfig = randomPPLMonitor(enabled = true)

        val userCustomRole = "custom_role_1"
        createUserWithAdminLevelCustomRole(
            user,
            listOf("backend_role_a", "backend_role_b"),
            userCustomRole
        )

        val pplMonitor = createMonitorV2WithClient(userClient!!, pplMonitorConfig, listOf("backend_role_a", "backend_role_b"))

        // getUser should have access to the monitor above created by user
        val getUser = "getUser"
        val getUserCustomRole = "custom_role_2" // different role from the one created before to avoid conflicts
        createUserWithAdminLevelCustomRole(
            getUser,
            listOf("backend_role_c"),
            getUserCustomRole
        )

        val getUserClient = SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), getUser, password)
            .setSocketTimeout(60000)
            .setConnectionRequestTimeout(180000)
            .build()

        val newMonitor = randomPPLMonitor()

        try {
            getUserClient!!.makeRequest(
                "PUT",
                "$MONITOR_V2_BASE_URI/${pplMonitor.id}",
                newMonitor.toHttpEntity(),
                BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
            )
            fail("Expected update monitor to fail as user does not have the correct backend roles")
        } catch (e: ResponseException) {
            assertEquals("Unexpected error status", RestStatus.FORBIDDEN.status, e.response.statusLine.statusCode)
        }

        // cleanup
        getUserClient.close()
    }

    fun `test RBAC get monitorV2 as user with correct backend roles succeeds`() {
        enableFilterBy()
        if (!isHttps()) {
            return
        }
        val pplMonitorConfig = randomPPLMonitor(enabled = true)

        val userCustomRole = "custom_role_1"
        createUserWithAdminLevelCustomRole(
            user,
            listOf("backend_role_a", "backend_role_b"),
            userCustomRole
        )

        val pplMonitor = createMonitorV2WithClient(userClient!!, pplMonitorConfig, listOf("backend_role_a", "backend_role_b"))

        // getUser should have access to the monitor above created by user
        val getUser = "getUser"
        val getUserCustomRole = "custom_role_2" // different role from the one created before to avoid conflicts
        createUserWithAdminLevelCustomRole(
            getUser,
            listOf("backend_role_a"),
            getUserCustomRole
        )

        val getUserClient = SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), getUser, password)
            .setSocketTimeout(60000)
            .setConnectionRequestTimeout(180000)
            .build()

        val getMonitorResponse = getUserClient!!.makeRequest(
            "GET",
            "$MONITOR_V2_BASE_URI/${pplMonitor.id}",
            null,
            BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
        )
        assertEquals("Get monitorV2 failed", RestStatus.OK, getMonitorResponse.restStatus())

        // cleanup
        getUserClient.close()
    }

    fun `test RBAC get monitorV2 as user without correct backend roles fails`() {
        enableFilterBy()
        if (!isHttps()) {
            return
        }
        val pplMonitorConfig = randomPPLMonitor(enabled = true)

        val userCustomRole = "custom_role_1"
        createUserWithAdminLevelCustomRole(
            user,
            listOf("backend_role_a", "backend_role_b"),
            userCustomRole
        )

        val pplMonitor = createMonitorV2WithClient(userClient!!, pplMonitorConfig, listOf("backend_role_a", "backend_role_b"))

        // getUser should not have access to the monitor above created by user
        val getUser = "getUser"
        val getUserCustomRole = "custom_role_2" // different role from the one created before to avoid conflicts
        createUserWithAdminLevelCustomRole(
            getUser,
            listOf("backend_role_c"),
            getUserCustomRole
        )

        val getUserClient = SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), getUser, password)
            .setSocketTimeout(60000)
            .setConnectionRequestTimeout(180000)
            .build()

        try {
            getUserClient!!.makeRequest(
                "GET",
                "$MONITOR_V2_BASE_URI/${pplMonitor.id}",
                null,
                BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
            )
            fail("Expected Forbidden exception")
        } catch (e: ResponseException) {
            assertEquals("Unexpected get monitor status", RestStatus.FORBIDDEN.status, e.response.statusLine.statusCode)
        } finally {
            getUserClient?.close()
        }
    }

    fun `test RBAC search monitorV2 as user with correct backend roles returns results`() {
        enableFilterBy()
        if (!isHttps()) {
            return
        }
        val pplMonitorConfig = randomPPLMonitor(enabled = true)

        val userCustomRole = "custom_role_1"
        createUserWithAdminLevelCustomRole(
            user,
            listOf("backend_role_a", "backend_role_b"),
            userCustomRole
        )

        createMonitorV2WithClient(userClient!!, pplMonitorConfig, listOf("backend_role_a", "backend_role_b"))

        // getUser should have access to the monitor above created by user
        val searchUser = "searchUser"
        val getUserCustomRole = "custom_role_2" // different role from the one created before to avoid conflicts
        createUserWithAdminLevelCustomRole(
            searchUser,
            listOf("backend_role_a"),
            getUserCustomRole
        )

        val getUserClient = SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), searchUser, password)
            .setSocketTimeout(60000)
            .setConnectionRequestTimeout(180000)
            .build()

        val search = SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).toString()
        val searchMonitorResponse = getUserClient!!.makeRequest(
            "POST",
            "$MONITOR_V2_BASE_URI/_search",
            StringEntity(search, ContentType.APPLICATION_JSON),
            BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
        )
        assertEquals("Search monitorV2 failed", RestStatus.OK, searchMonitorResponse.restStatus())

        val xcp = createParser(XContentType.JSON.xContent(), searchMonitorResponse.entity.content)
        val hits = xcp.map()["hits"]!! as Map<String, Map<String, Any>>
        val numberDocsFound = hits["total"]?.get("value")
        assertEquals("Created PPL Monitor should be visible but was not", 1, numberDocsFound)

        // cleanup
        getUserClient.close()
    }

    fun `test RBAC search monitorV2 as user without correct backend roles returns no results`() {
        enableFilterBy()
        if (!isHttps()) {
            return
        }
        val pplMonitorConfig = randomPPLMonitor(enabled = true)

        val userCustomRole = "custom_role_1"
        createUserWithAdminLevelCustomRole(
            user,
            listOf("backend_role_a", "backend_role_b"),
            userCustomRole
        )

        createMonitorV2WithClient(userClient!!, pplMonitorConfig, listOf("backend_role_a", "backend_role_b"))

        // getUser should have access to the monitor above created by user
        val searchUser = "searchUser"
        val getUserCustomRole = "custom_role_2" // different role from the one created before to avoid conflicts
        createUserWithAdminLevelCustomRole(
            searchUser,
            listOf("backend_role_c"),
            getUserCustomRole
        )

        val getUserClient = SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), searchUser, password)
            .setSocketTimeout(60000)
            .setConnectionRequestTimeout(180000)
            .build()

        val search = SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).toString()
        val searchMonitorResponse = getUserClient!!.makeRequest(
            "POST",
            "$MONITOR_V2_BASE_URI/_search",
            StringEntity(search, ContentType.APPLICATION_JSON),
            BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
        )
        assertEquals("Search monitorV2 failed", RestStatus.OK, searchMonitorResponse.restStatus())

        val xcp = createParser(XContentType.JSON.xContent(), searchMonitorResponse.entity.content)
        val hits = xcp.map()["hits"]!! as Map<String, Map<String, Any>>
        val numberDocsFound = hits["total"]?.get("value")
        assertEquals("Created PPL Monitor should be visible but was not", 0, numberDocsFound)

        // cleanup
        getUserClient.close()
    }

    fun `test RBAC execute monitorV2 as user with correct backend roles succeeds`() {
        enableFilterBy()
        if (!isHttps()) {
            return
        }
        val pplMonitorConfig = randomPPLMonitor(enabled = true)

        val userCustomRole = "custom_role_1"
        createUserWithAdminLevelCustomRole(
            user,
            listOf("backend_role_a", "backend_role_b"),
            userCustomRole
        )

        val pplMonitor = createMonitorV2WithClient(userClient!!, pplMonitorConfig, listOf("backend_role_a", "backend_role_b"))

        // getUser should have access to the monitor above created by user
        val executeUser = "executeUser"
        val executeUserCustomRole = "custom_role_2" // different role from the one created before to avoid conflicts
        createUserWithAdminLevelCustomRole(
            executeUser,
            listOf("backend_role_a"),
            executeUserCustomRole
        )

        val getUserClient = SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), executeUser, password)
            .setSocketTimeout(60000)
            .setConnectionRequestTimeout(180000)
            .build()

        val getMonitorResponse = getUserClient!!.makeRequest(
            "POST",
            "$MONITOR_V2_BASE_URI/${pplMonitor.id}/_execute",
            null,
            BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
        )
        assertEquals("Get monitorV2 failed", RestStatus.OK, getMonitorResponse.restStatus())

        // cleanup
        getUserClient.close()
    }

    fun `test RBAC execute monitorV2 as user without correct backend roles fails`() {
        enableFilterBy()
        if (!isHttps()) {
            return
        }
        val pplMonitorConfig = randomPPLMonitor(enabled = true)

        val userCustomRole = "custom_role_1"
        createUserWithAdminLevelCustomRole(
            user,
            listOf("backend_role_a", "backend_role_b"),
            userCustomRole
        )

        val pplMonitor = createMonitorV2WithClient(userClient!!, pplMonitorConfig, listOf("backend_role_a", "backend_role_b"))

        // getUser should not have access to the monitor above created by user
        val executeUser = "executeUser"
        val executeUserCustomRole = "custom_role_2" // different role from the one created before to avoid conflicts
        createUserWithAdminLevelCustomRole(
            executeUser,
            listOf("backend_role_c"),
            executeUserCustomRole
        )

        val getUserClient = SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), executeUser, password)
            .setSocketTimeout(60000)
            .setConnectionRequestTimeout(180000)
            .build()

        try {
            getUserClient!!.makeRequest(
                "POST",
                "$MONITOR_V2_BASE_URI/${pplMonitor.id}/_execute",
                null,
                BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
            )
            fail("Expected Forbidden exception")
        } catch (e: ResponseException) {
            assertEquals("Unexpected delete monitor status", RestStatus.FORBIDDEN.status, e.response.statusLine.statusCode)
        } finally {
            getUserClient?.close()
        }
    }

    fun `test RBAC delete monitorV2 as user with correct backend roles succeeds`() {
        enableFilterBy()
        if (!isHttps()) {
            return
        }
        val pplMonitorConfig = randomPPLMonitor(enabled = true)

        val userCustomRole = "custom_role_1"
        createUserWithAdminLevelCustomRole(
            user,
            listOf("backend_role_a", "backend_role_b"),
            userCustomRole
        )

        val pplMonitor = createMonitorV2WithClient(userClient!!, pplMonitorConfig, listOf("backend_role_a", "backend_role_b"))

        // getUser should have access to the monitor above created by user
        val deleteUser = "deleteUser"
        val deleteUserCustomRole = "custom_role_2" // different role from the one created before to avoid conflicts
        createUserWithAdminLevelCustomRole(
            deleteUser,
            listOf("backend_role_a"),
            deleteUserCustomRole
        )

        val getUserClient = SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), deleteUser, password)
            .setSocketTimeout(60000)
            .setConnectionRequestTimeout(180000)
            .build()

        val getMonitorResponse = getUserClient!!.makeRequest(
            "DELETE",
            "$MONITOR_V2_BASE_URI/${pplMonitor.id}",
            null,
            BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
        )
        assertEquals("Get monitorV2 failed", RestStatus.OK, getMonitorResponse.restStatus())

        ensureNumMonitorV2s(0)

        // cleanup
        getUserClient.close()
    }

    fun `test RBAC delete monitorV2 as user without correct backend roles fails`() {
        enableFilterBy()
        if (!isHttps()) {
            return
        }
        val pplMonitorConfig = randomPPLMonitor(enabled = true)

        val userCustomRole = "custom_role_1"
        createUserWithAdminLevelCustomRole(
            user,
            listOf("backend_role_a", "backend_role_b"),
            userCustomRole
        )

        val pplMonitor = createMonitorV2WithClient(userClient!!, pplMonitorConfig, listOf("backend_role_a", "backend_role_b"))

        // getUser should not have access to the monitor above created by user
        val deleteUser = "deleteUser"
        val deleteUserCustomRole = "custom_role_2" // different role from the one created before to avoid conflicts
        createUserWithAdminLevelCustomRole(
            deleteUser,
            listOf("backend_role_c"),
            deleteUserCustomRole
        )

        val getUserClient = SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), deleteUser, password)
            .setSocketTimeout(60000)
            .setConnectionRequestTimeout(180000)
            .build()

        try {
            getUserClient!!.makeRequest(
                "DELETE",
                "$MONITOR_V2_BASE_URI/${pplMonitor.id}",
                null,
                BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
            )
            fail("Expected Forbidden exception")
        } catch (e: ResponseException) {
            assertEquals("Unexpected delete monitor status", RestStatus.FORBIDDEN.status, e.response.statusLine.statusCode)
        } finally {
            getUserClient?.close()
        }

        ensureNumMonitorV2s(1)
    }
}

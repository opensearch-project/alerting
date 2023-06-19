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
import org.opensearch.alerting.ADMIN
import org.opensearch.alerting.ALERTING_BASE_URI
import org.opensearch.alerting.ALERTING_DELETE_MONITOR_ACCESS
import org.opensearch.alerting.ALERTING_EXECUTE_MONITOR_ACCESS
import org.opensearch.alerting.ALERTING_FULL_ACCESS_ROLE
import org.opensearch.alerting.ALERTING_GET_ALERTS_ACCESS
import org.opensearch.alerting.ALERTING_GET_MONITOR_ACCESS
import org.opensearch.alerting.ALERTING_INDEX_MONITOR_ACCESS
import org.opensearch.alerting.ALERTING_NO_ACCESS_ROLE
import org.opensearch.alerting.ALERTING_READ_ONLY_ACCESS
import org.opensearch.alerting.ALERTING_SEARCH_MONITOR_ONLY_ACCESS
import org.opensearch.alerting.ALL_ACCESS_ROLE
import org.opensearch.alerting.ALWAYS_RUN
import org.opensearch.alerting.AlertingRestTestCase
import org.opensearch.alerting.DRYRUN_MONITOR
import org.opensearch.alerting.READALL_AND_MONITOR_ROLE
import org.opensearch.alerting.TERM_DLS_QUERY
import org.opensearch.alerting.TEST_HR_BACKEND_ROLE
import org.opensearch.alerting.TEST_HR_INDEX
import org.opensearch.alerting.TEST_HR_ROLE
import org.opensearch.alerting.TEST_NON_HR_INDEX
import org.opensearch.alerting.assertUserNull
import org.opensearch.alerting.makeRequest
import org.opensearch.alerting.randomAction
import org.opensearch.alerting.randomAlert
import org.opensearch.alerting.randomBucketLevelMonitor
import org.opensearch.alerting.randomBucketLevelTrigger
import org.opensearch.alerting.randomDocumentLevelMonitor
import org.opensearch.alerting.randomQueryLevelMonitor
import org.opensearch.alerting.randomQueryLevelTrigger
import org.opensearch.alerting.randomTemplateScript
import org.opensearch.client.Response
import org.opensearch.client.ResponseException
import org.opensearch.client.RestClient
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentType
import org.opensearch.common.xcontent.json.JsonXContent
import org.opensearch.commons.alerting.aggregation.bucketselectorext.BucketSelectorExtAggregationBuilder
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.commons.alerting.model.DocLevelMonitorInput
import org.opensearch.commons.alerting.model.SearchInput
import org.opensearch.commons.authuser.User
import org.opensearch.commons.rest.SecureRestClientBuilder
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.index.query.QueryBuilders
import org.opensearch.rest.RestStatus
import org.opensearch.script.Script
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder
import org.opensearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.test.junit.annotations.TestLogging

@TestLogging("level:DEBUG", reason = "Debug for tests.")
@Suppress("UNCHECKED_CAST")
class SecureMonitorRestApiIT : AlertingRestTestCase() {

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

    // Create Monitor related security tests
    fun `test create monitor with an user with alerting role`() {

        createUserWithTestDataAndCustomRole(
            user,
            TEST_HR_INDEX,
            TEST_HR_ROLE,
            listOf(TEST_HR_BACKEND_ROLE),
            getClusterPermissionsFromCustomRole(ALERTING_INDEX_MONITOR_ACCESS)
        )
        try {
            // randomMonitor has a dummy user, api ignores the User passed as part of monitor, it picks user info from the logged-in user.
            val monitor = randomQueryLevelMonitor().copy(
                inputs = listOf(
                    SearchInput(
                        indices = listOf(TEST_HR_INDEX), query = SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
                    )
                )
            )
            val createResponse = userClient?.makeRequest("POST", ALERTING_BASE_URI, emptyMap(), monitor.toHttpEntity())
            assertEquals("Create monitor failed", RestStatus.CREATED, createResponse?.restStatus())

            assertUserNull(createResponse?.asMap()!!["monitor"] as HashMap<String, Any>)
        } finally {
            deleteRoleAndRoleMapping(TEST_HR_ROLE)
        }
    }

    /*
    TODO: https://github.com/opensearch-project/alerting/issues/300
    */
    fun `test create monitor with an user without alerting role`() {

        createUserWithTestDataAndCustomRole(
            user,
            TEST_HR_INDEX,
            TEST_HR_ROLE,
            listOf(TEST_HR_BACKEND_ROLE),
            getClusterPermissionsFromCustomRole(ALERTING_NO_ACCESS_ROLE)
        )
        try {
            val monitor = randomQueryLevelMonitor().copy(
                inputs = listOf(
                    SearchInput(
                        indices = listOf(TEST_HR_INDEX), query = SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
                    )
                )
            )
            userClient?.makeRequest("POST", ALERTING_BASE_URI, emptyMap(), monitor.toHttpEntity())
            fail("Expected 403 Method FORBIDDEN response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.FORBIDDEN, e.response.restStatus())
        } finally {
            deleteRoleAndRoleMapping(TEST_HR_ROLE)
        }
    }

    fun `test create monitor with an user with read-only role`() {

        createUserWithTestData(user, TEST_HR_INDEX, TEST_HR_ROLE, TEST_HR_BACKEND_ROLE)
        createUserRolesMapping(ALERTING_READ_ONLY_ACCESS, arrayOf(user))

        try {
            val monitor = randomQueryLevelMonitor().copy(
                inputs = listOf(
                    SearchInput(
                        indices = listOf(TEST_HR_INDEX), query = SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
                    )
                )
            )
            userClient?.makeRequest("POST", ALERTING_BASE_URI, emptyMap(), monitor.toHttpEntity())
            fail("Expected 403 Method FORBIDDEN response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.FORBIDDEN, e.response.restStatus())
        } finally {
            deleteRoleAndRoleMapping(TEST_HR_ROLE)
            deleteRoleMapping(ALERTING_READ_ONLY_ACCESS)
        }
    }

    fun `test query monitors with an user with only search monitor cluster permission`() {

        createUserWithTestDataAndCustomRole(
            user,
            TEST_HR_INDEX,
            TEST_HR_ROLE,
            listOf(TEST_HR_BACKEND_ROLE),
            getClusterPermissionsFromCustomRole(ALERTING_SEARCH_MONITOR_ONLY_ACCESS)
        )
        val monitor = createRandomMonitor(true)

        val search = SearchSourceBuilder().query(QueryBuilders.termQuery("_id", monitor.id)).toString()
        val searchResponse = client().makeRequest(
            "GET", "$ALERTING_BASE_URI/_search",
            emptyMap(),
            StringEntity(search, ContentType.APPLICATION_JSON)
        )

        assertEquals("Search monitor failed", RestStatus.OK, searchResponse.restStatus())
        val xcp = createParser(XContentType.JSON.xContent(), searchResponse.entity.content)
        val hits = xcp.map()["hits"]!! as Map<String, Map<String, Any>>
        val numberDocsFound = hits["total"]?.get("value")
        assertEquals("Monitor not found during search", 1, numberDocsFound)
        deleteRoleAndRoleMapping(TEST_HR_ROLE)
    }

    /*
    TODO: https://github.com/opensearch-project/alerting/issues/300
    */
    fun `test query monitors with an user without search monitor cluster permission`() {

        createUserWithTestDataAndCustomRole(
            user,
            TEST_HR_INDEX,
            TEST_HR_ROLE,
            listOf(TEST_HR_BACKEND_ROLE),
            getClusterPermissionsFromCustomRole(ALERTING_NO_ACCESS_ROLE)
        )
        try {
            val monitor = randomQueryLevelMonitor().copy(
                inputs = listOf(
                    SearchInput(
                        indices = listOf(TEST_HR_INDEX), query = SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
                    )
                )
            )
            userClient?.makeRequest("POST", ALERTING_BASE_URI, emptyMap(), monitor.toHttpEntity())
            fail("Expected 403 Method FORBIDDEN response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.FORBIDDEN, e.response.restStatus())
        } finally {
            deleteRoleAndRoleMapping(TEST_HR_ROLE)
        }
    }

    fun `test create monitor with an user without index read role`() {

        createUserWithTestDataAndCustomRole(
            user,
            TEST_HR_INDEX,
            TEST_HR_ROLE,
            listOf(TEST_HR_BACKEND_ROLE),
            getClusterPermissionsFromCustomRole(ALERTING_INDEX_MONITOR_ACCESS)
        )
        try {
            val monitor = randomQueryLevelMonitor().copy(
                inputs = listOf(
                    SearchInput(
                        indices = listOf(TEST_NON_HR_INDEX), query = SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
                    )
                )
            )
            val createResponse = userClient?.makeRequest("POST", ALERTING_BASE_URI, emptyMap(), monitor.toHttpEntity())
            assertEquals("Create monitor failed", RestStatus.CREATED, createResponse?.restStatus())
            fail("Expected 403 Method FORBIDDEN response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.FORBIDDEN, e.response.restStatus())
        } finally {
            deleteRoleAndRoleMapping(TEST_HR_ROLE)
        }
    }

    fun `test create monitor with disable filter by`() {
        disableFilterBy()
        val monitor = randomQueryLevelMonitor()
        val createResponse = client().makeRequest("POST", ALERTING_BASE_URI, emptyMap(), monitor.toHttpEntity())
        assertEquals("Create monitor failed", RestStatus.CREATED, createResponse.restStatus())
        assertUserNull(createResponse.asMap()["monitor"] as HashMap<String, Any>)
    }

    fun `test get monitor with an user with get monitor role`() {
        createUserWithTestDataAndCustomRole(
            user,
            TEST_HR_INDEX,
            TEST_HR_ROLE,
            listOf(TEST_HR_BACKEND_ROLE),
            getClusterPermissionsFromCustomRole(ALERTING_GET_MONITOR_ACCESS)
        )

        val monitor = createRandomMonitor(true)

        try {
            val getMonitorResponse = userClient?.makeRequest(
                "GET",
                "$ALERTING_BASE_URI/${monitor.id}",
                null,
                BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
            )
            assertEquals("Get monitor failed", RestStatus.OK, getMonitorResponse?.restStatus())
        } finally {
            deleteRoleAndRoleMapping(TEST_HR_ROLE)
        }
    }

    /*
    TODO: https://github.com/opensearch-project/alerting/issues/300
     */
    fun `test get monitor with an user without get monitor role`() {
        createUserWithTestDataAndCustomRole(
            user,
            TEST_HR_INDEX,
            TEST_HR_ROLE,
            listOf(TEST_HR_BACKEND_ROLE),
            getClusterPermissionsFromCustomRole(ALERTING_NO_ACCESS_ROLE)
        )

        val monitor = createRandomMonitor(true)

        try {
            userClient?.makeRequest(
                "GET",
                "$ALERTING_BASE_URI/${monitor.id}",
                null,
                BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
            )
            fail("Expected 403 Method FORBIDDEN response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.FORBIDDEN, e.response.restStatus())
        } finally {
            deleteRoleAndRoleMapping(TEST_HR_ROLE)
        }
    }

    fun getDocs(response: Response?): Any? {
        val hits = createParser(
            XContentType.JSON.xContent(),
            response?.entity?.content
        ).map()["hits"]!! as Map<String, Map<String, Any>>
        return hits["total"]?.get("value")
    }

    // Query Monitors related security tests
    fun `test update monitor with disable filter by`() {
        disableFilterBy()
        val monitor = randomQueryLevelMonitor(enabled = true)

        val createdMonitor = createMonitor(monitor = monitor)

        assertNotNull("The monitor was not created", createdMonitor)
        assertTrue("The monitor was not enabled", createdMonitor.enabled)

        val monitorV2 = createdMonitor.copy(enabled = false, enabledTime = null)
        val updatedMonitor = updateMonitor(monitor = monitorV2)

        assertFalse("The monitor was not disabled", updatedMonitor.enabled)
    }

    fun `test update monitor with enable filter by`() {
        enableFilterBy()
        if (!isHttps()) {
            // if security is disabled and filter by is enabled, we can't create monitor
            // refer: `test create monitor with enable filter by`
            return
        }
        val monitor = randomQueryLevelMonitor(enabled = true)

        val createdMonitor = createMonitor(monitor = monitor)

        assertNotNull("The monitor was not created", createdMonitor)
        assertTrue("The monitor was not enabled", createdMonitor.enabled)

        val monitorV2 = createdMonitor.copy(enabled = false, enabledTime = null)
        val updatedMonitor = updateMonitor(monitor = monitorV2)

        assertFalse("The monitor was not disabled", updatedMonitor.enabled)
    }

    fun `test create monitor with enable filter by with a user have access and without role has no access`() {
        enableFilterBy()
        if (!isHttps()) {
            // if security is disabled and filter by is enabled, we can't create monitor
            // refer: `test create monitor with enable filter by`
            return
        }
        val monitor = randomQueryLevelMonitor(enabled = true)

        createUserWithRoles(
            user,
            listOf(ALERTING_FULL_ACCESS_ROLE, READALL_AND_MONITOR_ROLE),
            listOf(TEST_HR_BACKEND_ROLE, "role2"),
            false
        )

        val createdMonitor = createMonitorWithClient(userClient!!, monitor = monitor, listOf(TEST_HR_BACKEND_ROLE, "role2"))
        assertNotNull("The monitor was not created", createdMonitor)

        createUserRolesMapping(ALERTING_FULL_ACCESS_ROLE, arrayOf())
        createUserRolesMapping(READALL_AND_MONITOR_ROLE, arrayOf())

        // getUser should have access to the monitor
        val getUser = "getUser"
        createUserWithTestDataAndCustomRole(
            getUser,
            TEST_HR_INDEX,
            TEST_HR_ROLE,
            listOf("role2"),
            getClusterPermissionsFromCustomRole(ALERTING_GET_MONITOR_ACCESS)
        )
        val getUserClient = SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), getUser, password)
            .setSocketTimeout(60000)
            .setConnectionRequestTimeout(180000)
            .build()

        val getMonitorResponse = getUserClient?.makeRequest(
            "GET",
            "$ALERTING_BASE_URI/${createdMonitor.id}",
            null,
            BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
        )
        assertEquals("Get monitor failed", RestStatus.OK, getMonitorResponse?.restStatus())

        // Remove backend role and ensure no access is granted after
        patchUserBackendRoles(getUser, arrayOf("role1"))
        try {
            getUserClient?.makeRequest(
                "GET",
                "$ALERTING_BASE_URI/${createdMonitor.id}",
                null,
                BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
            )
            fail("Expected Forbidden exception")
        } catch (e: ResponseException) {
            assertEquals("Get monitor failed", RestStatus.FORBIDDEN.status, e.response.statusLine.statusCode)
        } finally {
            deleteRoleAndRoleMapping(TEST_HR_ROLE)
            deleteUser(getUser)
            getUserClient?.close()
        }
    }

    fun `test create monitor with enable filter by with no backend roles`() {
        enableFilterBy()
        if (!isHttps()) {
            // if security is disabled and filter by is enabled, we can't create monitor
            // refer: `test create monitor with enable filter by`
            return
        }
        val monitor = randomQueryLevelMonitor(enabled = true)

        createUserWithRoles(
            user,
            listOf(ALERTING_FULL_ACCESS_ROLE, READALL_AND_MONITOR_ROLE),
            listOf(TEST_HR_BACKEND_ROLE, "role2"),
            false
        )

        try {
            createMonitorWithClient(userClient!!, monitor = monitor, listOf())
            fail("Expected exception since a non-admin user is trying to create a monitor with no backend roles")
        } catch (e: ResponseException) {
            assertEquals("Create monitor failed", RestStatus.FORBIDDEN.status, e.response.statusLine.statusCode)
        } finally {
            createUserRolesMapping(ALERTING_FULL_ACCESS_ROLE, arrayOf())
            createUserRolesMapping(READALL_AND_MONITOR_ROLE, arrayOf())
        }
    }

    fun `test create monitor as admin with enable filter by with no backend roles`() {
        enableFilterBy()
        if (!isHttps()) {
            // if security is disabled and filter by is enabled, we can't create monitor
            // refer: `test create monitor with enable filter by`
            return
        }
        val monitor = randomQueryLevelMonitor(enabled = true)

        createUserWithRoles(
            user,
            listOf(ALERTING_FULL_ACCESS_ROLE, READALL_AND_MONITOR_ROLE),
            listOf(TEST_HR_BACKEND_ROLE, "role2"),
            false
        )

        val createdMonitor = createMonitorWithClient(client(), monitor = monitor, listOf())
        assertNotNull("The monitor was not created", createdMonitor)

        try {
            userClient?.makeRequest(
                "GET",
                "$ALERTING_BASE_URI/${createdMonitor.id}",
                null,
                BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
            )
            fail("Expected Forbidden exception")
        } catch (e: ResponseException) {
            assertEquals("Get monitor failed", RestStatus.FORBIDDEN.status, e.response.statusLine.statusCode)
        } finally {
            createUserRolesMapping(ALERTING_FULL_ACCESS_ROLE, arrayOf())
            createUserRolesMapping(READALL_AND_MONITOR_ROLE, arrayOf())
        }
    }

    fun `test create monitor with enable filter by with roles user has no access and throw exception`() {
        enableFilterBy()
        if (!isHttps()) {
            // if security is disabled and filter by is enabled, we can't create monitor
            // refer: `test create monitor with enable filter by`
            return
        }
        val monitor = randomQueryLevelMonitor(enabled = true)

        createUserWithRoles(
            user,
            listOf(ALERTING_FULL_ACCESS_ROLE, READALL_AND_MONITOR_ROLE),
            listOf(TEST_HR_BACKEND_ROLE, "role2"),
            false
        )

        try {
            createMonitorWithClient(userClient!!, monitor = monitor, listOf(TEST_HR_BACKEND_ROLE, "role1", "role2"))
            fail("Expected create monitor to fail as user does not have role1 backend role")
        } catch (e: ResponseException) {
            assertEquals("Create monitor failed", RestStatus.FORBIDDEN.status, e.response.statusLine.statusCode)
        } finally {
            createUserRolesMapping(ALERTING_FULL_ACCESS_ROLE, arrayOf())
            createUserRolesMapping(READALL_AND_MONITOR_ROLE, arrayOf())
        }
    }

    fun `test create monitor as admin with enable filter by with a user have access and without role has no access`() {
        enableFilterBy()
        if (!isHttps()) {
            // if security is disabled and filter by is enabled, we can't create monitor
            // refer: `test create monitor with enable filter by`
            return
        }
        val monitor = randomQueryLevelMonitor(enabled = true)

        val createdMonitor = createMonitorWithClient(client(), monitor = monitor, listOf(TEST_HR_BACKEND_ROLE, "role1", "role2"))
        assertNotNull("The monitor was not created", createdMonitor)

        // user should have access to the admin monitor
        createUserWithTestDataAndCustomRole(
            user,
            TEST_HR_INDEX,
            TEST_HR_ROLE,
            listOf(TEST_HR_BACKEND_ROLE),
            getClusterPermissionsFromCustomRole(ALERTING_GET_MONITOR_ACCESS)
        )

        val getMonitorResponse = userClient?.makeRequest(
            "GET",
            "$ALERTING_BASE_URI/${createdMonitor.id}",
            null,
            BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
        )
        assertEquals("Get monitor failed", RestStatus.OK, getMonitorResponse?.restStatus())

        // Remove good backend role and ensure no access is granted after
        patchUserBackendRoles(user, arrayOf("role5"))
        try {
            userClient?.makeRequest(
                "GET",
                "$ALERTING_BASE_URI/${createdMonitor.id}",
                null,
                BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
            )
            fail("Expected Forbidden exception")
        } catch (e: ResponseException) {
            assertEquals("Get monitor failed", RestStatus.FORBIDDEN.status, e.response.statusLine.statusCode)
        } finally {
            deleteRoleAndRoleMapping(TEST_HR_ROLE)
        }
    }

    fun `test update monitor with enable filter by with removing a permission`() {
        enableFilterBy()
        if (!isHttps()) {
            // if security is disabled and filter by is enabled, we can't create monitor
            // refer: `test create monitor with enable filter by`
            return
        }
        val monitor = randomQueryLevelMonitor(enabled = true)

        createUserWithRoles(
            user,
            listOf(ALERTING_FULL_ACCESS_ROLE, READALL_AND_MONITOR_ROLE),
            listOf(TEST_HR_BACKEND_ROLE, "role2"),
            false
        )

        val createdMonitor = createMonitorWithClient(userClient!!, monitor = monitor, listOf(TEST_HR_BACKEND_ROLE, "role2"))
        assertNotNull("The monitor was not created", createdMonitor)

        // getUser should have access to the monitor
        val getUser = "getUser"
        createUserWithTestDataAndCustomRole(
            getUser,
            TEST_HR_INDEX,
            TEST_HR_ROLE,
            listOf("role2"),
            getClusterPermissionsFromCustomRole(ALERTING_GET_MONITOR_ACCESS)
        )
        val getUserClient = SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), getUser, password)
            .setSocketTimeout(60000)
            .setConnectionRequestTimeout(180000)
            .build()

        val getMonitorResponse = getUserClient?.makeRequest(
            "GET",
            "$ALERTING_BASE_URI/${createdMonitor.id}",
            null,
            BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
        )
        assertEquals("Get monitor failed", RestStatus.OK, getMonitorResponse?.restStatus())

        // Remove backend role from monitor
        val updatedMonitor = updateMonitorWithClient(userClient!!, createdMonitor, listOf(TEST_HR_BACKEND_ROLE))

        // getUser should no longer have access
        try {
            getUserClient?.makeRequest(
                "GET",
                "$ALERTING_BASE_URI/${updatedMonitor.id}",
                null,
                BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
            )
            fail("Expected Forbidden exception")
        } catch (e: ResponseException) {
            assertEquals("Get monitor failed", RestStatus.FORBIDDEN.status, e.response.statusLine.statusCode)
        } finally {
            deleteRoleAndRoleMapping(TEST_HR_ROLE)
            deleteUser(getUser)
            getUserClient?.close()
            createUserRolesMapping(ALERTING_FULL_ACCESS_ROLE, arrayOf())
            createUserRolesMapping(READALL_AND_MONITOR_ROLE, arrayOf())
        }
    }

    fun `test update monitor with enable filter by with no backend roles`() {
        enableFilterBy()
        if (!isHttps()) {
            // if security is disabled and filter by is enabled, we can't create monitor
            // refer: `test create monitor with enable filter by`
            return
        }
        val monitor = randomQueryLevelMonitor(enabled = true)

        createUserWithRoles(
            user,
            listOf(ALERTING_FULL_ACCESS_ROLE, READALL_AND_MONITOR_ROLE),
            listOf(TEST_HR_BACKEND_ROLE, "role2"),
            false
        )

        val createdMonitor = createMonitorWithClient(userClient!!, monitor = monitor, listOf("role2"))
        assertNotNull("The monitor was not created", createdMonitor)

        try {
            updateMonitorWithClient(userClient!!, createdMonitor, listOf())
        } catch (e: ResponseException) {
            assertEquals("Update monitor failed", RestStatus.FORBIDDEN.status, e.response.statusLine.statusCode)
        } finally {
            createUserRolesMapping(ALERTING_FULL_ACCESS_ROLE, arrayOf())
            createUserRolesMapping(READALL_AND_MONITOR_ROLE, arrayOf())
        }
    }

    fun `test update monitor as admin with enable filter by with no backend roles`() {
        enableFilterBy()
        if (!isHttps()) {
            // if security is disabled and filter by is enabled, we can't create monitor
            // refer: `test create monitor with enable filter by`
            return
        }
        val monitor = randomQueryLevelMonitor(enabled = true)

        createUserWithRoles(
            user,
            listOf(ALERTING_FULL_ACCESS_ROLE, READALL_AND_MONITOR_ROLE),
            listOf(TEST_HR_BACKEND_ROLE, "role2"),
            false
        )

        val createdMonitor = createMonitorWithClient(client(), monitor = monitor, listOf(TEST_HR_BACKEND_ROLE))
        assertNotNull("The monitor was not created", createdMonitor)

        val getMonitorResponse = userClient?.makeRequest(
            "GET",
            "$ALERTING_BASE_URI/${createdMonitor.id}",
            null,
            BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
        )
        assertEquals("Get monitor failed", RestStatus.OK, getMonitorResponse?.restStatus())

        val updatedMonitor = updateMonitorWithClient(client(), createdMonitor, listOf())

        try {
            userClient?.makeRequest(
                "GET",
                "$ALERTING_BASE_URI/${updatedMonitor.id}",
                null,
                BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
            )
            fail("Expected Forbidden exception")
        } catch (e: ResponseException) {
            assertEquals("Get monitor failed", RestStatus.FORBIDDEN.status, e.response.statusLine.statusCode)
        } finally {
            createUserRolesMapping(ALERTING_FULL_ACCESS_ROLE, arrayOf())
            createUserRolesMapping(READALL_AND_MONITOR_ROLE, arrayOf())
        }
    }

    fun `test update monitor with enable filter by with updating with a permission user has no access to and throw exception`() {
        enableFilterBy()
        if (!isHttps()) {
            // if security is disabled and filter by is enabled, we can't create monitor
            // refer: `test create monitor with enable filter by`
            return
        }
        val monitor = randomQueryLevelMonitor(enabled = true)

        createUserWithRoles(
            user,
            listOf(ALERTING_FULL_ACCESS_ROLE, READALL_AND_MONITOR_ROLE),
            listOf(TEST_HR_BACKEND_ROLE, "role2"),
            false
        )

        val createdMonitor = createMonitorWithClient(userClient!!, monitor = monitor, listOf(TEST_HR_BACKEND_ROLE, "role2"))
        assertNotNull("The monitor was not created", createdMonitor)

        // getUser should have access to the monitor
        val getUser = "getUser"
        createUserWithTestDataAndCustomRole(
            getUser,
            TEST_HR_INDEX,
            TEST_HR_ROLE,
            listOf("role2"),
            getClusterPermissionsFromCustomRole(ALERTING_GET_MONITOR_ACCESS)
        )
        val getUserClient = SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), getUser, password)
            .setSocketTimeout(60000)
            .setConnectionRequestTimeout(180000)
            .build()

        val getMonitorResponse = getUserClient?.makeRequest(
            "GET",
            "$ALERTING_BASE_URI/${createdMonitor.id}",
            null,
            BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
        )
        assertEquals("Get monitor failed", RestStatus.OK, getMonitorResponse?.restStatus())

        try {
            updateMonitorWithClient(userClient!!, createdMonitor, listOf(TEST_HR_BACKEND_ROLE, "role1"))
            fail("Expected update monitor to fail as user doesn't have access to role1")
        } catch (e: ResponseException) {
            assertEquals("Update monitor failed", RestStatus.FORBIDDEN.status, e.response.statusLine.statusCode)
        } finally {
            deleteRoleAndRoleMapping(TEST_HR_ROLE)
            deleteUser(getUser)
            getUserClient?.close()
            createUserRolesMapping(ALERTING_FULL_ACCESS_ROLE, arrayOf())
            createUserRolesMapping(READALL_AND_MONITOR_ROLE, arrayOf())
        }
    }

    fun `test update monitor as another user with enable filter by with removing a permission and adding permission`() {
        enableFilterBy()
        if (!isHttps()) {
            // if security is disabled and filter by is enabled, we can't create monitor
            // refer: `test create monitor with enable filter by`
            return
        }
        val monitor = randomQueryLevelMonitor(enabled = true)

        createUserWithRoles(
            user,
            listOf(ALERTING_FULL_ACCESS_ROLE, READALL_AND_MONITOR_ROLE),
            listOf(TEST_HR_BACKEND_ROLE, "role2"),
            false
        )

        val createdMonitor = createMonitorWithClient(userClient!!, monitor = monitor, listOf(TEST_HR_BACKEND_ROLE))
        assertNotNull("The monitor was not created", createdMonitor)

        // Remove backend role from monitor with new user and add role5
        val updateUser = "updateUser"
        createUserWithRoles(
            updateUser,
            listOf(ALERTING_FULL_ACCESS_ROLE, READALL_AND_MONITOR_ROLE),
            listOf(TEST_HR_BACKEND_ROLE, "role5"),
            false
        )

        val updateUserClient = SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), updateUser, password)
            .setSocketTimeout(60000)
            .setConnectionRequestTimeout(180000)
            .build()
        val updatedMonitor = updateMonitorWithClient(updateUserClient, createdMonitor, listOf("role5"))

        // old user should no longer have access
        try {
            userClient?.makeRequest(
                "GET",
                "$ALERTING_BASE_URI/${updatedMonitor.id}",
                null,
                BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
            )
            fail("Expected Forbidden exception")
        } catch (e: ResponseException) {
            assertEquals("Get monitor failed", RestStatus.FORBIDDEN.status, e.response.statusLine.statusCode)
        } finally {
            deleteUser(updateUser)
            updateUserClient?.close()
            createUserRolesMapping(ALERTING_FULL_ACCESS_ROLE, arrayOf())
            createUserRolesMapping(READALL_AND_MONITOR_ROLE, arrayOf())
        }
    }

    fun `test update monitor as admin with enable filter by with removing a permission`() {
        enableFilterBy()
        if (!isHttps()) {
            // if security is disabled and filter by is enabled, we can't create monitor
            // refer: `test create monitor with enable filter by`
            return
        }
        val monitor = randomQueryLevelMonitor(enabled = true)

        createUserWithRoles(
            user,
            listOf(ALERTING_FULL_ACCESS_ROLE, READALL_AND_MONITOR_ROLE),
            listOf(TEST_HR_BACKEND_ROLE, "role2"),
            false
        )

        val createdMonitor = createMonitorWithClient(userClient!!, monitor = monitor, listOf(TEST_HR_BACKEND_ROLE, "role2"))
        assertNotNull("The monitor was not created", createdMonitor)

        // getUser should have access to the monitor
        val getUser = "getUser"
        createUserWithTestDataAndCustomRole(
            getUser,
            TEST_HR_INDEX,
            TEST_HR_ROLE,
            listOf("role1", "role2"),
            getClusterPermissionsFromCustomRole(ALERTING_GET_MONITOR_ACCESS)
        )
        val getUserClient = SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), getUser, password)
            .setSocketTimeout(60000)
            .setConnectionRequestTimeout(180000)
            .build()

        val getMonitorResponse = getUserClient?.makeRequest(
            "GET",
            "$ALERTING_BASE_URI/${createdMonitor.id}",
            null,
            BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
        )
        assertEquals("Get monitor failed", RestStatus.OK, getMonitorResponse?.restStatus())

        // Remove backend role from monitor
        val updatedMonitor = updateMonitorWithClient(client(), createdMonitor, listOf("role4"))

        // original user should no longer have access
        try {
            userClient?.makeRequest(
                "GET",
                "$ALERTING_BASE_URI/${updatedMonitor.id}",
                null,
                BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
            )
            fail("Expected Forbidden exception")
        } catch (e: ResponseException) {
            assertEquals("Get monitor failed", RestStatus.FORBIDDEN.status, e.response.statusLine.statusCode)
        } finally {
            createUserRolesMapping(ALERTING_FULL_ACCESS_ROLE, arrayOf())
            createUserRolesMapping(READALL_AND_MONITOR_ROLE, arrayOf())
        }

        // get user should no longer have access
        try {
            getUserClient?.makeRequest(
                "GET",
                "$ALERTING_BASE_URI/${updatedMonitor.id}",
                null,
                BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
            )
            fail("Expected Forbidden exception")
        } catch (e: ResponseException) {
            assertEquals("Get monitor failed", RestStatus.FORBIDDEN.status, e.response.statusLine.statusCode)
        } finally {
            deleteRoleAndRoleMapping(TEST_HR_ROLE)
            deleteUser(getUser)
            getUserClient?.close()
        }
    }

    fun `test delete monitor with disable filter by`() {
        disableFilterBy()
        val monitor = randomQueryLevelMonitor(enabled = true)

        val createdMonitor = createMonitor(monitor = monitor)

        assertNotNull("The monitor was not created", createdMonitor)
        assertTrue("The monitor was not enabled", createdMonitor.enabled)

        deleteMonitor(monitor = createdMonitor)

        val search = SearchSourceBuilder().query(QueryBuilders.termQuery("_id", createdMonitor.id)).toString()
        // search as "admin" - must get 0 docs
        val adminSearchResponse = client().makeRequest(
            "POST",
            "$ALERTING_BASE_URI/_search",
            emptyMap(),
            StringEntity(search, ContentType.APPLICATION_JSON)
        )
        assertEquals("Search monitor failed", RestStatus.OK, adminSearchResponse.restStatus())

        val adminHits = createParser(
            XContentType.JSON.xContent(),
            adminSearchResponse.entity.content
        ).map()["hits"]!! as Map<String, Map<String, Any>>
        val adminDocsFound = adminHits["total"]?.get("value")
        assertEquals("Monitor found during search", 0, adminDocsFound)
    }

    fun `test delete monitor with enable filter by`() {
        enableFilterBy()
        if (!isHttps()) {
            // if security is disabled and filter by is enabled, we can't create monitor
            // refer: `test create monitor with enable filter by`
            return
        }
        val monitor = randomQueryLevelMonitor(enabled = true)

        val createdMonitor = createMonitor(monitor = monitor)

        assertNotNull("The monitor was not created", createdMonitor)
        assertTrue("The monitor was not enabled", createdMonitor.enabled)

        deleteMonitor(monitor = createdMonitor)

        val search = SearchSourceBuilder().query(QueryBuilders.termQuery("_id", createdMonitor.id)).toString()
        // search as "admin" - must get 0 docs
        val adminSearchResponse = client().makeRequest(
            "POST",
            "$ALERTING_BASE_URI/_search",
            emptyMap(),
            StringEntity(search, ContentType.APPLICATION_JSON)
        )
        assertEquals("Search monitor failed", RestStatus.OK, adminSearchResponse.restStatus())

        val adminHits = createParser(
            XContentType.JSON.xContent(),
            adminSearchResponse.entity.content
        ).map()["hits"]!! as Map<String, Map<String, Any>>
        val adminDocsFound = adminHits["total"]?.get("value")
        assertEquals("Monitor found during search", 0, adminDocsFound)
    }

    /*
    TODO: https://github.com/opensearch-project/alerting/issues/300
     */
    fun `test query monitors with disable filter by`() {
        disableFilterBy()

        // creates monitor as "admin" user.
        val monitor = createRandomMonitor(true)
        val search = SearchSourceBuilder().query(QueryBuilders.termQuery("_id", monitor.id)).toString()

        // search as "admin" - must get 1 docs
        val adminSearchResponse = client().makeRequest(
            "POST",
            "$ALERTING_BASE_URI/_search",
            emptyMap(),
            StringEntity(search, ContentType.APPLICATION_JSON)
        )
        assertEquals("Search monitor failed", RestStatus.OK, adminSearchResponse.restStatus())
        assertEquals("Monitor not found during search", 1, getDocs(adminSearchResponse))

        // search as userOne without alerting roles - must return 403 Forbidden
        try {
            userClient?.makeRequest(
                "POST", "$ALERTING_BASE_URI/_search",
                emptyMap(),
                StringEntity(search, ContentType.APPLICATION_JSON)
            )
            fail("Expected 403 FORBIDDEN response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.FORBIDDEN, e.response.restStatus())
        }
        // add alerting roles and search as userOne - must return 1 docs
        createUserWithTestDataAndCustomRole(
            user,
            TEST_HR_INDEX,
            TEST_HR_ROLE,
            listOf(TEST_HR_BACKEND_ROLE),
            getClusterPermissionsFromCustomRole(ALERTING_SEARCH_MONITOR_ONLY_ACCESS)
        )
        try {
            val userOneSearchResponse = userClient?.makeRequest(
                "POST",
                "$ALERTING_BASE_URI/_search",
                emptyMap(),
                StringEntity(search, ContentType.APPLICATION_JSON)
            )
            assertEquals("Search monitor failed", RestStatus.OK, userOneSearchResponse?.restStatus())
            assertEquals("Monitor not found during search", 1, getDocs(userOneSearchResponse))
        } finally {
            deleteRoleAndRoleMapping(TEST_HR_ROLE)
        }
    }

    fun `test query monitors with enable filter by`() {

        enableFilterBy()

        // creates monitor as "admin" user.
        val monitor = createRandomMonitor(true)
        val search = SearchSourceBuilder().query(QueryBuilders.termQuery("_id", monitor.id)).toString()

        // search as "admin" - must get 1 docs
        val adminSearchResponse = client().makeRequest(
            "POST",
            "$ALERTING_BASE_URI/_search",
            emptyMap(),
            StringEntity(search, ContentType.APPLICATION_JSON)
        )
        assertEquals("Search monitor failed", RestStatus.OK, adminSearchResponse.restStatus())
        assertEquals("Monitor not found during search", 1, getDocs(adminSearchResponse))

        // search as userOne without alerting roles - must return 403 Forbidden
        try {
            userClient?.makeRequest(
                "POST", "$ALERTING_BASE_URI/_search",
                emptyMap(),
                StringEntity(search, ContentType.APPLICATION_JSON)
            )
            fail("Expected 403 FORBIDDEN response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.FORBIDDEN, e.response.restStatus())
        }

        // add alerting roles and search as userOne - must return 0 docs
        createUserRolesMapping(ALERTING_FULL_ACCESS_ROLE, arrayOf(user))
        try {
            val userOneSearchResponse = userClient?.makeRequest(
                "POST",
                "$ALERTING_BASE_URI/_search",
                emptyMap(),
                StringEntity(search, ContentType.APPLICATION_JSON)
            )
            assertEquals("Search monitor failed", RestStatus.OK, userOneSearchResponse?.restStatus())
            assertEquals("Monitor not found during search", 0, getDocs(userOneSearchResponse))
        } finally {
            deleteRoleMapping(ALERTING_FULL_ACCESS_ROLE)
        }
    }

    fun `test execute monitor with an user with execute monitor access`() {
        createUserWithTestDataAndCustomRole(
            user,
            TEST_HR_INDEX,
            TEST_HR_ROLE,
            listOf(TEST_HR_BACKEND_ROLE),
            getClusterPermissionsFromCustomRole(ALERTING_EXECUTE_MONITOR_ACCESS)
        )

        val monitor = createRandomMonitor(true)

        try {
            val executeMonitorResponse = userClient?.makeRequest(
                "POST",
                "$ALERTING_BASE_URI/${monitor.id}/_execute",
                mutableMapOf()
            )
            assertEquals("Get monitor failed", RestStatus.OK, executeMonitorResponse?.restStatus())
        } finally {
            deleteRoleAndRoleMapping(TEST_HR_ROLE)
        }
    }

    /*
    TODO: https://github.com/opensearch-project/alerting/issues/300
    */
    fun `test execute monitor with an user without execute monitor access`() {
        createUserWithTestDataAndCustomRole(
            user,
            TEST_HR_INDEX,
            TEST_HR_ROLE,
            listOf(TEST_HR_BACKEND_ROLE),
            getClusterPermissionsFromCustomRole(ALERTING_NO_ACCESS_ROLE)
        )

        val monitor = createRandomMonitor(true)

        try {

            userClient?.makeRequest(
                "POST",
                "$ALERTING_BASE_URI/${monitor.id}/_execute",
                mutableMapOf()
            )
            fail("Expected 403 Method FORBIDDEN response")
        } catch (e: ResponseException) {
            assertEquals("Get monitor failed", RestStatus.FORBIDDEN, e.response.restStatus())
        } finally {
            deleteRoleAndRoleMapping(TEST_HR_ROLE)
        }
    }

    fun `test delete monitor with an user with delete monitor access`() {
        createUserWithTestDataAndCustomRole(
            user,
            TEST_HR_INDEX,
            TEST_HR_ROLE,
            listOf(TEST_HR_BACKEND_ROLE),
            getClusterPermissionsFromCustomRole(ALERTING_DELETE_MONITOR_ACCESS)
        )

        val monitor = createRandomMonitor(true)
        val refresh = true

        try {
            val deleteMonitorResponse = userClient?.makeRequest(
                "DELETE",
                "$ALERTING_BASE_URI/${monitor.id}?refresh=$refresh",
                emptyMap(),
                monitor.toHttpEntity()
            )
            assertEquals("Get monitor failed", RestStatus.OK, deleteMonitorResponse?.restStatus())
        } finally {
            deleteRoleAndRoleMapping(TEST_HR_ROLE)
        }
    }

    /*
    TODO: https://github.com/opensearch-project/alerting/issues/300
     */
    fun `test delete monitor with an user without delete monitor access`() {
        createUserWithTestDataAndCustomRole(
            user,
            TEST_HR_INDEX,
            TEST_HR_ROLE,
            listOf(TEST_HR_BACKEND_ROLE),
            getClusterPermissionsFromCustomRole(ALERTING_NO_ACCESS_ROLE)
        )

        val monitor = createRandomMonitor(true)
        val refresh = true

        try {
            userClient?.makeRequest(
                "DELETE",
                "$ALERTING_BASE_URI/${monitor.id}?refresh=$refresh",
                emptyMap(),
                monitor.toHttpEntity()
            )
            fail("Expected 403 Method FORBIDDEN response")
        } catch (e: ResponseException) {
            assertEquals("Get monitor failed", RestStatus.FORBIDDEN, e.response.restStatus())
        } finally {
            deleteRoleAndRoleMapping(TEST_HR_ROLE)
        }
    }

    fun `test query all alerts in all states with disabled filter by`() {

        disableFilterBy()
        putAlertMappings()
        val monitor = createRandomMonitor(refresh = true)
        createAlert(randomAlert(monitor).copy(state = Alert.State.ACKNOWLEDGED))
        createAlert(randomAlert(monitor).copy(state = Alert.State.COMPLETED))
        createAlert(randomAlert(monitor).copy(state = Alert.State.ERROR))
        createAlert(randomAlert(monitor).copy(state = Alert.State.ACTIVE))
        randomAlert(monitor).copy(id = "foobar")

        val inputMap = HashMap<String, Any>()
        inputMap["missing"] = "_last"

        // search as "admin" - must get 4 docs
        val adminResponseMap = getAlerts(client(), inputMap).asMap()
        assertEquals(4, adminResponseMap["totalAlerts"])

        // search as userOne without alerting roles - must return 403 Forbidden
        try {
            getAlerts(userClient as RestClient, inputMap).asMap()
            fail("Expected 403 FORBIDDEN response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.FORBIDDEN, e.response.restStatus())
        }

        // add alerting roles and search as userOne - must return 0 docs
        createUserRolesMapping(ALERTING_FULL_ACCESS_ROLE, arrayOf(user))
        try {
            val responseMap = getAlerts(userClient as RestClient, inputMap).asMap()
            assertEquals(4, responseMap["totalAlerts"])
        } finally {
            deleteRoleMapping(ALERTING_FULL_ACCESS_ROLE)
        }
    }

    fun `test query all alerts in all states with filter by`() {

        enableFilterBy()
        putAlertMappings()
        val adminUser = User(ADMIN, listOf(ADMIN), listOf(ALL_ACCESS_ROLE), listOf())
        var monitor = createRandomMonitor(refresh = true).copy(user = adminUser)
        createAlert(randomAlert(monitor).copy(state = Alert.State.ACKNOWLEDGED))
        createAlert(randomAlert(monitor).copy(state = Alert.State.COMPLETED))
        createAlert(randomAlert(monitor).copy(state = Alert.State.ERROR))
        createAlert(randomAlert(monitor).copy(state = Alert.State.ACTIVE))
        randomAlert(monitor).copy(id = "foobar")

        val inputMap = HashMap<String, Any>()
        inputMap["missing"] = "_last"

        // search as "admin" - must get 4 docs
        val adminResponseMap = getAlerts(client(), inputMap).asMap()
        assertEquals(4, adminResponseMap["totalAlerts"])

        // search as userOne without alerting roles - must return 403 Forbidden
        try {
            getAlerts(userClient as RestClient, inputMap).asMap()
            fail("Expected 403 FORBIDDEN response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.FORBIDDEN, e.response.restStatus())
        }
        // add alerting roles and search as userOne - must return 0 docs
        createUserRolesMapping(ALERTING_FULL_ACCESS_ROLE, arrayOf(user))
        try {
            val responseMap = getAlerts(userClient as RestClient, inputMap).asMap()
            assertEquals(0, responseMap["totalAlerts"])
        } finally {
            deleteRoleMapping(ALERTING_FULL_ACCESS_ROLE)
        }
    }

    fun `test get alerts with an user with get alerts role`() {

        putAlertMappings()
        val ackAlertsUser = User(ADMIN, listOf(ADMIN), listOf(ALERTING_GET_ALERTS_ACCESS), listOf())
        var monitor = createRandomMonitor(refresh = true).copy(user = ackAlertsUser)
        createAlert(randomAlert(monitor).copy(state = Alert.State.ACKNOWLEDGED))
        createAlert(randomAlert(monitor).copy(state = Alert.State.COMPLETED))
        createAlert(randomAlert(monitor).copy(state = Alert.State.ERROR))
        createAlert(randomAlert(monitor).copy(state = Alert.State.ACTIVE))
        randomAlert(monitor).copy(id = "foobar")

        val inputMap = HashMap<String, Any>()
        inputMap["missing"] = "_last"

        // search as "admin" - must get 4 docs
        val adminResponseMap = getAlerts(client(), inputMap).asMap()
        assertEquals(4, adminResponseMap["totalAlerts"])

        // add alerting roles and search as userOne - must return 1 docs
        createUserWithTestDataAndCustomRole(
            user,
            TEST_HR_INDEX,
            TEST_HR_ROLE,
            listOf(TEST_HR_BACKEND_ROLE),
            getClusterPermissionsFromCustomRole(ALERTING_GET_ALERTS_ACCESS)
        )
        try {
            val responseMap = getAlerts(userClient as RestClient, inputMap).asMap()
            assertEquals(4, responseMap["totalAlerts"])
        } finally {
            deleteRoleAndRoleMapping(TEST_HR_ROLE)
        }
    }

    // Execute Monitor related security tests

    fun `test execute monitor with elevate permissions`() {

        val action = randomAction(template = randomTemplateScript("Hello {{ctx.monitor.name}}"), destinationId = createDestination().id)
        val inputs = listOf(
            SearchInput(
                indices = kotlin.collections.listOf(TEST_NON_HR_INDEX),
                query = SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
            )
        )
        val monitor = randomQueryLevelMonitor(
            triggers = listOf(randomQueryLevelTrigger(condition = ALWAYS_RUN, actions = listOf(action))),
            inputs = inputs
        )

        // Make sure the elevating the permissions fails execute.
        val adminUser = User(ADMIN, listOf(ADMIN), listOf(ALL_ACCESS_ROLE), listOf())
        var modifiedMonitor = monitor.copy(user = adminUser)
        createUserRolesMapping(ALERTING_FULL_ACCESS_ROLE, arrayOf(user))

        try {
            val response = executeMonitor(userClient as RestClient, modifiedMonitor, params = DRYRUN_MONITOR)
            val output = entityAsMap(response)
            val inputResults = output.stringMap("input_results")
            assertTrue("Missing monitor error message", (inputResults?.get("error") as String).isNotEmpty())
            assertTrue((inputResults.get("error") as String).contains("no permissions for [indices:data/read/search]"))
        } finally {
            deleteRoleMapping(ALERTING_FULL_ACCESS_ROLE)
        }
    }

    fun `test admin all access with enable filter by`() {

        enableFilterBy()
        createUserWithTestData(user, TEST_HR_INDEX, TEST_HR_ROLE, TEST_HR_BACKEND_ROLE)
        createUserRolesMapping(ALERTING_FULL_ACCESS_ROLE, arrayOf(user))
        try {
            // randomMonitor has a dummy user, api ignores the User passed as part of monitor, it picks user info from the logged-in user.
            val monitor = randomQueryLevelMonitor().copy(
                inputs = listOf(
                    SearchInput(
                        indices = listOf(TEST_HR_INDEX), query = SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
                    )
                )
            )

            val createResponse = userClient?.makeRequest("POST", ALERTING_BASE_URI, emptyMap(), monitor.toHttpEntity())
            assertEquals("Create monitor failed", RestStatus.CREATED, createResponse?.restStatus())
            val monitorJson = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE,
                createResponse?.entity?.content
            ).map()

            val search = SearchSourceBuilder().query(QueryBuilders.termQuery("_id", monitorJson["_id"])).toString()

            // search as "admin" - must get 1 docs
            val adminSearchResponse = client().makeRequest(
                "POST",
                "$ALERTING_BASE_URI/_search",
                emptyMap(),
                StringEntity(search, ContentType.APPLICATION_JSON)
            )
            assertEquals("Search monitor failed", RestStatus.OK, adminSearchResponse.restStatus())
            assertEquals("Monitor not found during search", 1, getDocs(adminSearchResponse))

            // get as "admin" - must get 1 docs
            val id: String = monitorJson["_id"] as String
            val adminGetResponse = client().makeRequest(
                "GET",
                "$ALERTING_BASE_URI/$id",
                emptyMap(),
                StringEntity(search, ContentType.APPLICATION_JSON)
            )
            assertEquals("Get monitor failed", RestStatus.OK, adminGetResponse.restStatus())

            // delete as "admin"
            val adminDeleteResponse = client().makeRequest(
                "DELETE",
                "$ALERTING_BASE_URI/$id",
                emptyMap(),
                StringEntity(search, ContentType.APPLICATION_JSON)
            )
            assertEquals("Delete monitor failed", RestStatus.OK, adminDeleteResponse.restStatus())
        } finally {
            deleteRoleAndRoleMapping(TEST_HR_ROLE)
            deleteRoleMapping(ALERTING_FULL_ACCESS_ROLE)
        }
    }

    /*
    TODO: https://github.com/opensearch-project/alerting/issues/300
     */
    fun `test execute query-level monitor with user having partial index permissions`() {
        createUser(user, arrayOf(TEST_HR_BACKEND_ROLE))
        createTestIndex(TEST_HR_INDEX)
        createIndexRoleWithDocLevelSecurity(
            TEST_HR_ROLE,
            TEST_HR_INDEX,
            TERM_DLS_QUERY,
            getClusterPermissionsFromCustomRole(ALERTING_INDEX_MONITOR_ACCESS)
        )
        createUserRolesMapping(TEST_HR_ROLE, arrayOf(user))

        // Add a doc that is accessible to the user
        indexDoc(
            TEST_HR_INDEX, "1",
            """
            {
              "test_field": "a",
              "accessible": true 
            }
            """.trimIndent()
        )

        // Add a second doc that is not accessible to the user
        indexDoc(
            TEST_HR_INDEX, "2",
            """
            {
              "test_field": "b",
              "accessible": false
            }
            """.trimIndent()
        )

        val input = SearchInput(indices = listOf(TEST_HR_INDEX), query = SearchSourceBuilder().query(QueryBuilders.matchAllQuery()))
        val triggerScript = """
            // make sure there is exactly one hit
            return ctx.results[0].hits.hits.size() == 1 
        """.trimIndent()

        val trigger = randomQueryLevelTrigger(condition = Script(triggerScript)).copy(actions = listOf())
        val monitor = createMonitorWithClient(
            userClient!!,
            randomQueryLevelMonitor(inputs = listOf(input), triggers = listOf(trigger))
        )

        try {
            executeMonitor(monitor.id)
            val alerts = searchAlerts(monitor)
            assertEquals("Incorrect number of alerts", 1, alerts.size)
        } finally {
            deleteRoleAndRoleMapping(TEST_HR_ROLE)
        }
    }

    fun `test execute bucket-level monitor with user having partial index permissions`() {
        createUser(user, arrayOf(TEST_HR_BACKEND_ROLE))
        createTestIndex(TEST_HR_INDEX)
        createIndexRoleWithDocLevelSecurity(
            TEST_HR_ROLE,
            TEST_HR_INDEX,
            TERM_DLS_QUERY,
            getClusterPermissionsFromCustomRole(ALERTING_INDEX_MONITOR_ACCESS)
        )
        createUserRolesMapping(TEST_HR_ROLE, arrayOf(user))

        // Add a doc that is accessible to the user
        indexDoc(
            TEST_HR_INDEX, "1",
            """
            {
              "test_field": "a",
              "accessible": true
            }
            """.trimIndent()
        )

        // Add a second doc that is not accessible to the user
        indexDoc(
            TEST_HR_INDEX, "2",
            """
            {
              "test_field": "b",
              "accessible": false
            }
            """.trimIndent()
        )

        val compositeSources = listOf(
            TermsValuesSourceBuilder("test_field").field("test_field")
        )
        val compositeAgg = CompositeAggregationBuilder("composite_agg", compositeSources)
        val input = SearchInput(
            indices = listOf(TEST_HR_INDEX),
            query = SearchSourceBuilder().size(0).query(QueryBuilders.matchAllQuery()).aggregation(compositeAgg)
        )
        val triggerScript = """
            params.docCount > 0
        """.trimIndent()

        var trigger = randomBucketLevelTrigger()
        trigger = trigger.copy(
            bucketSelector = BucketSelectorExtAggregationBuilder(
                name = trigger.id,
                bucketsPathsMap = mapOf("docCount" to "_count"),
                script = Script(triggerScript),
                parentBucketPath = "composite_agg",
                filter = null
            ),
            actions = listOf()
        )
        val monitor = createMonitorWithClient(
            userClient!!,
            randomBucketLevelMonitor(inputs = listOf(input), enabled = false, triggers = listOf(trigger))
        )

        try {
            executeMonitor(monitor.id)
            val alerts = searchAlerts(monitor)
            assertEquals("Incorrect number of alerts", 1, alerts.size)
        } finally {
            deleteRoleAndRoleMapping(TEST_HR_ROLE)
        }
    }

    /**
     * We want to verify that user roles/permissions do not affect clean up of monitors during partial monitor creation failure
     */
    fun `test create monitor failure clean up with a user without delete monitor access`() {
        enableFilterBy()
        createUser(user, listOf(TEST_HR_BACKEND_ROLE, "role2").toTypedArray())
        createTestIndex(TEST_HR_INDEX)
        createCustomIndexRole(
            ALERTING_INDEX_MONITOR_ACCESS,
            TEST_HR_INDEX,
            getClusterPermissionsFromCustomRole(ALERTING_INDEX_MONITOR_ACCESS)
        )
        createUserWithRoles(
            user,
            listOf(ALERTING_INDEX_MONITOR_ACCESS, READALL_AND_MONITOR_ROLE),
            listOf(TEST_HR_BACKEND_ROLE, "role2"),
            false
        )
        val docLevelQueryIndex = ".opensearch-alerting-queries-000001"
        createIndex(
            docLevelQueryIndex, Settings.EMPTY,
            """
                 "properties" : {
                  "query": {
                              "type": "percolator_ext"
                            },
                            "monitor_id": {
                              "type": "text"
                            },
                            "index": {
                              "type": "text"
                            }
                }
                }
            """.trimIndent(),
            ".opensearch-alerting-queries"
        )
        closeIndex(docLevelQueryIndex) // close index to simulate doc level query indexing failure
        try {
            val monitor = randomDocumentLevelMonitor(
                withMetadata = false,
                triggers = listOf(),
                inputs = listOf(DocLevelMonitorInput("description", listOf(TEST_HR_INDEX), emptyList()))
            )
            userClient?.makeRequest("POST", ALERTING_BASE_URI, emptyMap(), monitor.toHttpEntity())
            fail("Monitor creation should have failed due to error in indexing doc level queries")
        } catch (e: ResponseException) {
            val search = SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).size(10).toString()
            val searchResponse = client().makeRequest(
                "GET", "$ALERTING_BASE_URI/_search",
                emptyMap(),
                StringEntity(search, ContentType.APPLICATION_JSON)
            )
            val xcp = createParser(XContentType.JSON.xContent(), searchResponse.entity.content)
            val hits = xcp.map()["hits"]!! as Map<String, Map<String, Any>>
            val numberDocsFound = hits["total"]?.get("value")
            assertEquals("Monitors found. Clean up unsuccessful", 0, numberDocsFound)
        } finally {
            deleteRoleAndRoleMapping(ALERTING_INDEX_MONITOR_ACCESS)
        }
    }
}

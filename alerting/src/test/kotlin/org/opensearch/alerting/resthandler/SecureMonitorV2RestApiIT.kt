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
import org.opensearch.alerting.ALERTING_FULL_ACCESS_ROLE
import org.opensearch.alerting.ALL_ACCESS_ROLE
import org.opensearch.alerting.AlertingPlugin.Companion.MONITOR_V2_BASE_URI
import org.opensearch.alerting.AlertingRestTestCase
import org.opensearch.alerting.PPL_FULL_ACCESS_ROLE
import org.opensearch.alerting.ROLE_TO_PERMISSION_MAPPING
import org.opensearch.alerting.TEST_INDEX_MAPPINGS
import org.opensearch.alerting.TEST_INDEX_NAME
import org.opensearch.alerting.core.settings.AlertingV2Settings
import org.opensearch.alerting.makeRequest
import org.opensearch.alerting.modelv2.PPLSQLMonitor
import org.opensearch.alerting.modelv2.PPLSQLTrigger.ConditionType
import org.opensearch.alerting.modelv2.PPLSQLTrigger.NumResultsCondition
import org.opensearch.alerting.modelv2.PPLSQLTrigger.TriggerMode
import org.opensearch.alerting.randomPPLMonitor
import org.opensearch.alerting.randomPPLTrigger
import org.opensearch.client.ResponseException
import org.opensearch.client.RestClient
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.model.IntervalSchedule
import org.opensearch.commons.rest.SecureRestClientBuilder
import org.opensearch.core.rest.RestStatus
import org.opensearch.index.query.QueryBuilders
import org.opensearch.search.builder.SearchSourceBuilder
import java.time.temporal.ChronoUnit.MINUTES

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
        client().updateSettings(AlertingV2Settings.ALERTING_V2_ENABLED.key, "true")
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

    fun `test create monitor as user without alerting access fails`() {
        if (!isHttps()) {
            return
        }

        val pplMonitorConfig = randomPPLMonitor()

        createUserWithTestDataAndCustomRole(
            user,
            TEST_INDEX_NAME,
            "custom_role",
            listOf(),
            null
        )

        try {
            createMonitorV2WithClient(
                userClient!!,
                monitorV2 = pplMonitorConfig
            )
            fail("Expected create monitor to fail as user does not have permissions to call alerting APIs")
        } catch (e: ResponseException) {
            assertEquals("Unexpected error status", RestStatus.FORBIDDEN.status, e.response.statusLine.statusCode)
        }

        ensureNumMonitorV2s(0)
    }

    fun `test create monitor that queries index user doesn't have access to fails`() {
        if (!isHttps()) {
            return
        }

        createIndex("some_index", Settings.EMPTY)

        val pplMonitorConfig = randomPPLMonitor(
            query = "source = some_index | head 10"
        )

        createUserWithTestDataAndCustomRole(
            user,
            "other_index",
            "custom_role",
            listOf(),
            getClusterPermissionsFromCustomRole(ALL_ACCESS_ROLE)
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

    fun `test update monitor that queries index user doesn't have access to fails`() {
        if (!isHttps()) {
            return
        }

        // RBAC is out of scope for this test, so give all users and requests the same one
        val backendRole = "backend_role_a"

        val pplMonitorConfig = randomPPLMonitor()

        // first create the monitor with a user that has access to all indices
        // (the FULL_ACCESS_ROLEs include full index permissions)
        createUserWithRoles(
            user,
            listOf(ALERTING_FULL_ACCESS_ROLE, PPL_FULL_ACCESS_ROLE),
            listOf(backendRole),
            false
        )

        // this function automatically creates index TEST_INDEX_NAME, then a monitor that queries it
        val pplMonitor = createMonitorV2WithClient(userClient!!, pplMonitorConfig, listOf(backendRole))

        /*
        user: String,
        index: String,
        role: String,
        backendRoles: List<String>,
        clusterPermissions: String?,
         */

        // prepare a user that has full access to all cluster actions,
        // but only access to a specific unrelated index
        val noIndicesUser = "noIndicesUser"
        createUserWithTestDataAndCustomRole(
            noIndicesUser,
            "unrelated_index",
            "unrelated_role",
            listOf(backendRole),
            listOf(ROLE_TO_PERMISSION_MAPPING[ALL_ACCESS_ROLE])
        )

        val noIndicesUserClient = SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), noIndicesUser, password)
            .setSocketTimeout(60000)
            .setConnectionRequestTimeout(180000)
            .build()

        // update some field that isn't the PPL query and the index it's querying
        val newMonitor = pplMonitorConfig.makeCopy(name = "some_random_name")

        try {
            // noIndicesUser, who only has access to index unrelated_index, should be blocked
            // from updating a monitor that queries index TEST_INDEX_NAME because noIndicesUser
            // has no access to TEST_INDEX_NAME
            noIndicesUserClient!!.makeRequest(
                "PUT",
                "$MONITOR_V2_BASE_URI/${pplMonitor.id}",
                newMonitor.toHttpEntity(),
                BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
            )
            fail("Expected update monitor to fail as user does not have permissions to index that monitor queries")
        } catch (e: ResponseException) {
            assertEquals("Unexpected error status", RestStatus.BAD_REQUEST.status, e.response.statusLine.statusCode)
        }

        // cleanup
        noIndicesUserClient.close()
    }

    fun `test RBAC create monitor with backend roles user has access to succeeds`() {
        enableFilterBy()
        if (!isHttps()) {
            return
        }

        val pplMonitorConfig = randomPPLMonitor(enabled = true)

        createUserWithRoles(
            user,
            listOf(ALERTING_FULL_ACCESS_ROLE, PPL_FULL_ACCESS_ROLE),
            listOf("backend_role_a", "backend_role_b"),
            false
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

        createUserWithRoles(
            user,
            listOf(ALERTING_FULL_ACCESS_ROLE, PPL_FULL_ACCESS_ROLE),
            listOf("backend_role_a", "backend_role_b"),
            false
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

        createUserWithRoles(
            user,
            listOf(ALERTING_FULL_ACCESS_ROLE, PPL_FULL_ACCESS_ROLE),
            listOf("backend_role_a", "backend_role_b"),
            false
        )

        val pplMonitor = createMonitorV2WithClient(userClient!!, pplMonitorConfig, listOf("backend_role_a", "backend_role_b"))

        // getUser should have access to the monitor above created by user
        val updateUser = "updateUser"

        createUserWithRoles(
            updateUser,
            listOf(ALERTING_FULL_ACCESS_ROLE, PPL_FULL_ACCESS_ROLE),
            listOf("backend_role_a"),
            true
        )

        val getUserClient = SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), updateUser, password)
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

        createUserWithRoles(
            user,
            listOf(ALERTING_FULL_ACCESS_ROLE, PPL_FULL_ACCESS_ROLE),
            listOf("backend_role_a", "backend_role_b"),
            false
        )

        val pplMonitor = createMonitorV2WithClient(userClient!!, pplMonitorConfig, listOf("backend_role_a", "backend_role_b"))

        // updateUser should have access to the monitor above created by user
        val updateUser = "updateUser"

        createUserWithRoles(
            updateUser,
            listOf(ALERTING_FULL_ACCESS_ROLE, PPL_FULL_ACCESS_ROLE),
            listOf("backend_role_c"),
            true
        )

        val getUserClient = SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), updateUser, password)
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

        createUserWithRoles(
            user,
            listOf(ALERTING_FULL_ACCESS_ROLE, PPL_FULL_ACCESS_ROLE),
            listOf("backend_role_a", "backend_role_b"),
            false
        )

        val pplMonitor = createMonitorV2WithClient(userClient!!, pplMonitorConfig, listOf("backend_role_a", "backend_role_b"))

        // getUser should have access to the monitor above created by user
        val getUser = "getUser"

        createUserWithRoles(
            getUser,
            listOf(ALERTING_FULL_ACCESS_ROLE, PPL_FULL_ACCESS_ROLE),
            listOf("backend_role_a"),
            true
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

        createUserWithRoles(
            user,
            listOf(ALERTING_FULL_ACCESS_ROLE, PPL_FULL_ACCESS_ROLE),
            listOf("backend_role_a", "backend_role_b"),
            false
        )

        val pplMonitor = createMonitorV2WithClient(userClient!!, pplMonitorConfig, listOf("backend_role_a", "backend_role_b"))

        // getUser should not have access to the monitor above created by user
        val getUser = "getUser"

        createUserWithRoles(
            getUser,
            listOf(ALERTING_FULL_ACCESS_ROLE, PPL_FULL_ACCESS_ROLE),
            listOf("backend_role_c"),
            true
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

        createUserWithRoles(
            user,
            listOf(ALERTING_FULL_ACCESS_ROLE, PPL_FULL_ACCESS_ROLE),
            listOf("backend_role_a", "backend_role_b"),
            false
        )

        createMonitorV2WithClient(userClient!!, pplMonitorConfig, listOf("backend_role_a", "backend_role_b"))

        // getUser should have access to the monitor above created by user
        val searchUser = "searchUser"

        createUserWithRoles(
            searchUser,
            listOf(ALERTING_FULL_ACCESS_ROLE, PPL_FULL_ACCESS_ROLE),
            listOf("backend_role_a"),
            true
        )

        val searchUserClient = SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), searchUser, password)
            .setSocketTimeout(60000)
            .setConnectionRequestTimeout(180000)
            .build()

        val search = SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).toString()
        val searchMonitorResponse = searchUserClient!!.makeRequest(
            "POST",
            "$MONITOR_V2_BASE_URI/_search",
            StringEntity(search, ContentType.APPLICATION_JSON),
            BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
        )
        assertEquals("Search monitorV2 failed", RestStatus.OK, searchMonitorResponse.restStatus())

        createParser(XContentType.JSON.xContent(), searchMonitorResponse.entity.content).use { xcp ->
            val hits = xcp.map()["hits"]!! as Map<String, Map<String, Any>>
            logger.info("hits: $hits")
            val numberDocsFound = hits["total"]?.get("value")
            assertEquals("Created PPL Monitor should be visible but was not", 1, numberDocsFound)
        }

        // cleanup
        searchUserClient.close()
    }

    fun `test RBAC search monitorV2 as user without correct backend roles returns no results`() {
        enableFilterBy()
        if (!isHttps()) {
            return
        }
        val pplMonitorConfig = randomPPLMonitor(enabled = true)

        createUserWithRoles(
            user,
            listOf(ALERTING_FULL_ACCESS_ROLE, PPL_FULL_ACCESS_ROLE),
            listOf("backend_role_a", "backend_role_b"),
            false
        )

        createMonitorV2WithClient(userClient!!, pplMonitorConfig, listOf("backend_role_a", "backend_role_b"))

        // getUser should have access to the monitor above created by user
        val searchUser = "searchUser"

        createUserWithRoles(
            searchUser,
            listOf(ALERTING_FULL_ACCESS_ROLE, PPL_FULL_ACCESS_ROLE),
            listOf("backend_role_c"),
            true
        )

        val searchUserClient = SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), searchUser, password)
            .setSocketTimeout(60000)
            .setConnectionRequestTimeout(180000)
            .build()

        val search = SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).toString()
        val searchMonitorResponse = searchUserClient!!.makeRequest(
            "POST",
            "$MONITOR_V2_BASE_URI/_search",
            StringEntity(search, ContentType.APPLICATION_JSON),
            BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
        )
        assertEquals("Search monitorV2 failed", RestStatus.OK, searchMonitorResponse.restStatus())

        createParser(XContentType.JSON.xContent(), searchMonitorResponse.entity.content).use { xcp ->
            val hits = xcp.map()["hits"]!! as Map<String, Map<String, Any>>
            val numberDocsFound = hits["total"]?.get("value")
            assertEquals("Created PPL Monitor should be visible but was not", 0, numberDocsFound)
        }

        // cleanup
        searchUserClient.close()
    }

    fun `test RBAC execute monitorV2 as user with correct backend roles succeeds`() {
        enableFilterBy()
        if (!isHttps()) {
            return
        }
        val pplMonitorConfig = randomPPLMonitor(enabled = true)

        createUserWithRoles(
            user,
            listOf(ALERTING_FULL_ACCESS_ROLE, PPL_FULL_ACCESS_ROLE),
            listOf("backend_role_a", "backend_role_b"),
            false
        )

        val pplMonitor = createMonitorV2WithClient(userClient!!, pplMonitorConfig, listOf("backend_role_a", "backend_role_b"))

        // getUser should have access to the monitor above created by user
        val executeUser = "executeUser"

        createUserWithRoles(
            executeUser,
            listOf(ALERTING_FULL_ACCESS_ROLE, PPL_FULL_ACCESS_ROLE),
            listOf("backend_role_a"),
            true
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

        createUserWithRoles(
            user,
            listOf(ALERTING_FULL_ACCESS_ROLE, PPL_FULL_ACCESS_ROLE),
            listOf("backend_role_a", "backend_role_b"),
            false
        )

        val pplMonitor = createMonitorV2WithClient(userClient!!, pplMonitorConfig, listOf("backend_role_a", "backend_role_b"))

        // getUser should not have access to the monitor above created by user
        val executeUser = "executeUser"

        createUserWithRoles(
            executeUser,
            listOf(ALERTING_FULL_ACCESS_ROLE, PPL_FULL_ACCESS_ROLE),
            listOf("backend_role_c"),
            true
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

    fun `test RBAC get alerts v2 as user with correct backend roles succeeds`() {
        enableFilterBy()
        if (!isHttps()) {
            return
        }

        createIndex(TEST_INDEX_NAME, Settings.EMPTY, TEST_INDEX_MAPPINGS)
        indexDocFromSomeTimeAgo(2, MINUTES, "abc", 5)

        val pplMonitorConfig = createRandomPPLMonitor(
            randomPPLMonitor(
                enabled = true,
                schedule = IntervalSchedule(interval = 1, unit = MINUTES),
                lookBackWindow = null,
                triggers = listOf(
                    randomPPLTrigger(
                        throttleDuration = null,
                        expireDuration = 5,
                        mode = TriggerMode.RESULT_SET,
                        conditionType = ConditionType.NUMBER_OF_RESULTS,
                        numResultsCondition = NumResultsCondition.GREATER_THAN,
                        numResultsValue = 0L,
                        customCondition = null
                    )
                ),
                query = "source = $TEST_INDEX_NAME | head 10"
            )
        )

        createUserWithRoles(
            user,
            listOf(ALERTING_FULL_ACCESS_ROLE, PPL_FULL_ACCESS_ROLE),
            listOf("backend_role_a", "backend_role_b"),
            false
        )

        val pplMonitor = createMonitorV2WithClient(
            userClient!!,
            pplMonitorConfig,
            null
        ) as PPLSQLMonitor

        val executeResponse = executeMonitorV2(pplMonitor.id)
        val triggered = isTriggered(pplMonitor, executeResponse)
        assertTrue(triggered)

        // TODO: creating this user overrides the ALERTING_FULL_ACCESS mapping and displaces "user"
        // TODO: above, even though passing in isExistingRole = true should trigger an update
        // TODO: role mappings call. doesn't block the test because "user" isn't used for the
        // TODO: rest of the test, but this could lead to unexpected behavior for future test writers
        // the get alerts user should be able to see the alerts
        val getAlertsUser = "getAlertsUser"
        createUserWithRoles(
            getAlertsUser,
            listOf(ALERTING_FULL_ACCESS_ROLE, PPL_FULL_ACCESS_ROLE),
            listOf("backend_role_a"),
            true
        )

        val getAlertsUserClient = SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), getAlertsUser, password)
            .setSocketTimeout(60000)
            .setConnectionRequestTimeout(180000)
            .build()

        val getAlertsResponse = getAlertsUserClient!!.makeRequest(
            "GET",
            "$MONITOR_V2_BASE_URI/alerts",
            null,
            BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
        )
        assertEquals("Get alerts v2 failed", RestStatus.OK, getAlertsResponse.restStatus())

        val alertsGenerated = numAlerts(getAlertsResponse) > 0
        assert(alertsGenerated)

        // cleanup
        getAlertsUserClient.close()
    }

    fun `test RBAC get alerts v2 as user without correct backend roles fails`() {
        enableFilterBy()
        if (!isHttps()) {
            return
        }

        createIndex(TEST_INDEX_NAME, Settings.EMPTY, TEST_INDEX_MAPPINGS)
        indexDocFromSomeTimeAgo(2, MINUTES, "abc", 5)

        val pplMonitorConfig = createRandomPPLMonitor(
            randomPPLMonitor(
                enabled = true,
                schedule = IntervalSchedule(interval = 1, unit = MINUTES),
                lookBackWindow = null,
                triggers = listOf(
                    randomPPLTrigger(
                        throttleDuration = null,
                        expireDuration = 5,
                        mode = TriggerMode.RESULT_SET,
                        conditionType = ConditionType.NUMBER_OF_RESULTS,
                        numResultsCondition = NumResultsCondition.GREATER_THAN,
                        numResultsValue = 0L,
                        customCondition = null
                    )
                ),
                query = "source = $TEST_INDEX_NAME | head 10"
            )
        )

        createUserWithRoles(
            user,
            listOf(ALERTING_FULL_ACCESS_ROLE, PPL_FULL_ACCESS_ROLE),
            listOf("backend_role_a", "backend_role_b"),
            false
        )

        val pplMonitor = createMonitorV2WithClient(
            userClient!!,
            pplMonitorConfig,
            null
        ) as PPLSQLMonitor

        val executeResponse = executeMonitorV2(pplMonitor.id)
        val triggered = isTriggered(pplMonitor, executeResponse)
        assertTrue(triggered)

        // the get alerts user should be able to see the alerts
        val getAlertsUser = "getAlertsUser"
        createUserWithRoles(
            getAlertsUser,
            listOf(ALERTING_FULL_ACCESS_ROLE, PPL_FULL_ACCESS_ROLE),
            listOf("backend_role_c"),
            true
        )

        val getAlertsUserClient = SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), getAlertsUser, password)
            .setSocketTimeout(60000)
            .setConnectionRequestTimeout(180000)
            .build()

        val getAlertsResponse = getAlertsUserClient!!.makeRequest(
            "GET",
            "$MONITOR_V2_BASE_URI/alerts",
            null,
            BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
        )
        assertEquals("Get alerts v2 failed", RestStatus.OK, getAlertsResponse.restStatus())

        val alertsGenerated = numAlerts(getAlertsResponse) > 0
        assert(!alertsGenerated)

        // cleanup
        getAlertsUserClient.close()
    }

    fun `test RBAC delete monitorV2 as user with correct backend roles succeeds`() {
        enableFilterBy()
        if (!isHttps()) {
            return
        }
        val pplMonitorConfig = randomPPLMonitor(enabled = true)

        createUserWithRoles(
            user,
            listOf(ALERTING_FULL_ACCESS_ROLE, PPL_FULL_ACCESS_ROLE),
            listOf("backend_role_a", "backend_role_b"),
            false
        )

        val pplMonitor = createMonitorV2WithClient(userClient!!, pplMonitorConfig, listOf("backend_role_a", "backend_role_b"))

        // getUser should have access to the monitor above created by user
        val deleteUser = "deleteUser"

        createUserWithRoles(
            deleteUser,
            listOf(ALERTING_FULL_ACCESS_ROLE, PPL_FULL_ACCESS_ROLE),
            listOf("backend_role_a"),
            true
        )

        val deleteUserClient = SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), deleteUser, password)
            .setSocketTimeout(60000)
            .setConnectionRequestTimeout(180000)
            .build()

        val getMonitorResponse = deleteUserClient!!.makeRequest(
            "DELETE",
            "$MONITOR_V2_BASE_URI/${pplMonitor.id}",
            null,
            BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
        )
        assertEquals("Get monitorV2 failed", RestStatus.OK, getMonitorResponse.restStatus())

        ensureNumMonitorV2s(0)

        // cleanup
        deleteUserClient.close()
    }

    fun `test RBAC delete monitorV2 as user without correct backend roles fails`() {
        enableFilterBy()
        if (!isHttps()) {
            return
        }
        val pplMonitorConfig = randomPPLMonitor(enabled = true)

        createUserWithRoles(
            user,
            listOf(ALERTING_FULL_ACCESS_ROLE, PPL_FULL_ACCESS_ROLE),
            listOf("backend_role_a", "backend_role_b"),
            false
        )

        val pplMonitor = createMonitorV2WithClient(userClient!!, pplMonitorConfig, listOf("backend_role_a", "backend_role_b"))

        // getUser should not have access to the monitor above created by user
        val deleteUser = "deleteUser"

        createUserWithRoles(
            deleteUser,
            listOf(ALERTING_FULL_ACCESS_ROLE, PPL_FULL_ACCESS_ROLE),
            listOf("backend_role_c"),
            true
        )

        val deleteUserClient = SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), deleteUser, password)
            .setSocketTimeout(60000)
            .setConnectionRequestTimeout(180000)
            .build()

        try {
            deleteUserClient!!.makeRequest(
                "DELETE",
                "$MONITOR_V2_BASE_URI/${pplMonitor.id}",
                null,
                BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
            )
            fail("Expected Forbidden exception")
        } catch (e: ResponseException) {
            assertEquals("Unexpected delete monitor status", RestStatus.FORBIDDEN.status, e.response.statusLine.statusCode)
        } finally {
            deleteUserClient?.close()
        }

        ensureNumMonitorV2s(1)
    }
}

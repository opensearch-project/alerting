/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.apache.http.HttpHeaders
import org.apache.http.entity.ContentType
import org.apache.http.message.BasicHeader
import org.apache.http.nio.entity.NStringEntity
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.opensearch.alerting.ALERTING_BASE_URI
import org.opensearch.alerting.ALERTING_DELETE_WORKFLOW_ACCESS
import org.opensearch.alerting.ALERTING_EXECUTE_WORKFLOW_ACCESS
import org.opensearch.alerting.ALERTING_FULL_ACCESS_ROLE
import org.opensearch.alerting.ALERTING_GET_WORKFLOW_ACCESS
import org.opensearch.alerting.ALERTING_INDEX_MONITOR_ACCESS
import org.opensearch.alerting.ALERTING_INDEX_WORKFLOW_ACCESS
import org.opensearch.alerting.ALERTING_NO_ACCESS_ROLE
import org.opensearch.alerting.ALERTING_READ_ONLY_ACCESS
import org.opensearch.alerting.ALWAYS_RUN
import org.opensearch.alerting.AlertingRestTestCase
import org.opensearch.alerting.READALL_AND_MONITOR_ROLE
import org.opensearch.alerting.TERM_DLS_QUERY
import org.opensearch.alerting.TEST_HR_BACKEND_ROLE
import org.opensearch.alerting.TEST_HR_INDEX
import org.opensearch.alerting.TEST_HR_ROLE
import org.opensearch.alerting.TEST_NON_HR_INDEX
import org.opensearch.alerting.WORKFLOW_ALERTING_BASE_URI
import org.opensearch.alerting.assertUserNull
import org.opensearch.alerting.makeRequest
import org.opensearch.alerting.randomBucketLevelMonitor
import org.opensearch.alerting.randomBucketLevelTrigger
import org.opensearch.alerting.randomDocLevelQuery
import org.opensearch.alerting.randomDocumentLevelMonitor
import org.opensearch.alerting.randomDocumentLevelTrigger
import org.opensearch.alerting.randomQueryLevelMonitor
import org.opensearch.alerting.randomWorkflow
import org.opensearch.client.Response
import org.opensearch.client.ResponseException
import org.opensearch.client.RestClient
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentType
import org.opensearch.common.xcontent.json.JsonXContent
import org.opensearch.commons.alerting.aggregation.bucketselectorext.BucketSelectorExtAggregationBuilder
import org.opensearch.commons.alerting.model.DataSources
import org.opensearch.commons.alerting.model.DocLevelMonitorInput
import org.opensearch.commons.alerting.model.DocLevelQuery
import org.opensearch.commons.alerting.model.SearchInput
import org.opensearch.commons.rest.SecureRestClientBuilder
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.index.query.QueryBuilders
import org.opensearch.rest.RestStatus
import org.opensearch.script.Script
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder
import org.opensearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.test.junit.annotations.TestLogging
import java.time.Instant

@TestLogging("level:DEBUG", reason = "Debug for tests.")
@Suppress("UNCHECKED_CAST")
class SecureWorkflowRestApiIT : AlertingRestTestCase() {

    companion object {

        @BeforeClass
        @JvmStatic
        fun setup() {
            // things to execute once and keep around for the class
            org.junit.Assume.assumeTrue(System.getProperty("security", "false")!!.toBoolean())
        }
    }

    val user = "userD"
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

    // Create Workflow related security tests
    fun `test create workflow with an user with alerting role`() {
        val clusterPermissions = listOf(
            getClusterPermissionsFromCustomRole(ALERTING_INDEX_WORKFLOW_ACCESS)
        )

        createUserWithTestDataAndCustomRole(
            user,
            TEST_HR_INDEX,
            TEST_HR_ROLE,
            listOf(TEST_HR_BACKEND_ROLE),
            clusterPermissions
        )
        try {
            val monitor = createMonitor(
                randomQueryLevelMonitor(
                    inputs = listOf(SearchInput(listOf(TEST_HR_INDEX), SearchSourceBuilder().query(QueryBuilders.matchAllQuery()))),
                ),
                true
            )

            val workflow = randomWorkflow(
                monitorIds = listOf(monitor.id)
            )

            val createResponse = userClient?.makeRequest("POST", WORKFLOW_ALERTING_BASE_URI, emptyMap(), workflow.toHttpEntity())
            assertEquals("Create workflow failed", RestStatus.CREATED, createResponse?.restStatus())

            assertUserNull(createResponse?.asMap()!!["workflow"] as HashMap<String, Any>)
        } finally {
            deleteRoleAndRoleMapping(TEST_HR_ROLE)
        }
    }

    fun `test create workflow with an user without alerting role`() {
        createUserWithTestDataAndCustomRole(
            user,
            TEST_HR_INDEX,
            TEST_HR_ROLE,
            listOf(TEST_HR_BACKEND_ROLE),
            getClusterPermissionsFromCustomRole(ALERTING_NO_ACCESS_ROLE)
        )
        try {
            val monitor = createRandomMonitor(true)

            val workflow = randomWorkflow(
                monitorIds = listOf(monitor.id)
            )

            userClient?.makeRequest("POST", WORKFLOW_ALERTING_BASE_URI, emptyMap(), workflow.toHttpEntity())
            fail("Expected 403 Method FORBIDDEN response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.FORBIDDEN, e.response.restStatus())
        } finally {
            deleteRoleAndRoleMapping(TEST_HR_ROLE)
        }
    }

    fun `test create workflow with an user with read-only role`() {
        createUserWithTestData(user, TEST_HR_INDEX, TEST_HR_ROLE, TEST_HR_BACKEND_ROLE)
        createUserRolesMapping(ALERTING_READ_ONLY_ACCESS, arrayOf(user))

        try {
            val monitor = createRandomMonitor(true)
            val workflow = randomWorkflow(
                monitorIds = listOf(monitor.id)
            )
            userClient?.makeRequest("POST", WORKFLOW_ALERTING_BASE_URI, emptyMap(), workflow.toHttpEntity())
            fail("Expected 403 Method FORBIDDEN response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.FORBIDDEN, e.response.restStatus())
        } finally {
            deleteRoleAndRoleMapping(TEST_HR_ROLE)
            deleteRoleMapping(ALERTING_READ_ONLY_ACCESS)
        }
    }

    fun `test create workflow with delegate with an user without index read role`() {
        createTestIndex(TEST_NON_HR_INDEX)
        val clusterPermissions = listOf(
            getClusterPermissionsFromCustomRole(ALERTING_INDEX_WORKFLOW_ACCESS)
        )
        createUserWithTestDataAndCustomRole(
            user,
            TEST_HR_INDEX,
            TEST_HR_ROLE,
            listOf(TEST_HR_BACKEND_ROLE),
            clusterPermissions
        )
        try {
            val query = randomDocLevelQuery(tags = listOf())
            val triggers = listOf(randomDocumentLevelTrigger(condition = Script("query[id=\"${query.id}\"]")))

            val monitor = createMonitor(
                randomDocumentLevelMonitor(
                    inputs = listOf(
                        DocLevelMonitorInput(
                            indices = listOf(TEST_NON_HR_INDEX),
                            queries = listOf(query)
                        )
                    ),
                    triggers = triggers
                ),
                true
            )

            val workflow = randomWorkflow(
                monitorIds = listOf(monitor.id)
            )

            userClient?.makeRequest("POST", WORKFLOW_ALERTING_BASE_URI, emptyMap(), workflow.toHttpEntity())
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.FORBIDDEN, e.response.restStatus())
        } finally {
            deleteRoleAndRoleMapping(TEST_HR_ROLE)
            deleteIndex(TEST_NON_HR_INDEX)
        }
    }

    fun `test create workflow with disable filter by`() {
        disableFilterBy()
        val monitor = createRandomMonitor(true)
        val workflow = randomWorkflow(
            monitorIds = listOf(monitor.id)
        )
        val createResponse = client().makeRequest("POST", WORKFLOW_ALERTING_BASE_URI, emptyMap(), workflow.toHttpEntity())
        assertEquals("Create workflow failed", RestStatus.CREATED, createResponse.restStatus())
        assertUserNull(createResponse.asMap()["workflow"] as HashMap<String, Any>)
    }

    fun `test get workflow with an user with get workflow role`() {
        createUserWithTestDataAndCustomRole(
            user,
            TEST_HR_INDEX,
            TEST_HR_ROLE,
            listOf(TEST_HR_BACKEND_ROLE),
            getClusterPermissionsFromCustomRole(ALERTING_GET_WORKFLOW_ACCESS)
        )

        val monitor = createRandomMonitor(true)
        val workflow = createWorkflow(randomWorkflow(monitorIds = listOf(monitor.id)))

        try {
            val getWorkflowResponse = userClient?.makeRequest(
                "GET",
                "$WORKFLOW_ALERTING_BASE_URI/${workflow.id}",
                null,
                BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
            )
            assertEquals("Get workflow failed", RestStatus.OK, getWorkflowResponse?.restStatus())
        } finally {
            deleteRoleAndRoleMapping(TEST_HR_ROLE)
        }
    }

    /*
    TODO: https://github.com/opensearch-project/alerting/issues/300
     */
    fun `test get workflow with an user without get monitor role`() {
        createUserWithTestDataAndCustomRole(
            user,
            TEST_HR_INDEX,
            TEST_HR_ROLE,
            listOf(TEST_HR_BACKEND_ROLE),
            getClusterPermissionsFromCustomRole(ALERTING_NO_ACCESS_ROLE)
        )

        val monitor = createRandomMonitor(true)
        val workflow = createWorkflow(randomWorkflow(monitorIds = listOf(monitor.id)))

        try {
            userClient?.makeRequest(
                "GET",
                "$WORKFLOW_ALERTING_BASE_URI/${workflow.id}",
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
    fun `test update workflow with disable filter by`() {
        disableFilterBy()

        val createdMonitor = createMonitor(monitor = randomQueryLevelMonitor(enabled = true))
        val createdWorkflow = createWorkflow(
            randomWorkflow(monitorIds = listOf(createdMonitor.id), enabled = true, enabledTime = Instant.now())
        )

        assertNotNull("The workflow was not created", createdWorkflow)
        assertTrue("The workflow was not enabled", createdWorkflow.enabled)

        val workflowV2 = createdWorkflow.copy(enabled = false, enabledTime = null)
        val updatedWorkflow = updateWorkflow(workflowV2)

        assertFalse("The monitor was not disabled", updatedWorkflow.enabled)
    }

    fun `test update workflow with enable filter by`() {
        enableFilterBy()
        if (!isHttps()) {
            // if security is disabled and filter by is enabled, we can't create monitor
            // refer: `test create monitor with enable filter by`
            return
        }

        val createdMonitor = createMonitorWithClient(
            client = client(),
            monitor = randomQueryLevelMonitor(enabled = true),
            rbacRoles = listOf("admin")
        )
        val createdWorkflow = createWorkflow(
            randomWorkflow(monitorIds = listOf(createdMonitor.id), enabled = true, enabledTime = Instant.now())
        )

        assertNotNull("The workflow was not created", createdWorkflow)
        assertTrue("The workflow was not enabled", createdWorkflow.enabled)

        val workflowV2 = createdWorkflow.copy(enabled = false, enabledTime = null)
        val updatedWorkflow = updateWorkflow(workflow = workflowV2)

        assertFalse("The monitor was not disabled", updatedWorkflow.enabled)
    }

    fun `test create workflow with enable filter by with a user have access and without role has no access`() {
        enableFilterBy()
        if (!isHttps()) {
            return
        }

        createUserWithRoles(
            user,
            listOf(ALERTING_FULL_ACCESS_ROLE, READALL_AND_MONITOR_ROLE),
            listOf(TEST_HR_BACKEND_ROLE, "role2"),
            false
        )

        val createdMonitor = createMonitorWithClient(
            userClient!!,
            monitor = randomQueryLevelMonitor(enabled = true),
            listOf(TEST_HR_BACKEND_ROLE, "role2")
        )

        assertNotNull("The monitor was not created", createdMonitor)

        val createdWorkflow = createWorkflowWithClient(
            userClient!!,
            workflow = randomWorkflow(monitorIds = listOf(createdMonitor.id), enabled = true),
            listOf(TEST_HR_BACKEND_ROLE, "role2")
        )
        assertNotNull("The workflow was not created", createdWorkflow)

        createUserRolesMapping(ALERTING_FULL_ACCESS_ROLE, arrayOf())
        createUserRolesMapping(READALL_AND_MONITOR_ROLE, arrayOf())

        // getUser should have access to the monitor
        val getUser = "getUser"
        createUserWithTestDataAndCustomRole(
            getUser,
            TEST_HR_INDEX,
            TEST_HR_ROLE,
            listOf("role2"),
            getClusterPermissionsFromCustomRole(ALERTING_GET_WORKFLOW_ACCESS)
        )
        val getUserClient = SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), getUser, getUser)
            .setSocketTimeout(60000).build()

        val getWorkflowResponse = getUserClient?.makeRequest(
            "GET",
            "$WORKFLOW_ALERTING_BASE_URI/${createdWorkflow.id}",
            null,
            BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
        )
        assertEquals("Get workflow failed", RestStatus.OK, getWorkflowResponse?.restStatus())

        // Remove backend role and ensure no access is granted after
        patchUserBackendRoles(getUser, arrayOf("role1"))
        try {
            getUserClient?.makeRequest(
                "GET",
                "$WORKFLOW_ALERTING_BASE_URI/${createdWorkflow.id}",
                null,
                BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
            )
            fail("Expected Forbidden exception")
        } catch (e: ResponseException) {
            assertEquals("Get workflow failed", RestStatus.FORBIDDEN.status, e.response.statusLine.statusCode)
        } finally {
            deleteRoleAndRoleMapping(TEST_HR_ROLE)
            deleteUser(getUser)
            getUserClient?.close()
        }
    }

    fun `test create workflow with enable filter by with a user with a backend role doesn't have access to monitor`() {
        enableFilterBy()
        if (!isHttps()) {
            return
        }

        createUserWithRoles(
            user,
            listOf(ALERTING_FULL_ACCESS_ROLE, READALL_AND_MONITOR_ROLE),
            listOf(TEST_HR_BACKEND_ROLE, "role2"),
            false
        )

        val createdMonitor = createMonitorWithClient(
            userClient!!,
            monitor = randomQueryLevelMonitor(enabled = true),
            listOf("role2")
        )

        assertNotNull("The monitor was not created", createdMonitor)

        val userWithDifferentRole = "role3User"

        createUserWithRoles(
            userWithDifferentRole,
            listOf(ALERTING_FULL_ACCESS_ROLE, READALL_AND_MONITOR_ROLE),
            listOf(TEST_HR_BACKEND_ROLE, "role3"),
            false
        )

        val userWithDifferentRoleClient = SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), userWithDifferentRole, userWithDifferentRole)
            .setSocketTimeout(60000).build()

        try {
            createWorkflowWithClient(
                userWithDifferentRoleClient!!,
                workflow = randomWorkflow(monitorIds = listOf(createdMonitor.id), enabled = true),
                listOf("role3")
            )
            fail("Expected Forbidden exception")
        } catch (e: ResponseException) {
            assertEquals("Create workflow failed", RestStatus.FORBIDDEN.status, e.response.statusLine.statusCode)
        } finally {
            deleteUser(userWithDifferentRole)
            userWithDifferentRoleClient?.close()
        }
    }

    fun `test create workflow with enable filter by with no backend roles`() {
        enableFilterBy()
        if (!isHttps()) {
            // if security is disabled and filter by is enabled, we can't create monitor
            // refer: `test create monitor with enable filter by`
            return
        }
        val monitor = createMonitor(randomQueryLevelMonitor(enabled = true))

        val workflow = randomWorkflow(monitorIds = listOf(monitor.id))

        createUserWithRoles(
            user,
            listOf(ALERTING_FULL_ACCESS_ROLE, READALL_AND_MONITOR_ROLE),
            listOf(TEST_HR_BACKEND_ROLE, "role2"),
            false
        )

        try {
            createWorkflowWithClient(userClient!!, workflow, listOf())
            fail("Expected exception since a non-admin user is trying to create a workflow with no backend roles")
        } catch (e: ResponseException) {
            assertEquals("Create workflow failed", RestStatus.FORBIDDEN.status, e.response.statusLine.statusCode)
        } finally {
            createUserRolesMapping(ALERTING_FULL_ACCESS_ROLE, arrayOf())
            createUserRolesMapping(READALL_AND_MONITOR_ROLE, arrayOf())
        }
    }

    fun `test create workflow as admin with enable filter by with no backend roles`() {
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

        val createdMonitor = createMonitor(monitor = monitor)
        val createdWorkflow = createWorkflow(randomWorkflow(monitorIds = listOf(createdMonitor.id)))
        assertNotNull("The workflow was not created", createdWorkflow)

        try {

            userClient?.makeRequest(
                "GET",
                "$WORKFLOW_ALERTING_BASE_URI/${createdWorkflow.id}",
                null,
                BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
            )
            fail("Expected Forbidden exception")
        } catch (e: ResponseException) {
            assertEquals("Get workflow failed", RestStatus.FORBIDDEN.status, e.response.statusLine.statusCode)
        } finally {
            createUserRolesMapping(ALERTING_FULL_ACCESS_ROLE, arrayOf())
            createUserRolesMapping(READALL_AND_MONITOR_ROLE, arrayOf())
        }
    }

    fun `test create workflow with enable filter by with roles user has no access and throw exception`() {
        enableFilterBy()
        if (!isHttps()) {
            // if security is disabled and filter by is enabled, we can't create monitor
            // refer: `test create monitor with enable filter by`
            return
        }
        val monitor = createMonitor(randomQueryLevelMonitor(enabled = true))
        val workflow = randomWorkflow(monitorIds = listOf(monitor.id))

        createUserWithRoles(
            user,
            listOf(ALERTING_FULL_ACCESS_ROLE, READALL_AND_MONITOR_ROLE),
            listOf(TEST_HR_BACKEND_ROLE, "role2"),
            false
        )

        try {
            createWorkflowWithClient(userClient!!, workflow = workflow, listOf(TEST_HR_BACKEND_ROLE, "role1", "role2"))
            fail("Expected create workflow to fail as user does not have role1 backend role")
        } catch (e: ResponseException) {
            assertEquals("Create workflow failed", RestStatus.FORBIDDEN.status, e.response.statusLine.statusCode)
        } finally {
            createUserRolesMapping(ALERTING_FULL_ACCESS_ROLE, arrayOf())
            createUserRolesMapping(READALL_AND_MONITOR_ROLE, arrayOf())
        }
    }

    fun `test create workflow as admin with enable filter by with a user have access and without role has no access`() {
        enableFilterBy()
        if (!isHttps()) {
            // if security is disabled and filter by is enabled, we can't create monitor
            // refer: `test create monitor with enable filter by`
            return
        }
        val monitor = randomQueryLevelMonitor(enabled = true)

        val createdMonitor = createMonitorWithClient(client(), monitor = monitor, listOf(TEST_HR_BACKEND_ROLE, "role1", "role2"))
        val createdWorkflow = createWorkflowWithClient(client(), randomWorkflow(monitorIds = listOf(createdMonitor.id)), listOf(TEST_HR_BACKEND_ROLE, "role1", "role2"))
        assertNotNull("The workflow was not created", createdWorkflow)

        // user should have access to the admin monitor
        createUserWithTestDataAndCustomRole(
            user,
            TEST_HR_INDEX,
            TEST_HR_ROLE,
            listOf(TEST_HR_BACKEND_ROLE),
            getClusterPermissionsFromCustomRole(ALERTING_GET_WORKFLOW_ACCESS)
        )

        val getWorkflowResponse = userClient?.makeRequest(
            "GET",
            "$WORKFLOW_ALERTING_BASE_URI/${createdWorkflow.id}",
            null,
            BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
        )
        assertEquals("Get workflow failed", RestStatus.OK, getWorkflowResponse?.restStatus())

        // Remove good backend role and ensure no access is granted after
        patchUserBackendRoles(user, arrayOf("role5"))
        try {
            userClient?.makeRequest(
                "GET",
                "$WORKFLOW_ALERTING_BASE_URI/${createdWorkflow.id}",
                null,
                BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
            )
            fail("Expected Forbidden exception")
        } catch (e: ResponseException) {
            assertEquals("Get workflow failed", RestStatus.FORBIDDEN.status, e.response.statusLine.statusCode)
        } finally {
            deleteRoleAndRoleMapping(TEST_HR_ROLE)
        }
    }

    fun `test update workflow with enable filter by with removing a permission`() {
        enableFilterBy()
        if (!isHttps()) {
            // if security is disabled and filter by is enabled, we can't create monitor
            // refer: `test create monitor with enable filter by`
            return
        }

        createUserWithRoles(
            user,
            listOf(ALERTING_FULL_ACCESS_ROLE, READALL_AND_MONITOR_ROLE),
            listOf(TEST_HR_BACKEND_ROLE, "role2"),
            false
        )

        val createdMonitor = createMonitorWithClient(userClient!!, randomQueryLevelMonitor(), listOf(TEST_HR_BACKEND_ROLE, "role2"))
        val createdWorkflow = createWorkflowWithClient(userClient!!, workflow = randomWorkflow(enabled = true, monitorIds = listOf(createdMonitor.id)), listOf(TEST_HR_BACKEND_ROLE, "role2"))
        assertNotNull("The workflow was not created", createdWorkflow)

        // getUser should have access to the monitor
        val getUser = "getUser"
        createUserWithTestDataAndCustomRole(
            getUser,
            TEST_HR_INDEX,
            TEST_HR_ROLE,
            listOf("role2"),
            getClusterPermissionsFromCustomRole(ALERTING_GET_WORKFLOW_ACCESS)
        )
        val getUserClient = SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), getUser, getUser)
            .setSocketTimeout(60000).build()

        val getWorkflowResponse = getUserClient?.makeRequest(
            "GET",
            "$WORKFLOW_ALERTING_BASE_URI/${createdWorkflow.id}",
            null,
            BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
        )
        assertEquals("Get workflow failed", RestStatus.OK, getWorkflowResponse?.restStatus())

        // Remove backend role from monitor
        val updatedWorkflow = updateWorkflowWithClient(userClient!!, createdWorkflow, listOf(TEST_HR_BACKEND_ROLE))

        // getUser should no longer have access
        try {
            getUserClient?.makeRequest(
                "GET",
                "$WORKFLOW_ALERTING_BASE_URI/${updatedWorkflow.id}",
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

    fun `test update workflow with enable filter by with no backend roles`() {
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

        val createdWorkflow = createWorkflowWithClient(
            userClient!!,
            randomWorkflow(monitorIds = listOf(createdMonitor.id)),
            listOf("role2")
        )

        assertNotNull("The workflow was not created", createdWorkflow)

        try {
            updateWorkflowWithClient(userClient!!, createdWorkflow, listOf())
        } catch (e: ResponseException) {
            assertEquals("Update monitor failed", RestStatus.FORBIDDEN.status, e.response.statusLine.statusCode)
        } finally {
            createUserRolesMapping(ALERTING_FULL_ACCESS_ROLE, arrayOf())
            createUserRolesMapping(READALL_AND_MONITOR_ROLE, arrayOf())
        }
    }

    fun `test update workflow as admin with enable filter by with no backend roles`() {
        enableFilterBy()
        if (!isHttps()) {
            // if security is disabled and filter by is enabled, we can't create monitor
            // refer: `test create monitor with enable filter by`
            return
        }
        val monitor = randomQueryLevelMonitor(enabled = true)
        val createdMonitorResponse = createMonitor(monitor, true)
        assertNotNull("The monitor was not created", createdMonitorResponse)

        createUserWithRoles(
            user,
            listOf(ALERTING_FULL_ACCESS_ROLE, READALL_AND_MONITOR_ROLE),
            listOf(TEST_HR_BACKEND_ROLE, "role2"),
            false
        )
        val workflow = randomWorkflow(
            monitorIds = listOf(createdMonitorResponse.id)
        )

        val createdWorkflow = createWorkflowWithClient(
            client(),
            workflow = workflow,
            rbacRoles = listOf(TEST_HR_BACKEND_ROLE)
        )

        assertNotNull("The workflow was not created", createdWorkflow)

        val getWorkflowResponse = userClient?.makeRequest(
            "GET",
            "$WORKFLOW_ALERTING_BASE_URI/${createdWorkflow.id}",
            null,
            BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
        )
        assertEquals("Get workflow failed", RestStatus.OK, getWorkflowResponse?.restStatus())

        val updatedWorkflow = updateWorkflowWithClient(client(), createdWorkflow, listOf())

        try {
            userClient?.makeRequest(
                "GET",
                "$WORKFLOW_ALERTING_BASE_URI/${updatedWorkflow.id}",
                null,
                BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
            )
            fail("Expected Forbidden exception")
        } catch (e: ResponseException) {
            assertEquals("Get workflow failed", RestStatus.FORBIDDEN.status, e.response.statusLine.statusCode)
        } finally {
            createUserRolesMapping(ALERTING_FULL_ACCESS_ROLE, arrayOf())
            createUserRolesMapping(READALL_AND_MONITOR_ROLE, arrayOf())
        }
    }

    fun `test update workflow with enable filter by with updating with a permission user has no access to and throw exception`() {
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

        val createdWorkflow = createWorkflowWithClient(
            userClient!!,
            workflow = randomWorkflow(monitorIds = listOf(createdMonitor.id)), listOf(TEST_HR_BACKEND_ROLE, "role2")
        )

        assertNotNull("The workflow was not created", createdWorkflow)

        // getUser should have access to the monitor
        val getUser = "getUser"
        createUserWithTestDataAndCustomRole(
            getUser,
            TEST_HR_INDEX,
            TEST_HR_ROLE,
            listOf("role2"),
            getClusterPermissionsFromCustomRole(ALERTING_GET_WORKFLOW_ACCESS)
        )
        val getUserClient = SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), getUser, getUser)
            .setSocketTimeout(60000).build()

        val getWorkflowResponse = getUserClient?.makeRequest(
            "GET",
            "$WORKFLOW_ALERTING_BASE_URI/${createdWorkflow.id}",
            null,
            BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
        )
        assertEquals("Get workflow failed", RestStatus.OK, getWorkflowResponse?.restStatus())

        try {
            updateWorkflowWithClient(userClient!!, createdWorkflow, listOf(TEST_HR_BACKEND_ROLE, "role1"))
            fail("Expected update workflow to fail as user doesn't have access to role1")
        } catch (e: ResponseException) {
            assertEquals("Update workflow failed", RestStatus.FORBIDDEN.status, e.response.statusLine.statusCode)
        } finally {
            deleteRoleAndRoleMapping(TEST_HR_ROLE)
            deleteUser(getUser)
            getUserClient?.close()
            createUserRolesMapping(ALERTING_FULL_ACCESS_ROLE, arrayOf())
            createUserRolesMapping(READALL_AND_MONITOR_ROLE, arrayOf())
        }
    }

    fun `test update workflow as another user with enable filter by with removing a permission and adding permission`() {
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

        val createdWorkflow = createWorkflowWithClient(
            userClient!!,
            workflow = randomWorkflow(monitorIds = listOf(createdMonitor.id), enabled = true)
        )

        assertNotNull("The workflow was not created", createdWorkflow)

        // Remove backend role from workflow with new user and add role5
        val updateUser = "updateUser"
        createUserWithRoles(
            updateUser,
            listOf(ALERTING_FULL_ACCESS_ROLE, READALL_AND_MONITOR_ROLE),
            listOf(TEST_HR_BACKEND_ROLE, "role5"),
            false
        )

        val updateUserClient = SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), updateUser, updateUser)
            .setSocketTimeout(60000).build()
        val updatedWorkflow = updateWorkflowWithClient(updateUserClient, createdWorkflow, listOf("role5"))

        // old user should no longer have access
        try {
            userClient?.makeRequest(
                "GET",
                "$WORKFLOW_ALERTING_BASE_URI/${updatedWorkflow.id}",
                null,
                BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
            )
            fail("Expected Forbidden exception")
        } catch (e: ResponseException) {
            assertEquals("Get workflow failed", RestStatus.FORBIDDEN.status, e.response.statusLine.statusCode)
        } finally {
            deleteUser(updateUser)
            updateUserClient?.close()
            createUserRolesMapping(ALERTING_FULL_ACCESS_ROLE, arrayOf())
            createUserRolesMapping(READALL_AND_MONITOR_ROLE, arrayOf())
        }
    }

    fun `test update workflow as admin with enable filter by with removing a permission`() {
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

        val createdWorkflow = createWorkflowWithClient(
            userClient!!,
            workflow = randomWorkflow(monitorIds = listOf(createdMonitor.id)),
            listOf(TEST_HR_BACKEND_ROLE, "role2")
        )
        assertNotNull("The workflow was not created", createdWorkflow)

        // getUser should have access to the monitor
        val getUser = "getUser"
        createUserWithTestDataAndCustomRole(
            getUser,
            TEST_HR_INDEX,
            TEST_HR_ROLE,
            listOf("role1", "role2"),
            getClusterPermissionsFromCustomRole(ALERTING_GET_WORKFLOW_ACCESS)
        )
        val getUserClient = SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), getUser, getUser)
            .setSocketTimeout(60000).build()

        val getWorkflowResponse = getUserClient?.makeRequest(
            "GET",
            "$WORKFLOW_ALERTING_BASE_URI/${createdWorkflow.id}",
            null,
            BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
        )
        assertEquals("Get workflow failed", RestStatus.OK, getWorkflowResponse?.restStatus())

        // Remove backend role from monitor
        val updatedWorkflow = updateWorkflowWithClient(client(), createdWorkflow, listOf("role4"))

        // original user should no longer have access
        try {
            userClient?.makeRequest(
                "GET",
                "$WORKFLOW_ALERTING_BASE_URI/${updatedWorkflow.id}",
                null,
                BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
            )
            fail("Expected Forbidden exception")
        } catch (e: ResponseException) {
            assertEquals("Get workflow failed", RestStatus.FORBIDDEN.status, e.response.statusLine.statusCode)
        } finally {
            createUserRolesMapping(ALERTING_FULL_ACCESS_ROLE, arrayOf())
            createUserRolesMapping(READALL_AND_MONITOR_ROLE, arrayOf())
        }

        // get user should no longer have access
        try {
            getUserClient?.makeRequest(
                "GET",
                "$WORKFLOW_ALERTING_BASE_URI/${updatedWorkflow.id}",
                null,
                BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
            )
            fail("Expected Forbidden exception")
        } catch (e: ResponseException) {
            assertEquals("Get workflow failed", RestStatus.FORBIDDEN.status, e.response.statusLine.statusCode)
        } finally {
            deleteRoleAndRoleMapping(TEST_HR_ROLE)
            deleteUser(getUser)
            getUserClient?.close()
        }
    }

    fun `test delete workflow with disable filter by`() {
        disableFilterBy()
        val monitor = randomQueryLevelMonitor(enabled = true)

        val createdMonitor = createMonitor(monitor = monitor)
        val createdWorkflow = createWorkflow(workflow = randomWorkflow(monitorIds = listOf(createdMonitor.id), enabled = true))

        assertNotNull("The workflow was not created", createdWorkflow)
        assertTrue("The workflow was not enabled", createdWorkflow.enabled)

        deleteWorkflow(workflow = createdWorkflow, deleteDelegates = true)

        val searchMonitor = SearchSourceBuilder().query(QueryBuilders.termQuery("_id", createdMonitor.id)).toString()
        // Verify if the delegate monitors are deleted
        // search as "admin" - must get 0 docs
        val adminMonitorSearchResponse = client().makeRequest(
            "POST",
            "$ALERTING_BASE_URI/_search",
            emptyMap(),
            NStringEntity(searchMonitor, ContentType.APPLICATION_JSON)
        )
        assertEquals("Search monitor failed", RestStatus.OK, adminMonitorSearchResponse.restStatus())

        val adminMonitorHits = createParser(
            XContentType.JSON.xContent(),
            adminMonitorSearchResponse.entity.content
        ).map()["hits"]!! as Map<String, Map<String, Any>>
        val adminMonitorDocsFound = adminMonitorHits["total"]?.get("value")
        assertEquals("Monitor found during search", 0, adminMonitorDocsFound)

        // Verify workflow deletion
        try {
            client().makeRequest(
                "GET",
                "$WORKFLOW_ALERTING_BASE_URI/${createdWorkflow.id}",
                emptyMap(),
                null
            )
            fail("Workflow found during search")
        } catch (e: ResponseException) {
            assertEquals("Get workflow failed", RestStatus.NOT_FOUND.status, e.response.statusLine.statusCode)
        }
    }

    fun `test delete workflow with enable filter by`() {
        enableFilterBy()
        if (!isHttps()) {
            // if security is disabled and filter by is enabled, we can't create monitor
            // refer: `test create monitor with enable filter by`
            return
        }
        val createdMonitor = createMonitorWithClient(
            monitor = randomQueryLevelMonitor(),
            client = client(),
            rbacRoles = listOf("admin")
        )

        assertNotNull("The monitor was not created", createdMonitor)

        val createdWorkflow = createWorkflow(workflow = randomWorkflow(monitorIds = listOf(createdMonitor.id), enabled = true))
        assertNotNull("The workflow was not created", createdWorkflow)
        assertTrue("The workflow was not enabled", createdWorkflow.enabled)

        deleteWorkflow(workflow = createdWorkflow, true)

        // Verify underlying delegates deletion
        val search = SearchSourceBuilder().query(QueryBuilders.termQuery("_id", createdMonitor.id)).toString()
        // search as "admin" - must get 0 docs
        val adminSearchResponse = client().makeRequest(
            "POST",
            "$ALERTING_BASE_URI/_search",
            emptyMap(),
            NStringEntity(search, ContentType.APPLICATION_JSON)
        )
        assertEquals("Search monitor failed", RestStatus.OK, adminSearchResponse.restStatus())

        val adminHits = createParser(
            XContentType.JSON.xContent(),
            adminSearchResponse.entity.content
        ).map()["hits"]!! as Map<String, Map<String, Any>>
        val adminDocsFound = adminHits["total"]?.get("value")
        assertEquals("Monitor found during search", 0, adminDocsFound)

        // Verify workflow deletion
        try {
            client().makeRequest(
                "GET",
                "$WORKFLOW_ALERTING_BASE_URI/${createdWorkflow.id}",
                emptyMap(),
                null
            )
            fail("Workflow found during search")
        } catch (e: ResponseException) {
            assertEquals("Get workflow failed", RestStatus.NOT_FOUND.status, e.response.statusLine.statusCode)
        }
    }

    fun `test delete workflow with enable filter with user that doesn't have delete_monitor cluster privilege failed`() {
        enableFilterBy()
        if (!isHttps()) {
            // if security is disabled and filter by is enabled, we can't create monitor
            // refer: `test create monitor with enable filter by`
            return
        }
        createUserWithRoles(
            user,
            listOf(ALERTING_FULL_ACCESS_ROLE, READALL_AND_MONITOR_ROLE),
            listOf(TEST_HR_BACKEND_ROLE, "role2"),
            false
        )

        val deleteUser = "deleteUser"
        createUserWithTestDataAndCustomRole(
            deleteUser,
            TEST_HR_INDEX,
            TEST_HR_ROLE,
            listOf("role1", "role3"),
            listOf(
                getClusterPermissionsFromCustomRole(ALERTING_DELETE_WORKFLOW_ACCESS),
                getClusterPermissionsFromCustomRole(ALERTING_GET_WORKFLOW_ACCESS)
            )
        )
        val deleteUserClient = SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), deleteUser, deleteUser)
            .setSocketTimeout(60000).build()

        try {
            val createdMonitor = createMonitorWithClient(userClient!!, monitor = randomQueryLevelMonitor())

            assertNotNull("The monitor was not created", createdMonitor)

            val createdWorkflow = createWorkflowWithClient(userClient!!, workflow = randomWorkflow(monitorIds = listOf(createdMonitor.id), enabled = true))
            assertNotNull("The workflow was not created", createdWorkflow)
            assertTrue("The workflow was not enabled", createdWorkflow.enabled)

            try {
                deleteWorkflowWithClient(deleteUserClient, workflow = createdWorkflow, true)
                fail("Expected Forbidden exception")
            } catch (e: ResponseException) {
                assertEquals("Get workflow failed", RestStatus.FORBIDDEN.status, e.response.statusLine.statusCode)
            }
            patchUserBackendRoles(deleteUser, arrayOf("role2"))

            val response = deleteWorkflowWithClient(deleteUserClient!!, workflow = createdWorkflow, true)
            assertEquals("Delete workflow failed", RestStatus.OK, response?.restStatus())
        } finally {
            deleteRoleAndRoleMapping(TEST_HR_ROLE)
            deleteUser(deleteUser)
            deleteUserClient?.close()
        }
    }

    fun `test execute workflow with an user with execute workflow access`() {
        createUserWithTestDataAndCustomRole(
            user,
            TEST_HR_INDEX,
            TEST_HR_ROLE,
            listOf(TEST_HR_BACKEND_ROLE),
            getClusterPermissionsFromCustomRole(ALERTING_EXECUTE_WORKFLOW_ACCESS)
        )

        val monitor = createRandomMonitor(true)
        val workflow = createRandomWorkflow(listOf(monitor.id), true)

        try {
            val executeWorkflowResponse = userClient?.makeRequest(
                "POST",
                "$WORKFLOW_ALERTING_BASE_URI/${workflow.id}/_execute",
                mutableMapOf()
            )
            assertEquals("Executing workflow failed", RestStatus.OK, executeWorkflowResponse?.restStatus())
        } finally {
            deleteRoleAndRoleMapping(TEST_HR_ROLE)
        }
    }

    fun `test execute workflow with an user without execute workflow access`() {
        createUserWithTestDataAndCustomRole(
            user,
            TEST_HR_INDEX,
            TEST_HR_ROLE,
            listOf(TEST_HR_BACKEND_ROLE),
            getClusterPermissionsFromCustomRole(ALERTING_NO_ACCESS_ROLE)
        )

        val monitor = createRandomMonitor(true)
        val workflow = createRandomWorkflow(listOf(monitor.id), true)

        try {
            userClient?.makeRequest(
                "POST",
                "$WORKFLOW_ALERTING_BASE_URI/${workflow.id}/_execute",
                mutableMapOf()
            )
            fail("Expected 403 Method FORBIDDEN response")
        } catch (e: ResponseException) {
            assertEquals("Execute workflow failed", RestStatus.FORBIDDEN, e.response.restStatus())
        } finally {
            deleteRoleAndRoleMapping(TEST_HR_ROLE)
        }
    }

    fun `test delete workflow with an user with delete workflow access`() {
        createUserWithTestDataAndCustomRole(
            user,
            TEST_HR_INDEX,
            TEST_HR_ROLE,
            listOf(TEST_HR_BACKEND_ROLE),
            getClusterPermissionsFromCustomRole(ALERTING_DELETE_WORKFLOW_ACCESS)
        )

        val monitor = createRandomMonitor(true)
        val workflow = createRandomWorkflow(monitorIds = listOf(monitor.id))
        val refresh = true

        try {
            val deleteWorkflowResponse = userClient?.makeRequest(
                "DELETE",
                "$WORKFLOW_ALERTING_BASE_URI/${workflow.id}?refresh=$refresh",
                emptyMap(),
                monitor.toHttpEntity()
            )
            assertEquals("DELETE workflow failed", RestStatus.OK, deleteWorkflowResponse?.restStatus())
        } finally {
            deleteRoleAndRoleMapping(TEST_HR_ROLE)
        }
    }

    fun `test delete workflow with deleting delegates with an user with delete workflow access`() {
        createUserWithTestDataAndCustomRole(
            user,
            TEST_HR_INDEX,
            TEST_HR_ROLE,
            listOf(TEST_HR_BACKEND_ROLE),
            getClusterPermissionsFromCustomRole(ALERTING_DELETE_WORKFLOW_ACCESS)
        )

        val monitor = createRandomMonitor(true)
        val workflow = createRandomWorkflow(monitorIds = listOf(monitor.id))

        try {
            val deleteWorkflowResponse = deleteWorkflowWithClient(
                userClient!!,
                workflow,
                deleteDelegates = true,
                refresh = true
            )
            assertEquals("DELETE workflow failed", RestStatus.OK, deleteWorkflowResponse?.restStatus())
        } finally {
            deleteRoleAndRoleMapping(TEST_HR_ROLE)
        }
        // Verify delegate deletion
        val search = SearchSourceBuilder().query(QueryBuilders.termQuery("_id", monitor.id)).toString()
        // search as "admin" - must get 0 docs
        val adminSearchResponse = client().makeRequest(
            "POST",
            "$ALERTING_BASE_URI/_search",
            emptyMap(),
            NStringEntity(search, ContentType.APPLICATION_JSON)
        )
        assertEquals("Search monitor failed", RestStatus.OK, adminSearchResponse.restStatus())

        val adminHits = createParser(
            XContentType.JSON.xContent(),
            adminSearchResponse.entity.content
        ).map()["hits"]!! as Map<String, Map<String, Any>>
        val adminDocsFound = adminHits["total"]?.get("value")
        assertEquals("Monitor found during search", 0, adminDocsFound)
    }

    fun `test delete workflow with an user without delete monitor access`() {
        createUserWithTestDataAndCustomRole(
            user,
            TEST_HR_INDEX,
            TEST_HR_ROLE,
            listOf(TEST_HR_BACKEND_ROLE),
            getClusterPermissionsFromCustomRole(ALERTING_NO_ACCESS_ROLE)
        )

        val monitor = createRandomMonitor(true)
        val workflow = createRandomWorkflow(monitorIds = listOf(monitor.id))

        try {
            userClient?.makeRequest(
                "DELETE",
                "$WORKFLOW_ALERTING_BASE_URI/${workflow.id}?refresh=true",
                emptyMap(),
                monitor.toHttpEntity()
            )
            fail("Expected 403 Method FORBIDDEN response")
        } catch (e: ResponseException) {
            assertEquals("DELETE workflow failed", RestStatus.FORBIDDEN, e.response.restStatus())
        } finally {
            deleteRoleAndRoleMapping(TEST_HR_ROLE)
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
                        indices = listOf(TEST_HR_INDEX),
                        query = SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
                    )
                )
            )

            val createResponse = userClient?.makeRequest("POST", ALERTING_BASE_URI, emptyMap(), monitor.toHttpEntity())
            assertEquals("Create monitor failed", RestStatus.CREATED, createResponse?.restStatus())
            val monitorJson = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE,
                createResponse?.entity?.content
            ).map()
            val monitorId = monitorJson["_id"] as String

            val workflow = randomWorkflow(monitorIds = listOf(monitorId))
            val createWorkflowResponse = userClient?.makeRequest("POST", WORKFLOW_ALERTING_BASE_URI, emptyMap(), workflow.toHttpEntity())
            assertEquals("Create workflow failed", RestStatus.CREATED, createWorkflowResponse?.restStatus())

            val workflowJson = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE,
                createWorkflowResponse?.entity?.content
            ).map()

            val id: String = workflowJson["_id"] as String
            val search = SearchSourceBuilder().query(QueryBuilders.termQuery("_id", id)).toString()

            // get as "admin" - must get 1 docs
            val adminGetResponse = client().makeRequest(
                "GET",
                "$WORKFLOW_ALERTING_BASE_URI/$id",
                emptyMap(),
                NStringEntity(search, ContentType.APPLICATION_JSON)
            )
            assertEquals("Get workflow failed", RestStatus.OK, adminGetResponse.restStatus())

            // delete as "admin"
            val adminDeleteResponse = client().makeRequest(
                "DELETE",
                "$WORKFLOW_ALERTING_BASE_URI/$id",
                emptyMap(),
                NStringEntity(search, ContentType.APPLICATION_JSON)
            )
            assertEquals("Delete workflow failed", RestStatus.OK, adminDeleteResponse.restStatus())
        } finally {
            deleteRoleAndRoleMapping(TEST_HR_ROLE)
            deleteRoleMapping(ALERTING_FULL_ACCESS_ROLE)
        }
    }

    fun `test execute workflow with bucket-level and doc-level chained monitors with user having partial index permissions`() {
        createUser(user, user, arrayOf(TEST_HR_BACKEND_ROLE))
        createTestIndex(TEST_HR_INDEX)

        createIndexRoleWithDocLevelSecurity(
            TEST_HR_ROLE,
            TEST_HR_INDEX,
            TERM_DLS_QUERY,
            listOf(ALERTING_INDEX_WORKFLOW_ACCESS, ALERTING_INDEX_MONITOR_ACCESS)
        )
        createUserRolesMapping(TEST_HR_ROLE, arrayOf(user))

        // Add a doc that is accessible to the user
        indexDoc(
            TEST_HR_INDEX,
            "1",
            """
            {
              "test_field": "a",
              "accessible": true
            }
            """.trimIndent()
        )

        // Add a second doc that is not accessible to the user
        indexDoc(
            TEST_HR_INDEX,
            "2",
            """
            {
              "test_field": "b",
              "accessible": false
            }
            """.trimIndent()
        )

        indexDoc(
            TEST_HR_INDEX,
            "3",
            """
            {
              "test_field": "c",
              "accessible": true
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
        val bucketMonitor = createMonitorWithClient(
            userClient!!,
            randomBucketLevelMonitor(
                inputs = listOf(input),
                enabled = false,
                triggers = listOf(trigger),
                dataSources = DataSources(findingsEnabled = true)
            )
        )
        assertNotNull("The bucket monitor was not created", bucketMonitor)

        val docQuery1 = DocLevelQuery(query = "test_field:\"a\"", name = "3")
        var monitor1 = randomDocumentLevelMonitor(
            inputs = listOf(DocLevelMonitorInput("description", listOf(TEST_HR_INDEX), listOf(docQuery1))),
            triggers = listOf(randomDocumentLevelTrigger(condition = ALWAYS_RUN))
        )
        val docMonitor = createMonitorWithClient(userClient!!, monitor1)!!
        assertNotNull("The doc level monitor was not created", docMonitor)

        val workflow = randomWorkflow(monitorIds = listOf(bucketMonitor.id, docMonitor.id))
        val workflowResponse = createWorkflowWithClient(userClient!!, workflow)
        assertNotNull("The workflow was not created", workflowResponse)

        try {
            executeWorkflow(workflowId = workflowResponse.id)
            val bucketAlerts = searchAlerts(bucketMonitor)
            assertEquals("Incorrect number of alerts", 2, bucketAlerts.size)

            val docAlerts = searchAlerts(docMonitor)
            assertEquals("Incorrect number of alerts", 1, docAlerts.size)
        } finally {
            deleteRoleAndRoleMapping(TEST_HR_ROLE)
        }
    }
}

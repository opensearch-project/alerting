/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.junit.After
import org.junit.Assume.assumeTrue
import org.junit.Before
import org.opensearch.alerting.ALERTING_FULL_ACCESS_ROLE
import org.opensearch.alerting.AlertingRestTestCase
import org.opensearch.alerting.READALL_AND_MONITOR_ROLE
import org.opensearch.alerting.makeRequest
import org.opensearch.alerting.randomQueryLevelMonitor
import org.opensearch.alerting.randomWorkflow
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.client.ResponseException
import org.opensearch.client.RestClient
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.alerting.model.SearchInput
import org.opensearch.commons.rest.SecureRestClientBuilder
import org.opensearch.search.builder.SearchSourceBuilder

class SecureExcludedBackendRolesRestApiIT : AlertingRestTestCase() {
    private val roles = listOf("team", "shared")

    @Before
    fun requireSecurity() {
        assumeTrue(securityEnabled())
        enableFilterBy()
        setFilterByBackendRolesStrategy("intersect")
    }

    @After
    fun resetExclusions() {
        if (securityEnabled()) {
            client().updateSettings(AlertingSettings.EXCLUDED_BACKEND_ROLES.key, emptyList<String>())
            setFilterByBackendRolesStrategy("intersect")
            disableFilterBy()
        }
    }

    fun `test exclusions on admin monitor and workflow writes`() {
        verifyStoredRoles(client(), roles)
    }

    fun `test exclusions on non admin monitor and workflow writes`() {
        verifyNonAdminWrites()
    }

    fun `test exclusions on non admin writes with ALL matching`() {
        setFilterByBackendRolesStrategy("all")
        verifyNonAdminWrites()
    }

    private fun verifyNonAdminWrites() {
        val username = "excluded-roles-user"
        createUserWithRoles(username, listOf(ALERTING_FULL_ACCESS_ROLE, READALL_AND_MONITOR_ROLE), roles, false)
        try {
            SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), username, password).build().use {
                // Without rbac_roles, creation starts with all of the caller's backend roles.
                verifyStoredRoles(it, null)
            }
        } finally {
            deleteUser(username)
            createUserRolesMapping(ALERTING_FULL_ACCESS_ROLE, emptyArray())
            createUserRolesMapping(READALL_AND_MONITOR_ROLE, emptyArray())
        }
    }

    private fun verifyStoredRoles(caller: RestClient, requestedRoles: List<String>?) {
        val index = "excluded-roles-data"
        createTestIndex(index)
        val template = randomQueryLevelMonitor(
            enabled = false, inputs = listOf(SearchInput(listOf(index), SearchSourceBuilder()))
        )
        client().updateSettings(AlertingSettings.EXCLUDED_BACKEND_ROLES.key, emptyList<String>())
        val monitor = createMonitorWithClient(caller, template, requestedRoles)
        val workflow = createWorkflowWithClient(caller, randomWorkflow(monitorIds = listOf(monitor.id), enabled = false), requestedRoles)
        assertStoredRoles(monitor.id, "monitor", roles)
        assertStoredRoles(workflow.id, "workflow", roles)

        client().updateSettings(AlertingSettings.EXCLUDED_BACKEND_ROLES.key, listOf("shared"))
        // Changing the setting does not rewrite existing documents.
        assertStoredRoles(monitor.id, "monitor", roles)
        assertStoredRoles(workflow.id, "workflow", roles)

        // Updates without rbac_roles start from the previously stored roles.
        caller.makeRequest("PUT", "${monitor.relativeUrl()}?refresh=true", monitor.toHttpEntity())
        caller.makeRequest("PUT", "${workflow.relativeUrl()}?refresh=true", workflow.toHttpEntity())
        assertStoredRoles(monitor.id, "monitor", listOf("team"))
        assertStoredRoles(workflow.id, "workflow", listOf("team"))

        // Explicit role assignment must not reintroduce an excluded role.
        updateMonitorWithClient(caller, monitor, roles)
        updateWorkflowWithClient(caller, workflow, roles)
        assertStoredRoles(monitor.id, "monitor", listOf("team"))
        assertStoredRoles(workflow.id, "workflow", listOf("team"))

        val filteredMonitor = createMonitorWithClient(caller, template, requestedRoles)
        val filteredWorkflow = createWorkflowWithClient(
            caller, randomWorkflow(monitorIds = listOf(filteredMonitor.id), enabled = false), requestedRoles
        )
        assertStoredRoles(filteredMonitor.id, "monitor", listOf("team"))
        assertStoredRoles(filteredWorkflow.id, "workflow", listOf("team"))

        client().updateSettings(AlertingSettings.EXCLUDED_BACKEND_ROLES.key, roles)
        assertAllRolesRejected { updateMonitorWithClient(caller, monitor, roles) }
        assertAllRolesRejected { updateWorkflowWithClient(caller, workflow, roles) }
        assertStoredRoles(monitor.id, "monitor", listOf("team"))
        assertStoredRoles(workflow.id, "workflow", listOf("team"))
        assertAllRolesRejected { createMonitorWithClient(caller, template, requestedRoles) }
        assertAllRolesRejected {
            createWorkflowWithClient(caller, randomWorkflow(monitorIds = listOf(monitor.id), enabled = false), requestedRoles)
        }

        // Removing the exclusions takes effect without restarting the node.
        client().updateSettings(AlertingSettings.EXCLUDED_BACKEND_ROLES.key, emptyList<String>())
        updateMonitorWithClient(caller, monitor, roles)
        updateWorkflowWithClient(caller, workflow, roles)
        assertStoredRoles(monitor.id, "monitor", roles)
        assertStoredRoles(workflow.id, "workflow", roles)
    }

    private fun assertStoredRoles(id: String, type: String, expected: List<String>) {
        val document = adminClient().makeRequest("GET", "/${ScheduledJob.SCHEDULED_JOBS_INDEX}/_doc/$id").asMap()
        val source = document["_source"] as Map<*, *>
        val resource = source[type] as Map<*, *>
        val user = resource["user"] as Map<*, *>
        assertEquals(expected.toSet(), (user["backend_roles"] as List<*>).toSet())
    }

    private fun assertAllRolesRejected(write: () -> Unit) {
        val exception = expectThrows(ResponseException::class.java) { write() }
        assertEquals(400, exception.response.statusLine.statusCode)
        assertTrue(exception.message!!.contains("would remove all backend roles"))
    }
}

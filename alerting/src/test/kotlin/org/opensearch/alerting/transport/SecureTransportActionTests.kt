/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import org.opensearch.alerting.settings.FilterByBackendRolesAccessStrategy
import org.opensearch.commons.authuser.User
import org.opensearch.test.OpenSearchTestCase

class SecureTransportActionTests : OpenSearchTestCase() {

    private class TestSecureTransportAction(
        override var filterByEnabled: Boolean = false,
        override var filterByAccessStrategy: String = FilterByBackendRolesAccessStrategy.INTERSECT.strategy
    ) : SecureTransportAction

    private val action = TestSecureTransportAction()

    private fun user(backendRoles: List<String>, roles: List<String> = listOf("some_role")) =
        User("test-user", backendRoles, roles, emptyMap())

    fun `test admin sees every backend role on the resource`() {
        val requester = user(listOf("admin-role"), listOf("all_access"))
        val resource = user(listOf("role-1", "role-2"))

        assertEquals(listOf("role-1", "role-2"), action.getVisibleBackendRoles(requester, resource))
    }

    fun `test null requester sees every backend role on the resource`() {
        // A null user means security is disabled or the caller is the super-admin; both see everything.
        val resource = user(listOf("role-1", "role-2"))

        assertEquals(listOf("role-1", "role-2"), action.getVisibleBackendRoles(null, resource))
    }

    fun `test non admin sees only the backend roles it belongs to`() {
        val requester = user(listOf("role-2", "role-3"))
        val resource = user(listOf("role-1", "role-2"))

        assertEquals(listOf("role-2"), action.getVisibleBackendRoles(requester, resource))
    }

    fun `test non admin with no shared backend roles sees none`() {
        val requester = user(listOf("role-3"))
        val resource = user(listOf("role-1", "role-2"))

        assertEquals(emptyList<String>(), action.getVisibleBackendRoles(requester, resource))
    }

    fun `test non admin with no backend roles sees none`() {
        val requester = user(emptyList())
        val resource = user(listOf("role-1", "role-2"))

        assertEquals(emptyList<String>(), action.getVisibleBackendRoles(requester, resource))
    }

    fun `test resource without a user has no backend roles to show`() {
        assertNull(action.getVisibleBackendRoles(user(listOf("role-1")), null))
    }

    fun `test filter by enabled does not change what is shown`() {
        val filteringAction = TestSecureTransportAction(filterByEnabled = true)
        val requester = user(listOf("role-2"))
        val resource = user(listOf("role-1", "role-2"))

        assertEquals(
            filteringAction.getVisibleBackendRoles(requester, resource),
            action.getVisibleBackendRoles(requester, resource)
        )
    }
}

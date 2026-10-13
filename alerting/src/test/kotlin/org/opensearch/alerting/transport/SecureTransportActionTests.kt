/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.settings.FilterByBackendRolesAccessStrategy
import org.opensearch.common.settings.ClusterSettings
import org.opensearch.common.settings.Settings
import org.opensearch.commons.authuser.User
import org.opensearch.test.OpenSearchTestCase

class SecureTransportActionTests : OpenSearchTestCase() {
    private val action = object : SecureTransportAction {
        override var filterByEnabled = true
        override var filterByAccessStrategy = FilterByBackendRolesAccessStrategy.ALL.strategy
    }

    fun `test empty exclusions preserve existing users`() {
        val user = User("alice", listOf("team", "everyone"), listOf("read"), mapOf("department" to "engineering"))
        assertSame(user, action.excludeBackendRoles(user, emptyList()))
        assertNull(action.excludeBackendRoles(null, listOf("everyone")))
        val emptyUser = User("", emptyList(), emptyList(), emptyMap())
        assertSame(emptyUser, action.excludeBackendRoles(emptyUser, listOf("everyone")))
    }

    fun `test exclusions change only copied backend roles`() {
        val user = User("alice", listOf("team", "everyone"), listOf("read"), mapOf("department" to "engineering"))
        val stored = action.excludeBackendRoles(user, listOf("everyone"))!!
        assertEquals(listOf("team"), stored.backendRoles)
        assertEquals(user.name, stored.name)
        assertEquals(user.roles, stored.roles)
        assertEquals(user.customAttributes, stored.customAttributes)
        assertEquals(listOf("team", "everyone"), user.backendRoles)
        assertNotSame(user, stored)
    }

    fun `test exclusions use exact case sensitive names`() {
        val user = User("alice", listOf("team", "Team", "team-other"), emptyList(), emptyMap())
        val stored = action.excludeBackendRoles(user, listOf("Team", "team-*", "missing"))!!
        assertEquals(listOf("team", "team-other"), stored.backendRoles)
    }

    fun `test exclusions apply to admin assigned roles too`() {
        val user = User("admin", listOf("team", "everyone"), listOf("all_access"), emptyMap())
        assertEquals(listOf("team"), action.excludeBackendRoles(user, listOf("everyone"))!!.backendRoles)
    }

    fun `test removing last role fails instead of broadening ALL access`() {
        assertTrue(action.doUserBackendRolesMatchResource(listOf("unrelated"), emptyList()))
        val user = User("alice", listOf("everyone"), emptyList(), emptyMap())
        val exception = expectThrows(IllegalArgumentException::class.java) {
            action.excludeBackendRoles(user, listOf("everyone"))
        }
        assertTrue(exception.message!!.contains("would remove all backend roles"))
        assertEquals(listOf("everyone"), user.backendRoles)
    }

    fun `test excluded roles setting is dynamic and deprecated`() {
        val setting = AlertingSettings.EXCLUDED_BACKEND_ROLES
        assertTrue(setting.isDynamic)
        assertTrue(setting.isDeprecated)
        assertTrue(setting.hasNodeScope())
        val clusterSettings = ClusterSettings(Settings.EMPTY, setOf(setting))
        assertEquals(emptyList<String>(), clusterSettings.get(setting))
        clusterSettings.applySettings(Settings.builder().putList(setting.key, "everyone").build())
        assertEquals(listOf("everyone"), clusterSettings.get(setting))
        clusterSettings.applySettings(Settings.EMPTY)
        assertEquals(emptyList<String>(), clusterSettings.get(setting))
        assertWarnings(
            "[plugins.alerting.excluded_backend_roles] setting was deprecated in OpenSearch and will be removed in a future release! " +
                "See the breaking changes documentation for the next major version."
        )
    }
}

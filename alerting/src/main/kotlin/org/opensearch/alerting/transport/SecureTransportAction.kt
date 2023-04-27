/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.ActionListener
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.util.AlertingException
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.commons.ConfigConstants
import org.opensearch.commons.authuser.User
import org.opensearch.rest.RestStatus

private val log = LogManager.getLogger(SecureTransportAction::class.java)

/**
 * TransportActon classes extend this interface to add filter-by-backend-roles functionality.
 *
 * 1. If filterBy is enabled
 *      a) Don't allow to create monitor/ destination (throw error) if the logged-on user has no backend roles configured.
 *
 * 2. If filterBy is enabled & monitors are created when filterBy is disabled:
 *      a) If backend_roles are saved with config, results will get filtered and data is shown
 *      b) If backend_roles are not saved with monitor config, results will get filtered and no monitors
 *         will be displayed.
 *      c) Users can edit and save the monitors to associate their backend_roles.
 *
 * 3. If filterBy is enabled & monitors are created by older version:
 *      a) No User details are present on monitor.
 *      b) No monitors will be displayed.
 *      c) Users can edit and save the monitors to associate their backend_roles.
 */
interface SecureTransportAction {

    var filterByEnabled: Boolean

    fun listenFilterBySettingChange(clusterService: ClusterService) {
        clusterService.clusterSettings.addSettingsUpdateConsumer(AlertingSettings.FILTER_BY_BACKEND_ROLES) { filterByEnabled = it }
    }

    fun readUserFromThreadContext(client: Client): User? {
        val userStr = client.threadPool().threadContext.getTransient<String>(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT)
        log.debug("User and roles string from thread context: $userStr")
        return User.parse(userStr)
    }

    fun doFilterForUser(user: User?): Boolean {
        log.debug("Is filterByEnabled: $filterByEnabled ; Is admin user: ${isAdmin(user)}")
        return if (isAdmin(user)) {
            false
        } else {
            filterByEnabled
        }
    }

    /**
     *  'all_access' role users are treated as admins.
     */
    fun isAdmin(user: User?): Boolean {
        return when {
            user == null -> {
                false
            }
            user.roles?.isNullOrEmpty() == true -> {
                false
            }
            else -> {
                user.roles?.contains("all_access") == true
            }
        }
    }

    fun <T : Any> validateUserBackendRoles(user: User?, actionListener: ActionListener<T>): Boolean {
        if (filterByEnabled) {
            if (user == null) {
                actionListener.onFailure(
                    AlertingException.wrap(
                        OpenSearchStatusException(
                            "Filter by user backend roles is enabled with security disabled.", RestStatus.FORBIDDEN
                        )
                    )
                )
                return false
            } else if (isAdmin(user)) {
                return true
            } else if (user.backendRoles.isNullOrEmpty()) {
                actionListener.onFailure(
                    AlertingException.wrap(
                        OpenSearchStatusException("User doesn't have backend roles configured. Contact administrator", RestStatus.FORBIDDEN)
                    )
                )
                return false
            }
        }
        return true
    }

    /**
     * If FilterBy is enabled, this function verifies that the requester user has FilterBy permissions to access
     * the resource. If FilterBy is disabled, we will assume the user has permissions and return true.
     *
     * This check will later to moved to the security plugin.
     */
    fun <T : Any> checkUserPermissionsWithResource(
        requesterUser: User?,
        resourceUser: User?,
        actionListener: ActionListener<T>,
        resourceType: String,
        resourceId: String
    ): Boolean {
        if (!doFilterForUser(requesterUser)) return true

        val resourceBackendRoles = resourceUser?.backendRoles
        val requesterBackendRoles = requesterUser?.backendRoles

        if (
            resourceBackendRoles == null ||
            requesterBackendRoles == null ||
            resourceBackendRoles.intersect(requesterBackendRoles).isEmpty()
        ) {
            actionListener.onFailure(
                AlertingException.wrap(
                    OpenSearchStatusException(
                        "Do not have permissions to resource, $resourceType, with id, $resourceId",
                        RestStatus.FORBIDDEN
                    )
                )
            )
            return false
        }
        return true
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.settings

/**
 * Defines the FilterByBackendRolesAccessStrategy
 */
enum class FilterByBackendRolesAccessStrategy(val strategy: String) {
    /**
     * Backend roles must be exactly equal to have access
     */
    EXACT("exact"),

    /**
     * Backend roles must intersect to have access
     */
    INTERSECT("intersect"),

    /**
     * User backend roles must contain all resource backend roles to have access
     */
    ALL("all"),
}

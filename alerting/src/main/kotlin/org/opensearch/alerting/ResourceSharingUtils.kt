/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.alerting

import org.opensearch.security.spi.resources.client.ResourceSharingClient

/**
 * Shared helpers for the resource-sharing framework.
 *
 * The [ResourceSharingClient] class is referenced only inside method bodies (never as a field type)
 * so this object can be class-loaded when the security plugin is absent without triggering
 * [NoClassDefFoundError]. Callers should invoke [shouldUseResourceAuthz] rather than reading the
 * accessor directly.
 */
internal object ResourceSharingUtils {

    /** Resource type registered by [AlertingResourceSharingExtension] for monitors and workflows. */
    const val MONITOR_RESOURCE_TYPE = "monitor"

    /**
     * Returns true only when the security plugin is loaded AND the resource-sharing feature is enabled
     * for [resourceType]. A non-null accessor client alone is insufficient — the plugin may be present
     * with the RSC feature flag disabled.
     */
    fun shouldUseResourceAuthz(resourceType: String = MONITOR_RESOURCE_TYPE): Boolean {
        val client = ResourceSharingClientAccessor.getResourceSharingClient() ?: return false
        return (client as ResourceSharingClient).isFeatureEnabledForType(resourceType)
    }
}

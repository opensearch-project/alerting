/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.alerting

import org.opensearch.security.spi.resources.client.ResourceSharingClient

/**
 * Accessor for resource sharing client.
 *
 * The internal field is typed as [Any?] so that loading this class does NOT trigger resolution of
 * [ResourceSharingClient] — which lives in the security-spi jar and is absent at runtime when the
 * security plugin is not installed. Callers that need methods on the client cast the result.
 */
object ResourceSharingClientAccessor {

    @Volatile
    private var client: Any? = null

    /**
     * Set the resource sharing client. Only called by [AlertingResourceSharingExtension.assignResourceSharingClient]
     * which is invoked by the security plugin — so [ResourceSharingClient] is guaranteed to be on the classpath
     * at that point.
     */
    @JvmStatic
    fun setResourceSharingClient(client: ResourceSharingClient?) {
        this.client = client
    }

    /**
     * Get the resource sharing client, or null if the security plugin is not loaded / resource sharing is disabled.
     * Returns [Any?] to avoid linking [ResourceSharingClient] in callers when the security plugin is absent.
     * Callers that need to invoke methods should cast: `getResourceSharingClient() as ResourceSharingClient`.
     */
    @JvmStatic
    fun getResourceSharingClient(): Any? = client

    /**
     * Clear the client (useful in tests).
     */
    @JvmStatic
    fun clear() {
        client = null
    }
}

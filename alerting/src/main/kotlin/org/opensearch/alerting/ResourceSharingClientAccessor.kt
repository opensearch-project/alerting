/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.alerting

import org.opensearch.security.spi.resources.client.ResourceSharingClient

/**
 * Accessor for resource sharing client
 */
object ResourceSharingClientAccessor {

    @Volatile
    private var client: ResourceSharingClient? = null

    /**
     * Set the resource sharing client
     */
    @JvmStatic
    fun setResourceSharingClient(client: ResourceSharingClient?) {
        this.client = client
    }

    /**
     * Get the resource sharing client (nullable to mirror Java)
     */
    @JvmStatic
    fun getResourceSharingClient(): ResourceSharingClient? = client

    /**
     * Optional: clear the client (useful in tests)
     */
    @JvmStatic
    fun clear() {
        client = null
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.junit.Before
import org.mockito.Mockito.mock
import org.opensearch.security.spi.resources.client.ResourceSharingClient
import org.opensearch.test.OpenSearchTestCase

class ResourceSharingClientAccessorTests : OpenSearchTestCase() {

    @Before
    fun setup() {
        ResourceSharingClientAccessor.clear()
    }

    fun `test get client returns null when not set`() {
        assertNull(ResourceSharingClientAccessor.getResourceSharingClient())
    }

    fun `test set and get client`() {
        val mockClient = mock(ResourceSharingClient::class.java)
        ResourceSharingClientAccessor.setResourceSharingClient(mockClient)
        assertSame(mockClient, ResourceSharingClientAccessor.getResourceSharingClient())
    }

    fun `test clear resets client to null`() {
        val mockClient = mock(ResourceSharingClient::class.java)
        ResourceSharingClientAccessor.setResourceSharingClient(mockClient)
        ResourceSharingClientAccessor.clear()
        assertNull(ResourceSharingClientAccessor.getResourceSharingClient())
    }

    fun `test set null client`() {
        val mockClient = mock(ResourceSharingClient::class.java)
        ResourceSharingClientAccessor.setResourceSharingClient(mockClient)
        ResourceSharingClientAccessor.setResourceSharingClient(null)
        assertNull(ResourceSharingClientAccessor.getResourceSharingClient())
    }
}

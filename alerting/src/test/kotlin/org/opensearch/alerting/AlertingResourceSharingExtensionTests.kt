/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.junit.Before
import org.mockito.Mockito.mock
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.security.spi.resources.client.ResourceSharingClient
import org.opensearch.test.OpenSearchTestCase

class AlertingResourceSharingExtensionTests : OpenSearchTestCase() {

    private lateinit var extension: AlertingResourceSharingExtension

    @Before
    fun setup() {
        extension = AlertingResourceSharingExtension()
        ResourceSharingClientAccessor.clear()
    }

    fun `test getResourceProviders returns one provider`() {
        val providers = extension.getResourceProviders()
        assertEquals(1, providers.size)
    }

    fun `test monitor provider has correct type and index`() {
        val providers = extension.getResourceProviders()
        val monitorProvider = providers.first { it.resourceType() == "monitor" }
        assertEquals(ScheduledJob.SCHEDULED_JOBS_INDEX, monitorProvider.resourceIndexName())
    }

    fun `test assignResourceSharingClient sets client in accessor`() {
        val mockClient = mock(ResourceSharingClient::class.java)
        extension.assignResourceSharingClient(mockClient)
        assertSame(mockClient, ResourceSharingClientAccessor.getResourceSharingClient())
    }

    fun `test assignResourceSharingClient with null`() {
        val mockClient = mock(ResourceSharingClient::class.java)
        extension.assignResourceSharingClient(mockClient)
        extension.assignResourceSharingClient(null)
        assertNull(ResourceSharingClientAccessor.getResourceSharingClient())
    }
}

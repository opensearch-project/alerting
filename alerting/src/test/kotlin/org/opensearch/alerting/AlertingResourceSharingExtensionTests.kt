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

    fun `test getResourceProviders registers monitor and workflow`() {
        val providers = extension.getResourceProviders()
        val types = providers.map { it.resourceType() }.toSet()
        assertEquals(setOf("monitor", "workflow"), types)
    }

    fun `test monitor provider declares nested typeField and owner paths`() {
        val monitorProvider = extension.getResourceProviders().first { it.resourceType() == "monitor" }
        assertEquals(ScheduledJob.SCHEDULED_JOBS_INDEX, monitorProvider.resourceIndexName())
        assertEquals("monitor.type", monitorProvider.typeField())
        assertEquals("/monitor/user/name", monitorProvider.ownerNamePath())
        assertEquals("/monitor/user/backend_roles", monitorProvider.ownerBackendRolesPath())
    }

    fun `test workflow provider declares nested typeField and owner paths`() {
        val workflowProvider = extension.getResourceProviders().first { it.resourceType() == "workflow" }
        assertEquals(ScheduledJob.SCHEDULED_JOBS_INDEX, workflowProvider.resourceIndexName())
        assertEquals("workflow.type", workflowProvider.typeField())
        assertEquals("/workflow/user/name", workflowProvider.ownerNamePath())
        assertEquals("/workflow/user/backend_roles", workflowProvider.ownerBackendRolesPath())
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

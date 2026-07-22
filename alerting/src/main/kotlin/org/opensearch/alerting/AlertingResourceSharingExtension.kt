/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.alerting.model.ScheduledJob.Companion.SCHEDULED_JOBS_INDEX
import org.opensearch.security.spi.resources.ResourceProvider
import org.opensearch.security.spi.resources.ResourceSharingExtension
import org.opensearch.security.spi.resources.client.ResourceSharingClient

class AlertingResourceSharingExtension : ResourceSharingExtension {
    /**
     * Monitors and workflows share [SCHEDULED_JOBS_INDEX], distinguished by the top-level
     * [ScheduledJob.RESOURCE_TYPE_FIELD] field on each document (values "monitor" / "workflow").
     * The security plugin reads that field via [ResourceProvider.typeField] to route write
     * operations to the correct provider.
     */
    override fun getResourceProviders(): Set<ResourceProvider> {
        return setOf(
            object : ResourceProvider {
                override fun resourceType(): String = ResourceSharingUtils.MONITOR_RESOURCE_TYPE
                override fun resourceIndexName(): String = SCHEDULED_JOBS_INDEX
                override fun typeField(): String = ScheduledJob.RESOURCE_TYPE_FIELD
            },
            object : ResourceProvider {
                override fun resourceType(): String = ResourceSharingUtils.WORKFLOW_RESOURCE_TYPE
                override fun resourceIndexName(): String = SCHEDULED_JOBS_INDEX
                override fun typeField(): String = ScheduledJob.RESOURCE_TYPE_FIELD
            }
        )
    }

    override fun assignResourceSharingClient(client: ResourceSharingClient?) {
        ResourceSharingClientAccessor.setResourceSharingClient(client)
    }
}

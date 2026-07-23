/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.opensearch.commons.alerting.model.ScheduledJob.Companion.SCHEDULED_JOBS_INDEX
import org.opensearch.security.spi.resources.ResourceProvider
import org.opensearch.security.spi.resources.ResourceSharingExtension
import org.opensearch.security.spi.resources.client.ResourceSharingClient

class AlertingResourceSharingExtension : ResourceSharingExtension {
    /**
     * Monitors and workflows share [SCHEDULED_JOBS_INDEX]. Each provider declares its own
     * type-specific Lucene field path (`monitor.type` / `workflow.type`) for
     * [ResourceProvider.typeField]. The security plugin iterates matching providers and picks
     * the first one whose typeField extraction yields a non-null value — so monitor docs match
     * the monitor provider (`monitor.type` present, `workflow.type` absent), workflow docs match
     * the workflow provider, and neither side needs a top-level discriminator field on the doc.
     */
    override fun getResourceProviders(): Set<ResourceProvider> {
        return setOf(
            object : ResourceProvider {
                override fun resourceType(): String = ResourceSharingUtils.MONITOR_RESOURCE_TYPE
                override fun resourceIndexName(): String = SCHEDULED_JOBS_INDEX
                override fun typeField(): String = "monitor.type"
            },
            object : ResourceProvider {
                override fun resourceType(): String = ResourceSharingUtils.WORKFLOW_RESOURCE_TYPE
                override fun resourceIndexName(): String = SCHEDULED_JOBS_INDEX
                override fun typeField(): String = "workflow.type"
            }
        )
    }

    override fun assignResourceSharingClient(client: ResourceSharingClient?) {
        ResourceSharingClientAccessor.setResourceSharingClient(client)
    }
}

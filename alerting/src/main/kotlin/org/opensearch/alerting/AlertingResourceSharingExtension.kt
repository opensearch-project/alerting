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
     * type-specific field paths so the security plugin can distinguish them without a
     * top-level discriminator on the stored doc:
     *
     *  - [ResourceProvider.typeField] — used by the postIndex hook to classify new writes.
     *    `monitor.type` is present on monitor docs and absent on workflow docs (and vice versa),
     *    so the framework iterates matching providers and picks the one whose typeField
     *    resolves non-null.
     *  - [ResourceProvider.ownerNamePath] / [ResourceProvider.ownerBackendRolesPath] — used by
     *    the security plugin's `POST /_plugins/_security/api/resources/migrate` endpoint when
     *    seeding sharing entries for legacy docs, so a single admin call can attribute owners
     *    across both types without pre-processing.
     */
    override fun getResourceProviders(): Set<ResourceProvider> {
        return setOf(
            object : ResourceProvider {
                override fun resourceType(): String = ResourceSharingUtils.MONITOR_RESOURCE_TYPE
                override fun resourceIndexName(): String = SCHEDULED_JOBS_INDEX
                override fun typeField(): String = "monitor.type"
                override fun ownerNamePath(): String = "/monitor/user/name"
                override fun ownerBackendRolesPath(): String = "/monitor/user/backend_roles"
            },
            object : ResourceProvider {
                override fun resourceType(): String = ResourceSharingUtils.WORKFLOW_RESOURCE_TYPE
                override fun resourceIndexName(): String = SCHEDULED_JOBS_INDEX
                override fun typeField(): String = "workflow.type"
                override fun ownerNamePath(): String = "/workflow/user/name"
                override fun ownerBackendRolesPath(): String = "/workflow/user/backend_roles"
            }
        )
    }

    override fun assignResourceSharingClient(client: ResourceSharingClient?) {
        ResourceSharingClientAccessor.setResourceSharingClient(client)
    }
}

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
    override fun getResourceProviders(): Set<ResourceProvider> {
        return setOf(
            object : ResourceProvider {
                override fun resourceType(): String = "monitor"
                override fun resourceIndexName(): String = SCHEDULED_JOBS_INDEX
            }
        )
    }

    override fun assignResourceSharingClient(client: ResourceSharingClient?) {
        ResourceSharingClientAccessor.setResourceSharingClient(client)
    }
}

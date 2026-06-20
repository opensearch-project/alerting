/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.opensearch.alerting.alerts.AlertIndices.Companion.ALL_ALERT_INDEX_PATTERN
import org.opensearch.alerting.comments.CommentsIndices.Companion.ALL_COMMENTS_INDEX_PATTERN
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
            },
            object : ResourceProvider {
                override fun resourceType(): String = "alert"
                override fun resourceIndexName(): String = ALL_ALERT_INDEX_PATTERN
            },
            object : ResourceProvider {
                override fun resourceType(): String = "comment"
                override fun resourceIndexName(): String = ALL_COMMENTS_INDEX_PATTERN
            }
        )
    }

    override fun assignResourceSharingClient(client: ResourceSharingClient?) {
        ResourceSharingClientAccessor.setResourceSharingClient(client)
    }
}

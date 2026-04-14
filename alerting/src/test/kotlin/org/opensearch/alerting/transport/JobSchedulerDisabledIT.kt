/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import org.opensearch.alerting.core.action.node.ScheduledJobsStatsAction
import org.opensearch.alerting.core.action.node.ScheduledJobsStatsRequest
import org.opensearch.common.settings.Settings
import org.opensearch.commons.alerting.model.ScheduledJob

class JobSchedulerDisabledIT : AlertingSingleNodeTestCase() {

    // Set ALTERING_MULTI_TENANCY_ENABLED config as true
    override fun nodeSettings(): Settings {
        return Settings.builder()
            .put(super.nodeSettings())
            .put("plugins.alerting.multi_tenancy_enabled", true)
            .build()
    }

    fun `test node starts successfully with multi tenancy enabled`() {
        val clusterHealth = client().admin().cluster().prepareHealth().get()
        assertNotNull("Cluster health should not be null", clusterHealth)
    }

    fun `test scheduled job index not created when multi tenancy enabled`() {
        val clusterState = client().admin().cluster().prepareState().get().state
        assertFalse(
            "Scheduled job index should not exist when multi tenancy is enabled",
            clusterState.routingTable.hasIndex(ScheduledJob.SCHEDULED_JOBS_INDEX)
        )
    }

    fun `test lock index not created when multi tenancy enabled`() {
        val clusterState = client().admin().cluster().prepareState().get().state
        assertFalse(
            "Lock index should not exist when sweeper is disabled",
            clusterState.routingTable.hasIndex(".opensearch-alerting-config-lock")
        )
    }

    fun `test no jobs scheduled when multi tenancy enabled`() {
        val statsRequest = ScheduledJobsStatsRequest(arrayOf()).all()
        val statsResponse = client().execute(ScheduledJobsStatsAction.INSTANCE, statsRequest).get()

        assertNotNull("Stats response should not be null", statsResponse)
        for (nodeStats in statsResponse.nodes) {
            assertEquals(
                "Node should have zero scheduled jobs when sweeper is disabled",
                0,
                nodeStats.jobInfos?.size ?: 0
            )
        }
    }
}

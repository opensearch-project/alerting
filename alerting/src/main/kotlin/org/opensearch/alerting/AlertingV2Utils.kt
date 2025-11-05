/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.apache.lucene.search.TotalHits
import org.apache.lucene.search.TotalHits.Relation
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.search.ShardSearchFailure
import org.opensearch.alerting.AlertingPlugin.Companion.MONITOR_BASE_URI
import org.opensearch.alerting.AlertingPlugin.Companion.MONITOR_V2_BASE_URI
import org.opensearch.alerting.modelv2.MonitorV2
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.alerting.model.Workflow
import org.opensearch.index.IndexNotFoundException
import org.opensearch.search.SearchHits
import org.opensearch.search.aggregations.InternalAggregations
import org.opensearch.search.internal.InternalSearchResponse
import org.opensearch.search.profile.SearchProfileShardResults
import org.opensearch.search.suggest.Suggest
import org.opensearch.transport.RemoteTransportException
import java.util.Collections

object AlertingV2Utils {
    // Validates that the given scheduled job is a Monitor
    // returns the exception to pass into actionListener.onFailure if not.
    fun validateMonitorV1(scheduledJob: ScheduledJob): Exception? {
        if (scheduledJob is MonitorV2) {
            return IllegalStateException(
                "The ID given corresponds to an Alerting V2 Monitor, but a V1 Monitor was expected. " +
                    "If you wish to operate on a V2 Monitor (e.g. PPL Monitor), please use " +
                    "the Alerting V2 APIs with endpoint prefix: $MONITOR_V2_BASE_URI."
            )
        } else if (scheduledJob !is Monitor && scheduledJob !is Workflow) {
            return IllegalStateException(
                "The ID given corresponds to a scheduled job of unknown type: ${scheduledJob.javaClass.name}. " +
                    "Please validate the ID and ensure it corresponds to a valid Monitor."
            )
        }
        return null
    }

    // Validates that the given scheduled job is a MonitorV2
    // returns the exception to pass into actionListener.onFailure if not.
    fun validateMonitorV2(scheduledJob: ScheduledJob): Exception? {
        if (scheduledJob is Monitor || scheduledJob is Workflow) {
            return IllegalStateException(
                "The ID given corresponds to an Alerting V1 Monitor, but a V2 Monitor was expected. " +
                    "If you wish to operate on a V1 Monitor (e.g. Per Query, Per Document, etc), please use " +
                    "the Alerting V1 APIs with endpoint prefix: $MONITOR_BASE_URI."
            )
        } else if (scheduledJob !is MonitorV2) {
            return IllegalStateException(
                "The ID given corresponds to a scheduled job of unknown type: ${scheduledJob.javaClass.name}. " +
                    "Please validate the ID and ensure it corresponds to a valid Monitor."
            )
        }
        return null
    }

    // Checks if the exception is caused by an IndexNotFoundException (directly or nested).
    // Used in Get and Search monitor functionalities to determine whether a "no results"
    // response should be returned
    fun isIndexNotFoundException(e: Exception): Boolean {
        if (e is IndexNotFoundException) {
            return true
        }
        if (e is RemoteTransportException) {
            val cause = e.cause
            if (cause is IndexNotFoundException) {
                return true
            }
        }
        return false
    }

    // Used in Get and Search monitor functionalities to return a "no results" response
    fun getEmptySearchResponse(): SearchResponse {
        val internalSearchResponse = InternalSearchResponse(
            SearchHits(emptyArray(), TotalHits(0L, Relation.EQUAL_TO), 0.0f),
            InternalAggregations.from(Collections.emptyList()),
            Suggest(Collections.emptyList()),
            SearchProfileShardResults(Collections.emptyMap()),
            false,
            false,
            0
        )

        return SearchResponse(
            internalSearchResponse,
            "",
            0,
            0,
            0,
            0,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        )
    }
}

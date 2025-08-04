/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.apache.lucene.search.TotalHits
import org.apache.lucene.search.TotalHits.Relation
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.search.ShardSearchFailure
import org.opensearch.index.IndexNotFoundException
import org.opensearch.search.SearchHits
import org.opensearch.search.aggregations.InternalAggregations
import org.opensearch.search.internal.InternalSearchResponse
import org.opensearch.search.profile.SearchProfileShardResults
import org.opensearch.search.suggest.Suggest
import org.opensearch.transport.RemoteTransportException
import java.util.Collections

/**
 * Some util functions that were initially created for Alerting V1, and are leveraged by
 * both Alerting V1 and V2
 */
object AlertingV1Utils {
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

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import org.opensearch.test.OpenSearchTestCase

class DocLevelMonitorQueriesTests : OpenSearchTestCase() {

    fun `test query indexing failures message lists every failure up to the limit`() {
        val failures = listOf(
            DocLevelQueryIndexingFailure("index-a", "q1", "No field mapping can be found for the field with name [f1]"),
            DocLevelQueryIndexingFailure("index-b", "q2", "No field mapping can be found for the field with name [f2]")
        )

        val message = DocLevelMonitorQueries.queryIndexingFailuresMessage("monitor-1", failures)

        assertEquals(
            "Monitor [monitor-1] failed to install [2] doc level queries, which are not evaluated: " +
                "[query: q1, index: index-a, reason: No field mapping can be found for the field with name [f1]], " +
                "[query: q2, index: index-b, reason: No field mapping can be found for the field with name [f2]]",
            message
        )
    }

    fun `test query indexing failures message truncates failures beyond the limit`() {
        val limit = DocLevelMonitorQueries.MAX_REPORTED_QUERY_INDEXING_FAILURES
        val failures = (1..limit + 3).map { DocLevelQueryIndexingFailure("index-a", "q$it", "reason-$it") }

        val message = DocLevelMonitorQueries.queryIndexingFailuresMessage("monitor-1", failures)

        assertTrue(message.startsWith("Monitor [monitor-1] failed to install [${limit + 3}] doc level queries"))
        (1..limit).forEach { assertTrue(message.contains("[query: q$it, index: index-a, reason: reason-$it]")) }
        assertFalse(message.contains("q${limit + 1},"))
        assertTrue(message.endsWith(" and [3] more"))
    }
}

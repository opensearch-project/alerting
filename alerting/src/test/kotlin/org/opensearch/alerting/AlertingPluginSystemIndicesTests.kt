/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.apache.lucene.util.automaton.Operations
import org.opensearch.alerting.core.lock.LockService
import org.opensearch.common.regex.Regex
import org.opensearch.common.settings.Settings
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.test.OpenSearchTestCase

class AlertingPluginSystemIndicesTests : OpenSearchTestCase() {

    private val descriptors = AlertingPlugin().getSystemIndexDescriptors(Settings.EMPTY)

    fun `test doc-level query index and its rollovers are system indices`() {
        val alias = ScheduledJob.DOC_LEVEL_QUERIES_INDEX
        assertEquals(".opensearch-alerting-queries", alias)
        // the alias name was a concrete index before the query index moved behind an alias
        listOf(alias, "$alias-000001", "$alias-000002").forEach { assertSingleDescriptorFor(it) }
    }

    fun `test job lock index is a system index`() {
        assertEquals(".opensearch-alerting-config-lock", LockService.LOCK_INDEX_NAME)
        assertSingleDescriptorFor(LockService.LOCK_INDEX_NAME)
    }

    fun `test query indices that a caller names in data sources are not covered`() {
        assertTrue(descriptors.none { it.matchesIndexPattern(".opensearch-sap-windows-detectors-queries-000001") })
    }

    // Core refuses to start a node whose system index patterns overlap, so this mirrors its check.
    fun `test descriptors do not overlap`() {
        descriptors.forEach { a ->
            descriptors.filter { it !== a }.forEach { b ->
                val intersection = Operations.intersection(
                    Regex.simpleMatchToAutomaton(a.indexPattern),
                    Regex.simpleMatchToAutomaton(b.indexPattern)
                )
                assertTrue("[${a.indexPattern}] overlaps [${b.indexPattern}]", Operations.isEmpty(intersection))
            }
        }
    }

    private fun assertSingleDescriptorFor(index: String) {
        val matching = descriptors.filter { it.matchesIndexPattern(index) }
        assertEquals("descriptors matching [$index]: ${matching.map { it.indexPattern }}", 1, matching.size)
    }
}

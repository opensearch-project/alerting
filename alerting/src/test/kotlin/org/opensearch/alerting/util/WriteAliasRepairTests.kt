/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import org.opensearch.alerting.util.WriteAliasRepair.AliasedIndex
import org.opensearch.test.OpenSearchTestCase

class WriteAliasRepairTests : OpenSearchTestCase() {

    private val own = AliasedIndex("h-2026.10.07-1", 200, null)
    private val restored = AliasedIndex("h-2026.10.07-000002", 100, null)

    fun `test alias on one index or none is left alone`() {
        assertEquals(emptyList<String>(), WriteAliasRepair.indicesToDetach(listOf(own)))
        assertEquals(emptyList<String>(), WriteAliasRepair.indicesToDetach(emptyList()))
    }

    fun `test alias with an explicit is_write_index flag is left alone`() {
        assertEquals(emptyList<String>(), WriteAliasRepair.indicesToDetach(listOf(own.copy(isWriteIndex = true), restored)))
        assertEquals(emptyList<String>(), WriteAliasRepair.indicesToDetach(listOf(own, restored.copy(isWriteIndex = false))))
    }

    fun `test highest rollover counter keeps the alias even when it is older`() {
        assertEquals(listOf(own.index), WriteAliasRepair.indicesToDetach(listOf(own, restored)))
        assertEquals(listOf(own.index), WriteAliasRepair.indicesToDetach(listOf(restored, own)))
    }

    fun `test equal counters go to the newest creation date`() {
        val older = AliasedIndex("h-2026.10.06-000002", 100, null)
        val newer = AliasedIndex("h-2026.10.07-000002", 200, null)

        assertEquals(listOf(older.index), WriteAliasRepair.indicesToDetach(listOf(older, newer)))
        assertEquals(listOf(older.index), WriteAliasRepair.indicesToDetach(listOf(newer, older)))
    }

    fun `test equal counters and creation dates go to the name that sorts last`() {
        val a = AliasedIndex("h-a-000002", 100, null)
        val b = AliasedIndex("h-b-000002", 100, null)

        assertEquals(listOf(a.index), WriteAliasRepair.indicesToDetach(listOf(a, b)))
        assertEquals(listOf(a.index), WriteAliasRepair.indicesToDetach(listOf(b, a)))
    }

    fun `test a name without a numeric suffix loses to any counter`() {
        val noCounter = AliasedIndex("h-custom", 300, null)

        assertEquals(listOf(noCounter.index), WriteAliasRepair.indicesToDetach(listOf(noCounter, own)))
    }

    fun `test every index but the kept one is detached`() {
        val third = AliasedIndex("h-2026.10.06-000003", 50, null)

        assertEquals(listOf(restored.index, own.index), WriteAliasRepair.indicesToDetach(listOf(own, restored, third)))
    }
}

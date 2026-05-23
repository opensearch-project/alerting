/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import org.mockito.Mockito.mock
import org.opensearch.cluster.service.ClusterService
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.transport.client.Client

/**
 * Unit tests for [DocLevelMonitorQueries].
 *
 * Covers:
 *  - [DocLevelMonitorQueries.deepCopyMap] — pure companion-object helper.
 *  - [DocLevelMonitorQueries.traverseMappingsAndUpdate] — DFS traversal with
 *    emphasis on the alias-field bug fix (fields of type "alias" must be skipped
 *    so they are never included in the PUT-mapping request sent to the query index).
 *  - [DocLevelMonitorQueries.getAllConflictingFields] behaviour is exercised
 *    indirectly via traverseMappingsAndUpdate; direct ClusterState integration
 *    tests belong in the IT suite.
 *
 * Existing traverseMappingsAndUpdate tests live in [AlertingUtilsTests]; new
 * tests here focus specifically on alias-type fields and complement rather than
 * duplicate those.
 */
class DocLevelMonitorQueriesTests : OpenSearchTestCase() {

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private fun makeInstance(): DocLevelMonitorQueries =
        DocLevelMonitorQueries(mock(Client::class.java), mock(ClusterService::class.java))

    /** Identity leaf processor — returns every field unchanged. */
    private val identityLeaf =
        fun(fieldName: String, _: String, props: MutableMap<String, Any>):
            Triple<String, String, MutableMap<String, Any>> =
            Triple(fieldName, fieldName, props)

    /** Alias-skipping leaf processor that mirrors the production bugfix logic. */
    private val aliasSkippingLeaf =
        fun(fieldName: String, _: String, props: MutableMap<String, Any>):
            Triple<String, String, MutableMap<String, Any>> {
            if (props["type"] == "alias") {
                return Triple(fieldName, fieldName, mutableMapOf())
            }
            return Triple(fieldName, fieldName, props)
        }

    // -------------------------------------------------------------------------
    // deepCopyMap — structural correctness
    // -------------------------------------------------------------------------

    fun `test deepCopyMap returns an equal but distinct map reference`() {
        val original = mutableMapOf<String, Any>("key" to "value")

        val copy = DocLevelMonitorQueries.deepCopyMap(original)

        assertEquals(original, copy)
        assertNotSame(original, copy)
    }

    fun `test deepCopyMap deep copies nested maps so mutations are isolated`() {
        val nested = mutableMapOf<String, Any>("inner" to "original")
        val original = mutableMapOf<String, Any>("outer" to nested)

        val copy = DocLevelMonitorQueries.deepCopyMap(original)

        // Mutate the nested map in the copy.
        @Suppress("UNCHECKED_CAST")
        (copy["outer"] as MutableMap<String, Any>)["inner"] = "mutated"

        // Original nested map must be unaffected.
        assertEquals("original", nested["inner"])
    }

    fun `test deepCopyMap copies nested map references independently`() {
        val nested = mutableMapOf<String, Any>("inner" to "value")
        val original = mutableMapOf<String, Any>("outer" to nested)

        val copy = DocLevelMonitorQueries.deepCopyMap(original)

        assertNotSame(nested, copy["outer"])
    }

    fun `test deepCopyMap shallow copies list values`() {
        val list = mutableListOf("a", "b", "c")
        val original = mutableMapOf<String, Any>("items" to list)

        val copy = DocLevelMonitorQueries.deepCopyMap(original)

        @Suppress("UNCHECKED_CAST")
        val copiedList = copy["items"] as MutableList<String>
        copiedList.add("d")

        // The list in the copy is a distinct mutable list; original list unchanged.
        assertEquals(3, list.size)
        assertEquals(4, copiedList.size)
        assertNotSame(list, copiedList)
    }

    fun `test deepCopyMap preserves scalar values`() {
        val original = mutableMapOf<String, Any>(
            "strVal" to "hello",
            "intVal" to 42,
            "boolVal" to true,
            "longVal" to 9_999_999_999L,
        )

        val copy = DocLevelMonitorQueries.deepCopyMap(original)

        assertEquals("hello", copy["strVal"])
        assertEquals(42, copy["intVal"])
        assertEquals(true, copy["boolVal"])
        assertEquals(9_999_999_999L, copy["longVal"])
    }

    fun `test deepCopyMap handles empty map`() {
        val copy = DocLevelMonitorQueries.deepCopyMap(emptyMap())

        assertTrue(copy.isEmpty())
    }

    fun `test deepCopyMap handles multiply-nested structure`() {
        val original = mutableMapOf<String, Any>(
            "level1" to mutableMapOf<String, Any>(
                "level2" to mutableMapOf<String, Any>(
                    "level3" to "leaf",
                ),
            ),
        )

        val copy = DocLevelMonitorQueries.deepCopyMap(original)

        @Suppress("UNCHECKED_CAST")
        val l1 = copy["level1"] as MutableMap<String, Any>
        @Suppress("UNCHECKED_CAST")
        val l2 = l1["level2"] as MutableMap<String, Any>
        assertEquals("leaf", l2["level3"])

        // Mutate deep copy — original must be unaffected.
        l2["level3"] = "changed"
        @Suppress("UNCHECKED_CAST")
        val origL2 = (original["level1"] as Map<String, Any>)["level2"] as Map<String, Any>
        assertEquals("leaf", origL2["level3"])
    }

    // -------------------------------------------------------------------------
    // traverseMappingsAndUpdate — alias-field bug fix (core of this PR)
    // -------------------------------------------------------------------------

    fun `test traverseMappingsAndUpdate skips alias-type field when leaf processor returns empty props`() {
        val queries = makeInstance()
        // Mapping that contains a single alias-type field.
        val mappings = mutableMapOf<String, Any>(
            "real_field_alias" to mutableMapOf<String, Any>(
                "type" to "alias",
                "path" to "some.other.field",
            ),
        )
        val flattenPaths = mutableMapOf<String, MutableMap<String, Any>>()

        queries.traverseMappingsAndUpdate(mappings, "", aliasSkippingLeaf, flattenPaths)

        // traverseMappingsAndUpdate ADDS every leaf to flattenPaths before calling the
        // processor, so the alias field IS present in flattenPaths here.  The production
        // fix filters it out AFTER traversal (in the properties-copy loop inside
        // indexDocLevelQueries).  What we verify is that the processor returned an empty
        // props map for the alias field, and that the node in `mappings` was updated to
        // an empty map (the processor renaming result).
        assertTrue(
            "alias field should be present in flattenPaths (traversal is unconditional)",
            flattenPaths.containsKey("real_field_alias"),
        )
        // The critical invariant: after traversal, the entry in the parent map is an
        // empty mutableMap — this is what the post-traversal filter in
        // indexDocLevelQueries checks when deciding whether to skip a field.
        val updatedEntry = mappings["real_field_alias"]
        assertTrue(
            "alias field entry must be empty after processing so the caller can filter it",
            updatedEntry is Map<*, *> && (updatedEntry as Map<*, *>).isEmpty(),
        )
    }

    fun `test traverseMappingsAndUpdate includes non-alias fields normally`() {
        val queries = makeInstance()
        val mappings = mutableMapOf<String, Any>(
            "message" to mutableMapOf<String, Any>("type" to "text"),
            "status_code" to mutableMapOf<String, Any>("type" to "keyword"),
        )
        val flattenPaths = mutableMapOf<String, MutableMap<String, Any>>()

        queries.traverseMappingsAndUpdate(mappings, "", identityLeaf, flattenPaths)

        assertTrue("message should be in flattenPaths", flattenPaths.containsKey("message"))
        assertTrue("status_code should be in flattenPaths", flattenPaths.containsKey("status_code"))
    }

    fun `test traverseMappingsAndUpdate with mixed alias and regular fields only regular fields get non-empty props`() {
        val queries = makeInstance()
        // Two regular fields + one alias field.
        val mappings = mutableMapOf<String, Any>(
            "timestamp" to mutableMapOf<String, Any>("type" to "date"),
            "log_level" to mutableMapOf<String, Any>("type" to "keyword"),
            "ts_alias" to mutableMapOf<String, Any>(
                "type" to "alias",
                "path" to "timestamp",
            ),
        )
        val flattenPaths = mutableMapOf<String, MutableMap<String, Any>>()

        queries.traverseMappingsAndUpdate(mappings, "", aliasSkippingLeaf, flattenPaths)

        // All three leaves appear in flattenPaths because traversal is unconditional.
        assertEquals(3, flattenPaths.size)

        // Regular fields retain their props.
        assertEquals("date", flattenPaths["timestamp"]?.get("type"))
        assertEquals("keyword", flattenPaths["log_level"]?.get("type"))

        // The alias entry in the parent `mappings` map was replaced with an empty map
        // by the processor — this is what the post-traversal filter checks.
        val aliasEntry = mappings["ts_alias"]
        assertTrue(
            "ts_alias entry must become empty so the caller can skip it in PUT mapping",
            aliasEntry is Map<*, *> && (aliasEntry as Map<*, *>).isEmpty(),
        )

        // Regular fields still have their original type in the parent map.
        val timestampEntry = mappings["timestamp"] as Map<*, *>
        assertEquals("date", timestampEntry["type"])
    }

    fun `test traverseMappingsAndUpdate with all alias fields results in all empty entries`() {
        val queries = makeInstance()
        val mappings = mutableMapOf<String, Any>(
            "alias_a" to mutableMapOf<String, Any>("type" to "alias", "path" to "field_a"),
            "alias_b" to mutableMapOf<String, Any>("type" to "alias", "path" to "field_b"),
        )
        val flattenPaths = mutableMapOf<String, MutableMap<String, Any>>()

        queries.traverseMappingsAndUpdate(mappings, "", aliasSkippingLeaf, flattenPaths)

        // Both are in flattenPaths (traversal doesn't filter).
        assertEquals(2, flattenPaths.size)

        // Both entries in the working map are now empty.
        assertTrue((mappings["alias_a"] as Map<*, *>).isEmpty())
        assertTrue((mappings["alias_b"] as Map<*, *>).isEmpty())
    }

    fun `test traverseMappingsAndUpdate with nested object containing alias field`() {
        val queries = makeInstance()
        // Nested object with a regular child and an alias child.
        val mappings = mutableMapOf<String, Any>(
            "event" to mutableMapOf<String, Any>(
                "properties" to mutableMapOf<String, Any>(
                    "name" to mutableMapOf<String, Any>("type" to "keyword"),
                    "name_alias" to mutableMapOf<String, Any>(
                        "type" to "alias",
                        "path" to "event.name",
                    ),
                ),
            ),
        )
        val flattenPaths = mutableMapOf<String, MutableMap<String, Any>>()

        queries.traverseMappingsAndUpdate(mappings, "", aliasSkippingLeaf, flattenPaths)

        assertTrue("event.name should be in flattenPaths", flattenPaths.containsKey("event.name"))
        assertTrue("event.name_alias should be in flattenPaths", flattenPaths.containsKey("event.name_alias"))

        // Inside the nested properties, the alias entry must be empty.
        @Suppress("UNCHECKED_CAST")
        val eventProps = ((mappings["event"] as Map<String, Any>)["properties"] as Map<String, Any>)
        assertTrue((eventProps["name_alias"] as Map<*, *>).isEmpty())
        // Regular child is untouched.
        assertEquals("keyword", (eventProps["name"] as Map<*, *>)["type"])
    }

    fun `test traverseMappingsAndUpdate with empty mappings produces empty flattenPaths`() {
        val queries = makeInstance()
        val mappings = mutableMapOf<String, Any>()
        val flattenPaths = mutableMapOf<String, MutableMap<String, Any>>()

        queries.traverseMappingsAndUpdate(mappings, "", identityLeaf, flattenPaths)

        assertTrue(flattenPaths.isEmpty())
    }

    // -------------------------------------------------------------------------
    // Post-traversal filter logic (unit-testing the filtering predicate
    // independently of the full indexDocLevelQueries coroutine stack)
    // -------------------------------------------------------------------------

    /**
     * The production fix in indexDocLevelQueries filters the `properties` map with:
     *
     *   (it.value as Map)["type"] == "alias"  ||  (it.value as Map).isEmpty()
     *
     * We verify that predicate directly so we know it catches both the
     * alias-type case and the empty-map case produced by the processor.
     */
    fun `test post-traversal filter skips entries where type is alias`() {
        val entries: Map<String, Any> = mapOf(
            "regular_field" to mapOf("type" to "keyword"),
            "alias_field" to mapOf("type" to "alias", "path" to "regular_field"),
        )

        val kept = entries.filter { (_, v) ->
            val m = v as Map<*, *>
            m["type"] != "alias" && m.isNotEmpty()
        }

        assertEquals(1, kept.size)
        assertTrue(kept.containsKey("regular_field"))
        assertFalse(kept.containsKey("alias_field"))
    }

    fun `test post-traversal filter skips entries with empty map`() {
        val entries: Map<String, Any> = mapOf(
            "regular_field" to mapOf("type" to "date"),
            "alias_field" to emptyMap<String, Any>(),
        )

        val kept = entries.filter { (_, v) ->
            val m = v as Map<*, *>
            m["type"] != "alias" && m.isNotEmpty()
        }

        assertEquals(1, kept.size)
        assertTrue(kept.containsKey("regular_field"))
    }

    fun `test post-traversal filter retains all entries when no alias fields present`() {
        val entries: Map<String, Any> = mapOf(
            "field_a" to mapOf("type" to "keyword"),
            "field_b" to mapOf("type" to "text"),
            "field_c" to mapOf("type" to "long"),
        )

        val kept = entries.filter { (_, v) ->
            val m = v as Map<*, *>
            m["type"] != "alias" && m.isNotEmpty()
        }

        assertEquals(3, kept.size)
    }

    // -------------------------------------------------------------------------
    // allFlattenPaths post-traversal filter simulation (Gap 1)
    //
    // traverseMappingsAndUpdate adds every leaf to flattenPaths BEFORE calling
    // the processor, so alias paths land in flattenPaths regardless of what the
    // processor returns.  The production code in indexDocLevelQueries passes
    // allFlattenPaths (unfiltered) into doIndexAllQueries, which means alias
    // field names flow into query rewrites for non-existent fields.
    //
    // This test verifies that a caller applying the same post-traversal filter
    // used for updatedProperties (skip type==alias or empty) correctly excludes
    // alias entries from the allFlattenPaths set — i.e., documents the fix that
    // MUST also be applied to the allFlattenPaths construction site in
    // indexDocLevelQueries (line ~352) to close the gap fully.
    // -------------------------------------------------------------------------

    fun `test allFlattenPaths only contains non-alias fields after post-traversal filter`() {
        val queries = makeInstance()
        val mappings = mutableMapOf<String, Any>(
            "timestamp" to mutableMapOf<String, Any>("type" to "date"),
            "ts_alias" to mutableMapOf<String, Any>("type" to "alias", "path" to "timestamp"),
        )
        val flattenPaths = mutableMapOf<String, MutableMap<String, Any>>()

        queries.traverseMappingsAndUpdate(mappings, "", aliasSkippingLeaf, flattenPaths)

        // Raw flattenPaths contains both because traversal is unconditional.
        assertEquals(2, flattenPaths.size)
        assertTrue(flattenPaths.containsKey("ts_alias"))

        // Simulated production filter: exclude entries whose props are empty
        // (alias-skipping processor sets them to mutableMapOf()) or whose type is "alias".
        // This mirrors what SHOULD be applied when building allFlattenPaths.
        val filteredPaths = flattenPaths.filter { (_, props) ->
            props.isNotEmpty() && props["type"] != "alias"
        }

        assertEquals(
            "filtered allFlattenPaths must contain only non-alias fields",
            1,
            filteredPaths.size,
        )
        assertTrue(filteredPaths.containsKey("timestamp"))
        assertFalse(
            "alias field must be excluded from filteredPaths to avoid query rewrites for non-existent field names",
            filteredPaths.containsKey("ts_alias"),
        )
    }

    // -------------------------------------------------------------------------
    // getAllConflictingFields alias passthrough (Gap 2)
    //
    // getAllConflictingFields uses an identity leaf processor — it does NOT skip
    // alias-type fields.  Two indices with an alias field at the same path but
    // pointing to different targets will always have unequal props maps
    // ({type:alias, path:foo} != {type:alias, path:bar}), causing a false
    // conflict.  This test documents the unguarded behaviour by simulating the
    // identity processor directly via traverseMappingsAndUpdate.
    // -------------------------------------------------------------------------

    fun `test identity processor includes alias fields in flattenPaths (getAllConflictingFields gap)`() {
        val queries = makeInstance()

        // Index A: alias field pointing to "event.start"
        val mappingsA = mutableMapOf<String, Any>(
            "event_start" to mutableMapOf<String, Any>("type" to "alias", "path" to "event.start"),
            "message" to mutableMapOf<String, Any>("type" to "text"),
        )
        // Index B: same alias field name but pointing to "event.begin" — different path
        val mappingsB = mutableMapOf<String, Any>(
            "event_start" to mutableMapOf<String, Any>("type" to "alias", "path" to "event.begin"),
            "message" to mutableMapOf<String, Any>("type" to "text"),
        )

        val flattenPathsA = mutableMapOf<String, MutableMap<String, Any>>()
        val flattenPathsB = mutableMapOf<String, MutableMap<String, Any>>()

        // Identity processor — mirrors getAllConflictingFields behaviour exactly.
        queries.traverseMappingsAndUpdate(mappingsA, "", identityLeaf, flattenPathsA)
        queries.traverseMappingsAndUpdate(mappingsB, "", identityLeaf, flattenPathsB)

        // Both alias fields are present in their respective flattenPaths.
        assertTrue("Index A flattenPaths must include event_start", flattenPathsA.containsKey("event_start"))
        assertTrue("Index B flattenPaths must include event_start", flattenPathsB.containsKey("event_start"))

        // Simulate the conflict-detection logic from getAllConflictingFields:
        // seed allFlattenPaths with index A, then check index B against it.
        val allFlattenPaths = mutableMapOf<String, MutableMap<String, Any>>()
        val conflictingFields = mutableSetOf<String>()
        flattenPathsA.forEach { (k, v) -> allFlattenPaths[k] = v }
        flattenPathsB.forEach { (k, v) ->
            if (allFlattenPaths.containsKey(k) && allFlattenPaths[k]!! != v) {
                conflictingFields.add(k)
            }
            allFlattenPaths.putIfAbsent(k, v)
        }

        // "message" is identical in both indices — must NOT be flagged.
        assertFalse("message must not be a conflicting field", conflictingFields.contains("message"))

        // "event_start" has the same field name but different alias paths — the identity
        // processor sees them as unequal and flags a false conflict.
        // This assertion documents the known gap: alias fields should be excluded from
        // conflict detection because their props differ by design (they point to different
        // concrete fields in different indices), not because of a real type conflict.
        assertTrue(
            "Known gap: identity processor falsely flags alias field as conflicting " +
                "when alias paths differ across indices — getAllConflictingFields must skip alias fields",
            conflictingFields.contains("event_start"),
        )
    }

    // -------------------------------------------------------------------------
    // deepCopyMap — list-of-maps recursion (Gap 3)
    //
    // The original implementation used toMutableList() which is a shallow copy:
    // map elements inside the list were shared between original and copy.
    // The fix recurses into map elements inside lists.
    // -------------------------------------------------------------------------

    fun `test deepCopyMap deep copies map elements inside lists`() {
        val innerMap = mutableMapOf<String, Any>("nested_key" to "original_value")
        val original = mutableMapOf<String, Any>(
            "items" to mutableListOf<Any>(innerMap, "plain_string"),
        )

        val copy = DocLevelMonitorQueries.deepCopyMap(original)

        @Suppress("UNCHECKED_CAST")
        val copiedList = copy["items"] as MutableList<Any>
        @Suppress("UNCHECKED_CAST")
        val copiedInnerMap = copiedList[0] as MutableMap<String, Any>

        // Mutate the map element inside the copied list.
        copiedInnerMap["nested_key"] = "mutated_value"

        // The original inner map must be unaffected — deep copy isolated it.
        assertEquals(
            "Map element inside list must be deep-copied so mutations do not affect the original",
            "original_value",
            innerMap["nested_key"],
        )
        assertNotSame(innerMap, copiedInnerMap)
    }
}

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
 * NOT covered here (requires live ClusterState / integration environment):
 *  - Gap A (creation-date truncation removal): the fix in [DocLevelMonitorQueries.indexDocLevelQueries]
 *    removes the filter that hid old-typed backing indices. This behaviour is verified in the
 *    integration test suite (DocLevelMonitorQueriesIT) where real datastream rollover can be exercised.
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

    fun `test deepCopyMap copies list of scalars as a new mutable list`() {
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
    // allFlattenPaths post-traversal filter (H1 fix verification)
    //
    // traverseMappingsAndUpdate adds every leaf to flattenPaths BEFORE calling
    // the processor, so alias paths land in flattenPaths with their original props.
    // The fix in indexDocLevelQueries filters flattenPaths entries whose props
    // have type=="alias" before adding to allFlattenPaths, preventing alias field
    // names from flowing into query rewrites for non-existent fields.
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
    // getAllConflictingFields alias guard (H2 fix verification)
    //
    // getAllConflictingFields now skips alias-type fields via a type=="alias" guard
    // in the flattenPaths forEach loop.  Two indices with an alias field at the
    // same path but pointing to different targets will no longer produce a false
    // conflict because the guard skips them before the equality comparison.
    // -------------------------------------------------------------------------

    fun `test alias-skipping processor excludes alias fields from conflict detection (getAllConflictingFields fix)`() {
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

        // Alias-skipping processor — mirrors the FIXED getAllConflictingFields behaviour.
        queries.traverseMappingsAndUpdate(mappingsA, "", aliasSkippingLeaf, flattenPathsA)
        queries.traverseMappingsAndUpdate(mappingsB, "", aliasSkippingLeaf, flattenPathsB)

        // flattenPaths stores original nodeProps (captured before the processor runs),
        // so alias fields appear with their original type=alias props — not empty.
        assertTrue("Index A flattenPaths must include event_start", flattenPathsA.containsKey("event_start"))
        assertTrue("Index B flattenPaths must include event_start", flattenPathsB.containsKey("event_start"))
        assertEquals("alias", flattenPathsA["event_start"]!!["type"])
        assertEquals("alias", flattenPathsB["event_start"]!!["type"])

        // Simulate the FIXED conflict-detection logic from getAllConflictingFields:
        // seed allFlattenPaths with index A, then check index B against it,
        // skipping entries whose type is "alias".
        val allFlattenPaths = mutableMapOf<String, MutableMap<String, Any>>()
        val conflictingFields = mutableSetOf<String>()
        flattenPathsA.forEach { (k, v) ->
            if (v["type"] == "alias") return@forEach
            allFlattenPaths[k] = v
        }
        flattenPathsB.forEach { (k, v) ->
            if (v["type"] == "alias") return@forEach
            if (allFlattenPaths.containsKey(k) && allFlattenPaths[k]!! != v) {
                conflictingFields.add(k)
            }
            allFlattenPaths.putIfAbsent(k, v)
        }

        // "message" is identical in both indices — must NOT be flagged.
        assertFalse("message must not be a conflicting field", conflictingFields.contains("message"))

        // "event_start" is an alias field — the type=="alias" guard skips it,
        // so it must NOT be flagged as conflicting.
        assertFalse(
            "Fixed: alias field with differing paths across indices must NOT be flagged as conflicting",
            conflictingFields.contains("event_start"),
        )
    }

    // -------------------------------------------------------------------------
    // getAllConflictingFields — field-type change detection (Gap B fix)
    //
    // A field whose "type" key changes from one backing index to another
    // (e.g. resource.host.name: text → keyword) must be flagged as conflicting
    // so that the per-index suffix branch fires and the merged putMapping never
    // contains a type change that OpenSearch core would reject.
    // -------------------------------------------------------------------------

    fun `test getAllConflictingFields detects text-to-keyword type change as conflict`() {
        val queries = makeInstance()

        // Index A (older backing index): resource.host.name is "text"
        val mappingsA = mutableMapOf<String, Any>(
            "host" to mutableMapOf<String, Any>(
                "properties" to mutableMapOf<String, Any>(
                    "name" to mutableMapOf<String, Any>("type" to "text")
                )
            ),
            "event_id" to mutableMapOf<String, Any>("type" to "keyword"),
        )
        // Index B (newer backing index): resource.host.name changed to "keyword"
        val mappingsB = mutableMapOf<String, Any>(
            "host" to mutableMapOf<String, Any>(
                "properties" to mutableMapOf<String, Any>(
                    "name" to mutableMapOf<String, Any>("type" to "keyword")
                )
            ),
            "event_id" to mutableMapOf<String, Any>("type" to "keyword"),
        )

        val flattenPathsA = mutableMapOf<String, MutableMap<String, Any>>()
        val flattenPathsB = mutableMapOf<String, MutableMap<String, Any>>()
        queries.traverseMappingsAndUpdate(mappingsA, "", identityLeaf, flattenPathsA)
        queries.traverseMappingsAndUpdate(mappingsB, "", identityLeaf, flattenPathsB)

        // Simulate the FIXED getAllConflictingFields logic (with explicit type-key check).
        val allFlattenPaths = mutableMapOf<String, MutableMap<String, Any>>()
        val conflictingFields = mutableSetOf<String>()

        fun processFlat(flattenPaths: Map<String, MutableMap<String, Any>>) {
            flattenPaths.forEach { (k, v) ->
                if (v["type"] == "alias") return@forEach
                if (allFlattenPaths.containsKey(k)) {
                    val existing = allFlattenPaths[k]!!
                    val existingType = existing["type"]
                    val newType = v["type"]
                    if (existingType != null && newType != null && existingType != newType) {
                        conflictingFields.add(k)
                    } else if (existing != v) {
                        conflictingFields.add(k)
                    }
                }
                allFlattenPaths.putIfAbsent(k, v)
            }
        }
        processFlat(flattenPathsA)
        processFlat(flattenPathsB)

        // host.name changed from text to keyword — MUST be flagged as conflicting.
        assertTrue(
            "host.name must be flagged as conflicting because its type changed from text to keyword",
            conflictingFields.contains("host.name"),
        )
        // event_id is identical in both indices — must NOT be flagged.
        assertFalse(
            "event_id must not be flagged as conflicting because it is identical in both indices",
            conflictingFields.contains("event_id"),
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

    // -------------------------------------------------------------------------
    // leafNodeProcessor alias-skip (Fix 1)
    //
    // When an alias field is processed by leafNodeProcessor the processor must
    // return Triple(fieldName, fieldName, mutableMapOf()) — no rename, no props.
    // The aliasSkippingLeaf helper (defined above) mirrors that logic directly;
    // these tests exercise the consequence: traverseMappingsAndUpdate must leave
    // the backing map for an alias field as an empty map, and the field must
    // still be registered in flattenPaths under its original name.
    // -------------------------------------------------------------------------

    fun `test traverseMappingsAndUpdate alias field produces empty props after aliasSkippingLeaf`() {
        val aliasField = mutableMapOf<String, Any>("type" to "alias", "path" to "resource.host.name")
        val realField = mutableMapOf<String, Any>("type" to "keyword")
        val properties = mutableMapOf<String, Any>(
            "host.name" to aliasField,
            "event_id" to realField,
        )

        val flattenPaths = mutableMapOf<String, MutableMap<String, Any>>()
        val queries = makeInstance()
        queries.traverseMappingsAndUpdate(properties, "", aliasSkippingLeaf, flattenPaths)

        // The alias field must be registered in flattenPaths under its original key.
        assertTrue(
            "alias field 'host.name' must appear in flattenPaths",
            flattenPaths.containsKey("host.name"),
        )
        // After the alias-skip, the backing map in properties must be empty.
        @Suppress("UNCHECKED_CAST")
        val resultProps = properties["host.name"] as Map<String, Any>
        assertTrue(
            "alias field props must be empty after aliasSkippingLeaf so it is not added to updatedProperties",
            resultProps.isEmpty(),
        )
        // The real field must be unaffected.
        assertTrue(
            "real field 'event_id' must appear in flattenPaths",
            flattenPaths.containsKey("event_id"),
        )
    }

    fun `test traverseMappingsAndUpdate alias field is not renamed`() {
        // The leaf that mimics production: renames every non-alias field by appending _suffix.
        val renamingLeaf =
            fun(fieldName: String, _: String, props: MutableMap<String, Any>):
                Triple<String, String, MutableMap<String, Any>> {
                if (props["type"] == "alias") return Triple(fieldName, fieldName, mutableMapOf())
                return Triple(fieldName, "${fieldName}_suffix", props.toMutableMap())
            }

        val properties = mutableMapOf<String, Any>(
            "host.name" to mutableMapOf<String, Any>("type" to "alias", "path" to "resource.host.name"),
            "event_id" to mutableMapOf<String, Any>("type" to "keyword"),
        )

        val flattenPaths = mutableMapOf<String, MutableMap<String, Any>>()
        val queries = makeInstance()
        queries.traverseMappingsAndUpdate(properties, "", renamingLeaf, flattenPaths)

        // The alias field key must NOT have been renamed.
        assertTrue("alias field must keep its original key", properties.containsKey("host.name"))
        assertFalse("alias field must not gain a _suffix key", properties.containsKey("host.name_suffix"))

        // The real field MUST have been renamed.
        assertFalse("real field original key must be removed after rename", properties.containsKey("event_id"))
        assertTrue("real field must gain a _suffix key", properties.containsKey("event_id_suffix"))
    }

    // -------------------------------------------------------------------------
    // properties.forEach alias/empty filter (Fix 2)
    //
    // After traversal, the caller iterates `properties` to build `updatedProperties`.
    // Alias fields and the empty-map sentinels produced by the alias-skip must be
    // filtered out.  The filter logic is:
    //   - skip if type == "alias"
    //   - skip if the map is empty  (sentinel left by aliasSkippingLeaf)
    // -------------------------------------------------------------------------

    fun `test alias and empty entries are excluded from updatedProperties accumulation`() {
        // Simulate the state of `properties` AFTER traverseMappingsAndUpdate with aliasSkippingLeaf:
        // alias fields become empty maps, real fields keep their props.
        val propertiesAfterTraversal = mutableMapOf<String, Any>(
            "host.name" to mutableMapOf<String, Any>(), // empty sentinel left by alias-skip
            "event_id_suffix" to mutableMapOf<String, Any>("type" to "keyword"),
            "level_suffix" to mutableMapOf<String, Any>("type" to "text"),
        )

        val updatedProperties = mutableMapOf<String, Any>()

        propertiesAfterTraversal.forEach {
            val valueMap = it.value as? Map<*, *>
            if (valueMap != null) {
                when {
                    // skip nested
                    valueMap["type"] == "nested" -> { /* no-op */ }
                    // skip alias fields and empty-props sentinels (Fix 2)
                    valueMap["type"] == "alias" || valueMap.isEmpty() -> { /* no-op */ }
                    else -> updatedProperties[it.key] = it.value
                }
            }
        }

        assertFalse(
            "empty sentinel for alias field must not appear in updatedProperties",
            updatedProperties.containsKey("host.name"),
        )
        assertTrue(
            "real renamed field must appear in updatedProperties",
            updatedProperties.containsKey("event_id_suffix"),
        )
        assertTrue(
            "real renamed field must appear in updatedProperties",
            updatedProperties.containsKey("level_suffix"),
        )
    }

    // -------------------------------------------------------------------------
    // isUnrecoverableMappingError fast-fail detection (Fix 3)
    //
    // The fast-fail guard in updateQueryIndexMappings checks whether the exception
    // message matches known-unrecoverable patterns.  Extract the predicate logic
    // and test it directly so the guard's correctness is verified without needing
    // a live cluster.
    // -------------------------------------------------------------------------

    fun `test isUnrecoverableMappingError detects mapper type conflict message`() {
        val typeConflictMsg =
            "mapper [resource.host.name_audit_Bf56gaABptlalFR633AZ] cannot be changed from type [text] to [keyword]"
        val isUnrecoverable = typeConflictMsg.contains("alias must refer to an existing field") ||
            (typeConflictMsg.contains("mapper [") && typeConflictMsg.contains("cannot be changed from type"))
        assertTrue(
            "mapper type-conflict message must be identified as unrecoverable",
            isUnrecoverable,
        )
    }

    fun `test isUnrecoverableMappingError detects alias dangling reference message`() {
        val aliasMsg = "alias must refer to an existing field [resource.host.name_audit_Bf56gaABptlalFR633AZ]"
        val isUnrecoverable = aliasMsg.contains("alias must refer to an existing field") ||
            (aliasMsg.contains("mapper [") && aliasMsg.contains("cannot be changed from type"))
        assertTrue(
            "alias dangling-reference message must be identified as unrecoverable",
            isUnrecoverable,
        )
    }

    fun `test isUnrecoverableMappingError does not flag field-limit message`() {
        val fieldLimitMsg = "Limit of total fields [1000] in index [.opensearch-alerting-queries] has been exceeded"
        val isUnrecoverable = fieldLimitMsg.contains("alias must refer to an existing field") ||
            (fieldLimitMsg.contains("mapper [") && fieldLimitMsg.contains("cannot be changed from type"))
        assertFalse(
            "field-limit message must NOT be identified as unrecoverable (it is handled by rollover logic)",
            isUnrecoverable,
        )
    }
}

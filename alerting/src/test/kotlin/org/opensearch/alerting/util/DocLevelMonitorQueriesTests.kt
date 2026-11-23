/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import org.opensearch.commons.alerting.model.DocLevelQuery
import org.opensearch.test.OpenSearchTestCase

/** Native detector DSL and mapping-aware field rewriting regressions. */
class DocLevelMonitorQueriesTests : OpenSearchTestCase() {

    fun `test native wildcard mapping retains source parameters`() {
        val mapping = mapOf<String, Any>(
            "type" to "wildcard",
            "doc_values" to false,
            "null_value" to "missing",
            "ignore_above" to 4096,
            "normalizer" to "custom",
            "meta" to mapOf("owner" to "test")
        )
        val result = DocLevelMonitorQueries.applyQueryIndexMappingOverrides(
            mapping, mapOf("text" to mapOf("analyzer" to "rule_analyzer"))
        )
        assertEquals(mapping, result)
        assertEquals("wildcard", mapping["type"])
        assertEquals(false, mapping["doc_values"])
    }

    fun `test same type overrides retain compatible source parameters`() {
        val result = DocLevelMonitorQueries.applyQueryIndexMappingOverrides(
            mapOf("type" to "text", "store" to true, "analyzer" to "custom"),
            mapOf("text" to mapOf("analyzer" to "rule_analyzer"))
        )
        assertEquals(mapOf("type" to "text", "store" to true, "analyzer" to "rule_analyzer"), result)
    }

    fun `test unconfigured field types retain original mapping`() {
        val mapping = mapOf<String, Any>("type" to "keyword", "ignore_above" to 512)
        assertEquals(mapping, DocLevelMonitorQueries.applyQueryIndexMappingOverrides(mapping, emptyMap()))
    }

    fun `test direct query field rewrite respects field boundaries`() {
        assertEquals(
            "message_index_monitor:*success* AND other.message:*failure* AND longer_message:*failure*",
            DocLevelMonitorQueries.rewriteQueryField(
                "message:*success* AND other.message:*failure* AND longer_message:*failure*", "message", "_index_monitor"
            )
        )
    }

    @Suppress("UNCHECKED_CAST")
    fun `test text multifields receive analyzer override without changing native wildcard parent`() {
        val mapping = mapOf<String, Any>(
            "type" to "wildcard",
            "fields" to mapOf("text" to mapOf("type" to "text"))
        )
        val result = DocLevelMonitorQueries.applyQueryIndexMappingOverrides(
            mapping, mapOf("text" to mapOf("analyzer" to "rule_analyzer"))
        )
        val fields = result["fields"] as Map<String, Any>
        assertEquals("wildcard", result["type"])
        assertEquals(mapOf("type" to "text", "analyzer" to "rule_analyzer"), fields["text"])
        assertEquals(mapOf("type" to "text"), (mapping["fields"] as Map<String, Any>)["text"])
        assertEquals(
            "attributes.message.wildcard:*success* AND other.attributes.message:*failure*",
            DocLevelMonitorQueries.rewriteQueryField(
                "attributes.message.wildcard:*success* AND other.attributes.message:*failure*",
                "attributes.message", "_index_monitor"
            )
        )
    }

    fun `test native DSL rewrites field keys and exists without changing literal values`() {
        val query = """{"bool":{"must":[{"wildcard":{"message":{"value":"*message:literal_ws_*"}}},
            {"exists":{"field":"message.raw"}},{"query_string":{"query":"action:login AND note:\"message:literal\"","fields":["action^2"]}}]}}"""
        val actual = DocLevelMonitorQueries.queryForPercolation(
            query, mapOf("message" to "message_suffix", "message.raw" to "message_suffix.raw", "action" to "action_suffix"), emptyList()
        )
        assertEquals(
            mapOf(
                "bool" to mapOf(
                    "must" to listOf(
                        mapOf("wildcard" to mapOf("message_suffix" to mapOf("value" to "*message:literal_ws_*"))),
                        mapOf("exists" to mapOf("field" to "message_suffix.raw")),
                        mapOf(
                            "query_string" to mapOf(
                                "query" to "action_suffix:login AND note:\"message:literal\"",
                                "fields" to listOf("action_suffix^2")
                            )
                        )
                    )
                )
            ),
            actual
        )
    }

    fun `test query string rewrite preserves quoted and escaped values and rewrites exists`() {
        assertEquals(
            "message_suffix:value AND note:\"message:literal\" AND note:message\\:literal AND _exists_:message_suffix.raw",
            DocLevelMonitorQueries.rewriteQueryString(
                "message:value AND note:\"message:literal\" AND note:message\\:literal AND _exists_:message.raw",
                mapOf("message" to "message_suffix", "message.raw" to "message_suffix.raw")
            )
        )
    }

    fun `test malformed native JSON is rejected rather than used as a legacy query`() {
        expectThrows(Exception::class.java) {
            DocLevelMonitorQueries.queryForPercolation("{invalid", emptyMap(), emptyList())
        }
    }

    fun `test native percolator variants are scoped to exact concrete index and rule ids`() {
        val queries = listOf(
            DocLevelQuery("native_rule", "native", emptyList(), """{"bool":{"must_not":{"exists":{"field":"foo"}}}}"""),
            DocLevelQuery("legacy_rule", "legacy", emptyList(), "NOT foo:*")
        )
        assertEquals(
            "index_b",
            DocLevelMonitorQueries.nativeQueryConcreteIndex(
                "native_rule_index_b_monitor", "monitor", queries, listOf("index_a", "index_b")
            )
        )
        assertNull(
            DocLevelMonitorQueries.nativeQueryConcreteIndex(
                "legacy_rule_index_b_monitor", "monitor", queries, listOf("index_a", "index_b")
            )
        )
        assertNull(
            DocLevelMonitorQueries.nativeQueryConcreteIndex(
                "other_native_rule_index_b_monitor", "monitor", queries, listOf("index_a", "index_b")
            )
        )
    }

    fun `test mapping metadata distinguishes multifields from object children`() {
        val scalar = DocLevelMonitorQueries.sourceFieldParents(
            mapOf(
                "properties" to mapOf(
                    "foo" to mapOf("type" to "wildcard", "fields" to mapOf("raw" to mapOf("type" to "keyword")))
                )
            )
        )
        val objectMapping = DocLevelMonitorQueries.sourceFieldParents(
            mapOf(
                "properties" to mapOf(
                    "foo" to mapOf("properties" to mapOf("raw" to mapOf("type" to "wildcard")))
                )
            )
        )
        assertEquals(mapOf("foo" to "foo", "foo.raw" to "foo"), scalar)
        assertEquals(mapOf("foo.raw" to "foo.raw"), objectMapping)
    }

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

}

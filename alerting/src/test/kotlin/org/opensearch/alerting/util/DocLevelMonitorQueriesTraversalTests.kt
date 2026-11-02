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
 * Unit tests for [DocLevelMonitorQueries.traverseMappingsAndUpdate] focused on the
 * ClassCastException guard: values returned by OpenSearch may be unmodifiable or empty maps,
 * and the fix uses `(it.value as Map<String, Any>).toMutableMap()` instead of a direct
 * `as MutableMap` cast.
 */
class DocLevelMonitorQueriesTraversalTests : OpenSearchTestCase() {

    private val client: Client = mock(Client::class.java)
    private val clusterService: ClusterService = mock(ClusterService::class.java)
    private val queries = DocLevelMonitorQueries(client, clusterService)

    private val identityLeafFn: (String, String, MutableMap<String, Any>) -> Triple<String, String, MutableMap<String, Any>> =
        { key, _, props -> Triple(key, key, props) }

    fun `test traverseMappingsAndUpdate with unmodifiable map values does not throw`() {
        // Simulates what OpenSearch cluster state may return: values wrapped in Collections.unmodifiableMap
        val unmodifiableProps: Map<String, Any> = java.util.Collections.unmodifiableMap(
            mapOf("type" to "keyword")
        )
        val node = mutableMapOf<String, Any>("field1" to unmodifiableProps)
        val flattenPaths = mutableMapOf<String, MutableMap<String, Any>>()

        // Must not throw ClassCastException
        queries.traverseMappingsAndUpdate(node, "", identityLeafFn, flattenPaths)

        assertTrue("flattenPaths must contain leaf 'field1'", flattenPaths.containsKey("field1"))
    }

    fun `test traverseMappingsAndUpdate with empty map value does not throw`() {
        // An entry whose value is Collections.emptyMap() has no TYPE key and no PROPERTIES key,
        // so it is silently skipped — no leaf added, no recursion.
        val node = mutableMapOf<String, Any>(
            "empty_field" to java.util.Collections.emptyMap<String, Any>()
        )
        val flattenPaths = mutableMapOf<String, MutableMap<String, Any>>()

        // Must not throw ClassCastException or UnsupportedOperationException
        queries.traverseMappingsAndUpdate(node, "", identityLeafFn, flattenPaths)

        assertTrue("No leaves should be registered for an empty-map entry", flattenPaths.isEmpty())
    }

    fun `test traverseMappingsAndUpdate renames leaf node`() {
        val node = mutableMapOf<String, Any>(
            "field1" to mutableMapOf<String, Any>("type" to "keyword")
        )
        val flattenPaths = mutableMapOf<String, MutableMap<String, Any>>()
        val renamingLeafFn: (String, String, MutableMap<String, Any>) -> Triple<String, String, MutableMap<String, Any>> =
            { key, _, props ->
                val newKey = "${key}_renamed"
                Triple(key, newKey, props)
            }

        queries.traverseMappingsAndUpdate(node, "", renamingLeafFn, flattenPaths)

        assertTrue("node must contain the renamed key", node.containsKey("field1_renamed"))
        assertFalse("node must not retain the old key after rename", node.containsKey("field1"))
    }

    fun `test traverseMappingsAndUpdate recurses into nested properties`() {
        // Two-level map: outer -> { properties -> { inner -> { type -> text } } }
        val innerField = mutableMapOf<String, Any>("type" to "text")
        val innerProperties = mutableMapOf<String, Any>("inner" to innerField)
        val outerField = mutableMapOf<String, Any>(DocLevelMonitorQueries.PROPERTIES to innerProperties)
        val node = mutableMapOf<String, Any>("outer" to outerField)
        val flattenPaths = mutableMapOf<String, MutableMap<String, Any>>()

        queries.traverseMappingsAndUpdate(node, "", identityLeafFn, flattenPaths)

        assertTrue(
            "flattenPaths must contain fully-qualified path 'outer.inner'",
            flattenPaths.containsKey("outer.inner")
        )
        assertFalse(
            "outer itself is not a leaf and must not appear in flattenPaths",
            flattenPaths.containsKey("outer")
        )
    }

    fun `test traverseMappingsAndUpdate with unmodifiable nested properties does not throw`() {
        // Simulates nested properties returned as an unmodifiable map from OpenSearch
        val innerField: Map<String, Any> = java.util.Collections.unmodifiableMap(
            mapOf("type" to "text")
        )
        val innerProperties: Map<String, Any> = java.util.Collections.unmodifiableMap(
            mapOf("inner" to innerField)
        )
        val outerField: Map<String, Any> = java.util.Collections.unmodifiableMap(
            mapOf(DocLevelMonitorQueries.PROPERTIES to innerProperties)
        )
        val node = mutableMapOf<String, Any>("outer" to outerField)
        val flattenPaths = mutableMapOf<String, MutableMap<String, Any>>()

        // Must not throw ClassCastException when recursing into unmodifiable properties map
        queries.traverseMappingsAndUpdate(node, "", identityLeafFn, flattenPaths)

        assertTrue(
            "flattenPaths must contain 'outer.inner' even when all values are unmodifiable",
            flattenPaths.containsKey("outer.inner")
        )
    }
}

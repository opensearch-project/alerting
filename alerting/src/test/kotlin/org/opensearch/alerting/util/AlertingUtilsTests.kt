/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import org.mockito.Mockito.mock
import org.opensearch.alerting.AlertService
import org.opensearch.alerting.MonitorRunnerService
import org.opensearch.alerting.model.AlertContext
import org.opensearch.alerting.randomAction
import org.opensearch.alerting.randomBucketLevelTrigger
import org.opensearch.alerting.randomChainedAlertTrigger
import org.opensearch.alerting.randomDocumentLevelTrigger
import org.opensearch.alerting.randomQueryLevelTrigger
import org.opensearch.alerting.randomTemplateScript
import org.opensearch.alerting.script.BucketLevelTriggerExecutionContext
import org.opensearch.alerting.script.DocumentLevelTriggerExecutionContext
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.unit.TimeValue
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.transport.client.Client
class AlertingUtilsTests : OpenSearchTestCase() {
    fun `test parseSampleDocTags only returns expected tags`() {
        val expectedDocSourceTags = (0..3).map { "field$it" }
        val unexpectedDocSourceTags = ((expectedDocSourceTags.size + 1)..(expectedDocSourceTags.size + 5))
            .map { "field$it" }

        val unexpectedTagsScriptSource = unexpectedDocSourceTags.joinToString { field -> "$field = {{$field}}" }
        val expectedTagsScriptSource = unexpectedTagsScriptSource + """
                ${unexpectedDocSourceTags.joinToString("\n") { field -> "$field = {{$field}}" }}
                {{#alerts}}
                {{#${AlertContext.SAMPLE_DOCS_FIELD}}}
                    ${expectedDocSourceTags.joinToString("\n") { field -> "$field = {{_source.$field}}" }}
                {{/${AlertContext.SAMPLE_DOCS_FIELD}}}
                {{/alerts}}
        """.trimIndent()

        // Action that prints doc source data
        val trigger1 = randomDocumentLevelTrigger(
            actions = listOf(randomAction(template = randomTemplateScript(source = expectedTagsScriptSource)))
        )

        // Action that does not print doc source data
        val trigger2 = randomDocumentLevelTrigger(
            actions = listOf(randomAction(template = randomTemplateScript(source = unexpectedTagsScriptSource)))
        )

        // No actions
        val trigger3 = randomDocumentLevelTrigger(actions = listOf())

        val tags = parseSampleDocTags(listOf(trigger1, trigger2, trigger3))

        assertEquals(expectedDocSourceTags.size, tags.size)
        expectedDocSourceTags.forEach { tag -> assertTrue(tags.contains(tag)) }
        unexpectedDocSourceTags.forEach { tag -> assertFalse(tags.contains(tag)) }
    }

    fun `test printsSampleDocData entire ctx tag returns TRUE`() {
        val tag = "{{ctx}}"
        val triggers = listOf(
            randomBucketLevelTrigger(actions = listOf(randomAction(template = randomTemplateScript(source = tag)))),
            randomDocumentLevelTrigger(actions = listOf(randomAction(template = randomTemplateScript(source = tag))))
        )

        triggers.forEach { trigger -> assertTrue(printsSampleDocData(trigger)) }
    }

    fun `test printsSampleDocData entire alerts tag returns TRUE`() {
        val triggers = listOf(
            randomBucketLevelTrigger(
                actions = listOf(
                    randomAction(
                        template = randomTemplateScript(
                            source = "{{ctx.${BucketLevelTriggerExecutionContext.NEW_ALERTS_FIELD}}}"
                        )
                    )
                )
            ),
            randomDocumentLevelTrigger(
                actions = listOf(
                    randomAction(
                        template = randomTemplateScript(
                            source = "{{ctx.${DocumentLevelTriggerExecutionContext.ALERTS_FIELD}}}"
                        )
                    )
                )
            )
        )

        triggers.forEach { trigger -> assertTrue(printsSampleDocData(trigger)) }
    }

    fun `test printsSampleDocData entire sample_docs tag returns TRUE`() {
        val triggers = listOf(
            randomBucketLevelTrigger(
                actions = listOf(
                    randomAction(
                        template = randomTemplateScript(
                            source = """
                                {{#ctx.${BucketLevelTriggerExecutionContext.NEW_ALERTS_FIELD}}}
                                    {{${AlertContext.SAMPLE_DOCS_FIELD}}}
                                {{/ctx.${BucketLevelTriggerExecutionContext.NEW_ALERTS_FIELD}}}
                            """.trimIndent()
                        )
                    )
                )
            ),
            randomDocumentLevelTrigger(
                actions = listOf(
                    randomAction(
                        template = randomTemplateScript(
                            source = """
                                {{#ctx.${DocumentLevelTriggerExecutionContext.ALERTS_FIELD}}}
                                    {{${AlertContext.SAMPLE_DOCS_FIELD}}}
                                {{/ctx.${DocumentLevelTriggerExecutionContext.ALERTS_FIELD}}}
                            """.trimIndent()
                        )
                    )
                )
            )
        )

        triggers.forEach { trigger -> assertTrue(printsSampleDocData(trigger)) }
    }

    fun `test printsSampleDocData sample_docs iteration block returns TRUE`() {
        val triggers = listOf(
            randomBucketLevelTrigger(
                actions = listOf(
                    randomAction(
                        template = randomTemplateScript(
                            source = """
                                {{#ctx.${BucketLevelTriggerExecutionContext.NEW_ALERTS_FIELD}}}
                                    "{{#${AlertContext.SAMPLE_DOCS_FIELD}}}"
                                        {{_source.field}}
                                    "{{/${AlertContext.SAMPLE_DOCS_FIELD}}}"
                                {{/ctx.${BucketLevelTriggerExecutionContext.NEW_ALERTS_FIELD}}}
                            """.trimIndent()
                        )
                    )
                )
            ),
            randomDocumentLevelTrigger(
                actions = listOf(
                    randomAction(
                        template = randomTemplateScript(
                            source = """
                                {{#ctx.${DocumentLevelTriggerExecutionContext.ALERTS_FIELD}}}
                                    {{#${AlertContext.SAMPLE_DOCS_FIELD}}}
                                        {{_source.field}}
                                    {{/${AlertContext.SAMPLE_DOCS_FIELD}}}
                                {{/ctx.${DocumentLevelTriggerExecutionContext.ALERTS_FIELD}}}
                            """.trimIndent()
                        )
                    )
                )
            )
        )

        triggers.forEach { trigger -> assertTrue(printsSampleDocData(trigger)) }
    }

    fun `test printsSampleDocData unrelated tag returns FALSE`() {
        val tag = "{{ctx.monitor.name}}"
        val triggers = listOf(
            randomBucketLevelTrigger(actions = listOf(randomAction(template = randomTemplateScript(source = tag)))),
            randomDocumentLevelTrigger(actions = listOf(randomAction(template = randomTemplateScript(source = tag))))
        )

        triggers.forEach { trigger -> assertFalse(printsSampleDocData(trigger)) }
    }

    fun `test printsSampleDocData unsupported trigger types return FALSE`() {
        val tag = "{{ctx}}"
        val triggers = listOf(
            randomQueryLevelTrigger(actions = listOf(randomAction(template = randomTemplateScript(source = tag)))),
            randomChainedAlertTrigger(actions = listOf(randomAction(template = randomTemplateScript(source = tag))))
        )

        triggers.forEach { trigger -> assertFalse(printsSampleDocData(trigger)) }
    }

    fun `test getCancelAfterTimeInterval returns -1 when setting is default`() {
        val original = MonitorRunnerService.monitorCtx.cancelAfterTimeInterval
        try {
            MonitorRunnerService.monitorCtx.cancelAfterTimeInterval = TimeValue.timeValueMinutes(-1)
            assertEquals(-1L, getCancelAfterTimeInterval())
        } finally {
            MonitorRunnerService.monitorCtx.cancelAfterTimeInterval = original
        }
    }

    fun `test getCancelAfterTimeInterval returns at least ALERTS_SEARCH_TIMEOUT`() {
        val original = MonitorRunnerService.monitorCtx.cancelAfterTimeInterval
        try {
            // Setting lower than ALERTS_SEARCH_TIMEOUT (5 min) should return 5 min
            MonitorRunnerService.monitorCtx.cancelAfterTimeInterval = TimeValue.timeValueMinutes(1)
            assertEquals(AlertService.ALERTS_SEARCH_TIMEOUT.minutes, getCancelAfterTimeInterval())
        } finally {
            MonitorRunnerService.monitorCtx.cancelAfterTimeInterval = original
        }
    }

    fun `test getCancelAfterTimeInterval returns setting when higher than ALERTS_SEARCH_TIMEOUT`() {
        val original = MonitorRunnerService.monitorCtx.cancelAfterTimeInterval
        try {
            MonitorRunnerService.monitorCtx.cancelAfterTimeInterval = TimeValue.timeValueMinutes(10)
            assertEquals(10L, getCancelAfterTimeInterval())
        } finally {
            MonitorRunnerService.monitorCtx.cancelAfterTimeInterval = original
        }
    }

    // ----- sanitizeFieldMappingAttributes -----

    fun `test sanitizeFieldMappingAttributes strips analyzer from text field`() {
        val mapping = mutableMapOf<String, Any>(
            "type" to "text",
            "analyzer" to "my_custom_analyzer",
            "search_analyzer" to "my_custom_analyzer"
        )
        DocLevelMonitorQueries.sanitizeFieldMappingAttributes("text", mapping)
        assertFalse("analyzer must be removed", mapping.containsKey("analyzer"))
        assertFalse("search_analyzer must be removed", mapping.containsKey("search_analyzer"))
        assertEquals("type must be preserved", "text", mapping["type"])
    }

    fun `test sanitizeFieldMappingAttributes strips all analysis attributes`() {
        val mapping = mutableMapOf<String, Any>(
            "type" to "keyword",
            "normalizer" to "my_normalizer",
            "similarity" to "my_similarity",
            "search_quote_analyzer" to "my_analyzer",
            "doc_values" to true
        )
        DocLevelMonitorQueries.sanitizeFieldMappingAttributes("keyword", mapping)
        assertFalse("normalizer must be removed", mapping.containsKey("normalizer"))
        assertFalse("similarity must be removed", mapping.containsKey("similarity"))
        assertFalse("search_quote_analyzer must be removed", mapping.containsKey("search_quote_analyzer"))
        // Non-analysis attributes must not be affected
        assertTrue("doc_values must be preserved", mapping.containsKey("doc_values"))
        assertEquals("type must be preserved", "keyword", mapping["type"])
    }

    fun `test sanitizeFieldMappingAttributes strips properties from scalar field`() {
        // Reproduces the MapperParsingException[unknown parameter [properties] on mapper of type [text]]
        // failure caused by dynamic mapping collisions on the source index.
        val mapping = mutableMapOf<String, Any>(
            "type" to "text",
            "properties" to mutableMapOf<String, Any>(
                "subfield" to mutableMapOf<String, Any>("type" to "keyword")
            )
        )
        DocLevelMonitorQueries.sanitizeFieldMappingAttributes("text", mapping)
        assertFalse("properties must be removed from scalar-typed field", mapping.containsKey("properties"))
        assertEquals("type must be preserved", "text", mapping["type"])
    }

    fun `test sanitizeFieldMappingAttributes preserves properties on object field`() {
        val subProperties = mutableMapOf<String, Any>("sub" to mutableMapOf<String, Any>("type" to "keyword"))
        val mapping = mutableMapOf<String, Any>(
            "type" to "object",
            "properties" to subProperties
        )
        DocLevelMonitorQueries.sanitizeFieldMappingAttributes("object", mapping)
        assertTrue("properties must be kept on object field", mapping.containsKey("properties"))
    }

    fun `test sanitizeFieldMappingAttributes preserves properties on nested field`() {
        val subProperties = mutableMapOf<String, Any>("sub" to mutableMapOf<String, Any>("type" to "keyword"))
        val mapping = mutableMapOf<String, Any>(
            "type" to "nested",
            "properties" to subProperties
        )
        DocLevelMonitorQueries.sanitizeFieldMappingAttributes("nested", mapping)
        assertTrue("properties must be kept on nested field", mapping.containsKey("properties"))
    }

    fun `test sanitizeFieldMappingAttributes preserves properties when type is absent`() {
        // Absent type defaults to object in OpenSearch — must not strip properties
        val subProperties = mutableMapOf<String, Any>("sub" to mutableMapOf<String, Any>("type" to "keyword"))
        val mapping = mutableMapOf<String, Any>(
            "properties" to subProperties
        )
        DocLevelMonitorQueries.sanitizeFieldMappingAttributes(null, mapping)
        assertTrue("properties must be kept when type is absent", mapping.containsKey("properties"))
    }

    fun `test sanitizeFieldMappingAttributes recurses into multi-fields`() {
        val subField = mutableMapOf<String, Any>(
            "type" to "keyword",
            "normalizer" to "my_normalizer"
        )
        val mapping = mutableMapOf<String, Any>(
            "type" to "text",
            "analyzer" to "my_analyzer",
            "fields" to mutableMapOf<String, Any>("raw" to subField)
        )
        DocLevelMonitorQueries.sanitizeFieldMappingAttributes("text", mapping)
        assertFalse("analyzer must be removed from top-level field", mapping.containsKey("analyzer"))
        @Suppress("UNCHECKED_CAST")
        val rawField = (mapping["fields"] as Map<*, *>)["raw"] as Map<*, *>
        assertFalse("normalizer must be removed from multi-field", rawField.containsKey("normalizer"))
        assertEquals("type must be preserved in multi-field", "keyword", rawField["type"])
    }

    fun `test sanitizeFieldMappingAttributes does not modify clean field`() {
        val mapping = mutableMapOf<String, Any>(
            "type" to "keyword",
            "doc_values" to true,
            "index" to false
        )
        val original = mapping.toMap()
        DocLevelMonitorQueries.sanitizeFieldMappingAttributes("keyword", mapping)
        assertEquals("clean mapping must be unchanged", original, mapping)
    }

    fun `test traverseMappingsAndUpdate with nested field type without properties succeeds`() {
        // Verifies fix for https://github.com/opensearch-project/security-analytics/issues/1472
        val docLevelMonitorQueries = DocLevelMonitorQueries(mock(Client::class.java), mock(ClusterService::class.java))
        val mappings = mutableMapOf<String, Any>(
            "message" to mutableMapOf<String, Any>("type" to "text"),
            "http_request_headers" to mutableMapOf<String, Any>("type" to "nested")
        )
        val flattenPaths = mutableMapOf<String, MutableMap<String, Any>>()
        val leafProcessor =
            fun(fieldName: String, _: String, props: MutableMap<String, Any>):
                Triple<String, String, MutableMap<String, Any>> {
                return Triple(fieldName, fieldName, props)
            }

        docLevelMonitorQueries.traverseMappingsAndUpdate(mappings, "", leafProcessor, flattenPaths)

        assertTrue("Expected 'message' in flatten paths", flattenPaths.containsKey("message"))
        assertFalse("Expected nested field to be skipped", flattenPaths.containsKey("http_request_headers"))
    }

    fun `test traverseMappingsAndUpdate with nested field type with properties works`() {
        val docLevelMonitorQueries = DocLevelMonitorQueries(mock(Client::class.java), mock(ClusterService::class.java))
        val mappings = mutableMapOf<String, Any>(
            "message" to mutableMapOf<String, Any>("type" to "text"),
            "dll" to mutableMapOf<String, Any>(
                "type" to "nested",
                "properties" to mutableMapOf<String, Any>(
                    "name" to mutableMapOf<String, Any>("type" to "keyword")
                )
            )
        )
        val flattenPaths = mutableMapOf<String, MutableMap<String, Any>>()
        val leafProcessor =
            fun(fieldName: String, _: String, props: MutableMap<String, Any>):
                Triple<String, String, MutableMap<String, Any>> {
                return Triple(fieldName, fieldName, props)
            }

        docLevelMonitorQueries.traverseMappingsAndUpdate(mappings, "", leafProcessor, flattenPaths)

        assertTrue("Expected 'message' in flatten paths", flattenPaths.containsKey("message"))
        assertTrue("Expected 'dll.name' in flatten paths", flattenPaths.containsKey("dll.name"))
    }

    // sanitizeFieldMappingAttributes — sub-property recursion (Gap 1 regression tests)

    fun `test sanitizeFieldMappingAttributes strips analyzer from sub-field inside object field`() {
        // Regression: sanitizer must recurse into object sub-properties, not just multi-fields.
        val urlField = mutableMapOf<String, Any>("type" to "text", "analyzer" to "custom_analyzer")
        val mapping = mutableMapOf<String, Any>(
            "type" to "object",
            "properties" to mutableMapOf<String, Any>("url" to urlField)
        )
        DocLevelMonitorQueries.sanitizeFieldMappingAttributes("object", mapping)
        @Suppress("UNCHECKED_CAST")
        val urlProps = ((mapping["properties"] as Map<*, *>)["url"] as Map<*, *>)
        assertFalse("analyzer must be stripped from sub-field inside object", urlProps.containsKey("analyzer"))
        assertEquals("type of sub-field must be preserved", "text", urlProps["type"])
        assertTrue("properties must be kept on object field", mapping.containsKey("properties"))
    }

    fun `test sanitizeFieldMappingAttributes strips analyzer from sub-field inside implicit-object field`() {
        // Implicit object (no "type" key) must also have its sub-properties sanitized.
        val titleField = mutableMapOf<String, Any>(
            "type" to "text", "analyzer" to "custom_analyzer", "search_analyzer" to "standard"
        )
        val mapping = mutableMapOf<String, Any>("properties" to mutableMapOf<String, Any>("title" to titleField))
        DocLevelMonitorQueries.sanitizeFieldMappingAttributes(null, mapping)
        @Suppress("UNCHECKED_CAST")
        val titleProps = ((mapping["properties"] as Map<*, *>)["title"] as Map<*, *>)
        assertFalse("analyzer must be stripped", titleProps.containsKey("analyzer"))
        assertFalse("search_analyzer must be stripped", titleProps.containsKey("search_analyzer"))
        assertTrue("properties must be kept", mapping.containsKey("properties"))
    }

    fun `test sanitizeFieldMappingAttributes recurses into deeply nested object properties`() {
        // Three levels: outer object -> inner object -> leaf with normalizer.
        val leafField = mutableMapOf<String, Any>("type" to "keyword", "normalizer" to "my_normalizer")
        val innerObject = mutableMapOf<String, Any>(
            "type" to "object",
            "properties" to mutableMapOf<String, Any>("code" to leafField)
        )
        val mapping = mutableMapOf<String, Any>(
            "type" to "object",
            "properties" to mutableMapOf<String, Any>("status" to innerObject)
        )
        DocLevelMonitorQueries.sanitizeFieldMappingAttributes("object", mapping)
        @Suppress("UNCHECKED_CAST")
        val statusProps = ((mapping["properties"] as Map<*, *>)["status"] as Map<*, *>)
        @Suppress("UNCHECKED_CAST")
        val codeProps = ((statusProps["properties"] as Map<*, *>)["code"] as Map<*, *>)
        assertFalse("normalizer must be stripped at depth-3 leaf", codeProps.containsKey("normalizer"))
        assertEquals("type at depth-3 leaf must be preserved", "keyword", codeProps["type"])
    }

    // traverseMappingsAndUpdate — implicit-object writeback (Gap 2 regression tests)

    fun `test traverseMappingsAndUpdate propagates sanitization through implicit-object field`() {
        // Regression: mutations inside the recursive call must write back to the original tree.
        val docLevelMonitorQueries = DocLevelMonitorQueries(mock(Client::class.java), mock(ClusterService::class.java))
        val subField = mutableMapOf<String, Any>("type" to "text", "analyzer" to "custom_analyzer")
        val mappings = mutableMapOf<String, Any>(
            "request" to mutableMapOf<String, Any>("properties" to mutableMapOf<String, Any>("url" to subField))
        )
        val sanitizingLeaf =
            fun(fieldName: String, _: String, props: MutableMap<String, Any>):
                Triple<String, String, MutableMap<String, Any>> {
                DocLevelMonitorQueries.sanitizeFieldMappingAttributes(props["type"] as? String, props)
                return Triple(fieldName, fieldName, props)
            }
        val flattenPaths = mutableMapOf<String, MutableMap<String, Any>>()
        docLevelMonitorQueries.traverseMappingsAndUpdate(mappings, "", sanitizingLeaf, flattenPaths)
        @Suppress("UNCHECKED_CAST")
        val urlProps = ((mappings["request"] as Map<*, *>)["properties"] as Map<*, *>)["url"] as Map<*, *>
        assertFalse("analyzer must be absent after traversal", urlProps.containsKey("analyzer"))
        assertEquals("type must be preserved", "text", urlProps["type"])
    }

    fun `test traverseMappingsAndUpdate sanitizes sub-field inside explicit object-typed field`() {
        // End-to-end Gap 1: traversal classifies type="object" as a leaf; sanitizer must recurse into its properties.
        val docLevelMonitorQueries = DocLevelMonitorQueries(mock(Client::class.java), mock(ClusterService::class.java))
        val urlField = mutableMapOf<String, Any>("type" to "text", "analyzer" to "custom_analyzer")
        val mappings = mutableMapOf<String, Any>(
            "request" to mutableMapOf<String, Any>(
                "type" to "object",
                "properties" to mutableMapOf<String, Any>("url" to urlField)
            )
        )
        val sanitizingLeaf =
            fun(fieldName: String, _: String, props: MutableMap<String, Any>):
                Triple<String, String, MutableMap<String, Any>> {
                DocLevelMonitorQueries.sanitizeFieldMappingAttributes(props["type"] as? String, props)
                return Triple(fieldName, fieldName, props)
            }
        val flattenPaths = mutableMapOf<String, MutableMap<String, Any>>()
        docLevelMonitorQueries.traverseMappingsAndUpdate(mappings, "", sanitizingLeaf, flattenPaths)
        assertTrue("request must be in flattenPaths as a leaf", flattenPaths.containsKey("request"))
        @Suppress("UNCHECKED_CAST")
        val urlProps = ((mappings["request"] as Map<*, *>)["properties"] as Map<*, *>)["url"] as Map<*, *>
        assertFalse("analyzer must be stripped from sub-field of explicit object field", urlProps.containsKey("analyzer"))
        assertEquals("type must be preserved", "text", urlProps["type"])
    }

    fun `test sanitizeFieldMappingAttributes sanitizes field with both fields and properties`() {
        // A field with both multi-fields and sub-properties: both must be sanitized.
        val multiField = mutableMapOf<String, Any>("type" to "keyword", "normalizer" to "my_normalizer")
        val subPropField = mutableMapOf<String, Any>("type" to "text", "analyzer" to "custom_analyzer")
        val mapping = mutableMapOf<String, Any>(
            "type" to "object",
            "fields" to mutableMapOf<String, Any>("raw" to multiField),
            "properties" to mutableMapOf<String, Any>("desc" to subPropField)
        )
        DocLevelMonitorQueries.sanitizeFieldMappingAttributes("object", mapping)
        @Suppress("UNCHECKED_CAST")
        val rawField = (mapping["fields"] as Map<*, *>)["raw"] as Map<*, *>
        assertFalse("normalizer must be stripped from multi-field", rawField.containsKey("normalizer"))
        @Suppress("UNCHECKED_CAST")
        val descField = (mapping["properties"] as Map<*, *>)["desc"] as Map<*, *>
        assertFalse("analyzer must be stripped from sub-property field", descField.containsKey("analyzer"))
        assertEquals("type of sub-property field must be preserved", "text", descField["type"])
    }
}

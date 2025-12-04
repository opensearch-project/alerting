/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import org.opensearch.alerting.model.AlertContext
import org.opensearch.alerting.randomAction
import org.opensearch.alerting.randomBucketLevelTrigger
import org.opensearch.alerting.randomChainedAlertTrigger
import org.opensearch.alerting.randomDocumentLevelTrigger
import org.opensearch.alerting.randomQueryLevelTrigger
import org.opensearch.alerting.randomTemplateScript
import org.opensearch.alerting.script.BucketLevelTriggerExecutionContext
import org.opensearch.alerting.script.DocumentLevelTriggerExecutionContext
import org.opensearch.test.OpenSearchTestCase
import kotlin.test.Test

class AlertingUtilsTests : OpenSearchTestCase() {
    @Test
    fun `test parseSampleDocTags only returns expected tags`() {
        val expectedDocSourceTags = (0..3).map { "field$it" }
        val unexpectedDocSourceTags =
            ((expectedDocSourceTags.size + 1)..(expectedDocSourceTags.size + 5))
                .map { "field$it" }

        val unexpectedTagsScriptSource = unexpectedDocSourceTags.joinToString { field -> "$field = {{$field}}" }
        val expectedTagsScriptSource =
            unexpectedTagsScriptSource +
                """
                ${unexpectedDocSourceTags.joinToString("\n") { field -> "$field = {{$field}}" }}
                {{#alerts}}
                {{#${AlertContext.SAMPLE_DOCS_FIELD}}}
                    ${expectedDocSourceTags.joinToString("\n") { field -> "$field = {{_source.$field}}" }}
                {{/${AlertContext.SAMPLE_DOCS_FIELD}}}
                {{/alerts}}
                """.trimIndent()

        // Action that prints doc source data
        val trigger1 =
            randomDocumentLevelTrigger(
                actions = listOf(randomAction(template = randomTemplateScript(source = expectedTagsScriptSource))),
            )

        // Action that does not print doc source data
        val trigger2 =
            randomDocumentLevelTrigger(
                actions = listOf(randomAction(template = randomTemplateScript(source = unexpectedTagsScriptSource))),
            )

        // No actions
        val trigger3 = randomDocumentLevelTrigger(actions = listOf())

        val tags = parseSampleDocTags(listOf(trigger1, trigger2, trigger3))

        assertEquals(expectedDocSourceTags.size, tags.size)
        expectedDocSourceTags.forEach { tag -> assertTrue(tags.contains(tag)) }
        unexpectedDocSourceTags.forEach { tag -> assertFalse(tags.contains(tag)) }
    }

    @Test
    fun `test printsSampleDocData entire ctx tag returns TRUE`() {
        val tag = "{{ctx}}"
        val triggers =
            listOf(
                randomBucketLevelTrigger(actions = listOf(randomAction(template = randomTemplateScript(source = tag)))),
                randomDocumentLevelTrigger(actions = listOf(randomAction(template = randomTemplateScript(source = tag)))),
            )

        triggers.forEach { trigger -> assertTrue(printsSampleDocData(trigger)) }
    }

    @Test
    fun `test printsSampleDocData entire alerts tag returns TRUE`() {
        val triggers =
            listOf(
                randomBucketLevelTrigger(
                    actions =
                        listOf(
                            randomAction(
                                template =
                                    randomTemplateScript(
                                        source = "{{ctx.${BucketLevelTriggerExecutionContext.NEW_ALERTS_FIELD}}}",
                                    ),
                            ),
                        ),
                ),
                randomDocumentLevelTrigger(
                    actions =
                        listOf(
                            randomAction(
                                template =
                                    randomTemplateScript(
                                        source = "{{ctx.${DocumentLevelTriggerExecutionContext.ALERTS_FIELD}}}",
                                    ),
                            ),
                        ),
                ),
            )

        triggers.forEach { trigger -> assertTrue(printsSampleDocData(trigger)) }
    }

    @Test
    fun `test printsSampleDocData entire sample_docs tag returns TRUE`() {
        val triggers =
            listOf(
                randomBucketLevelTrigger(
                    actions =
                        listOf(
                            randomAction(
                                template =
                                    randomTemplateScript(
                                        source =
                                            """
                                            {{#ctx.${BucketLevelTriggerExecutionContext.NEW_ALERTS_FIELD}}}
                                                {{${AlertContext.SAMPLE_DOCS_FIELD}}}
                                            {{/ctx.${BucketLevelTriggerExecutionContext.NEW_ALERTS_FIELD}}}
                                            """.trimIndent(),
                                    ),
                            ),
                        ),
                ),
                randomDocumentLevelTrigger(
                    actions =
                        listOf(
                            randomAction(
                                template =
                                    randomTemplateScript(
                                        source =
                                            """
                                            {{#ctx.${DocumentLevelTriggerExecutionContext.ALERTS_FIELD}}}
                                                {{${AlertContext.SAMPLE_DOCS_FIELD}}}
                                            {{/ctx.${DocumentLevelTriggerExecutionContext.ALERTS_FIELD}}}
                                            """.trimIndent(),
                                    ),
                            ),
                        ),
                ),
            )

        triggers.forEach { trigger -> assertTrue(printsSampleDocData(trigger)) }
    }

    @Test
    fun `test printsSampleDocData sample_docs iteration block returns TRUE`() {
        val triggers =
            listOf(
                randomBucketLevelTrigger(
                    actions =
                        listOf(
                            randomAction(
                                template =
                                    randomTemplateScript(
                                        source =
                                            """
                                            {{#ctx.${BucketLevelTriggerExecutionContext.NEW_ALERTS_FIELD}}}
                                                "{{#${AlertContext.SAMPLE_DOCS_FIELD}}}"
                                                    {{_source.field}}
                                                "{{/${AlertContext.SAMPLE_DOCS_FIELD}}}"
                                            {{/ctx.${BucketLevelTriggerExecutionContext.NEW_ALERTS_FIELD}}}
                                            """.trimIndent(),
                                    ),
                            ),
                        ),
                ),
                randomDocumentLevelTrigger(
                    actions =
                        listOf(
                            randomAction(
                                template =
                                    randomTemplateScript(
                                        source =
                                            """
                                            {{#ctx.${DocumentLevelTriggerExecutionContext.ALERTS_FIELD}}}
                                                {{#${AlertContext.SAMPLE_DOCS_FIELD}}}
                                                    {{_source.field}}
                                                {{/${AlertContext.SAMPLE_DOCS_FIELD}}}
                                            {{/ctx.${DocumentLevelTriggerExecutionContext.ALERTS_FIELD}}}
                                            """.trimIndent(),
                                    ),
                            ),
                        ),
                ),
            )

        triggers.forEach { trigger -> assertTrue(printsSampleDocData(trigger)) }
    }

    @Test
    fun `test printsSampleDocData unrelated tag returns FALSE`() {
        val tag = "{{ctx.monitor.name}}"
        val triggers =
            listOf(
                randomBucketLevelTrigger(actions = listOf(randomAction(template = randomTemplateScript(source = tag)))),
                randomDocumentLevelTrigger(actions = listOf(randomAction(template = randomTemplateScript(source = tag)))),
            )

        triggers.forEach { trigger -> assertFalse(printsSampleDocData(trigger)) }
    }

    @Test
    fun `test printsSampleDocData unsupported trigger types return FALSE`() {
        val tag = "{{ctx}}"
        val triggers =
            listOf(
                randomQueryLevelTrigger(actions = listOf(randomAction(template = randomTemplateScript(source = tag)))),
                randomChainedAlertTrigger(actions = listOf(randomAction(template = randomTemplateScript(source = tag)))),
            )

        triggers.forEach { trigger -> assertFalse(printsSampleDocData(trigger)) }
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import com.fasterxml.jackson.databind.ObjectMapper
import org.mockito.Mockito
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoInteractions
import org.opensearch.common.settings.Settings
import org.opensearch.script.Script
import org.opensearch.script.ScriptService
import org.opensearch.script.ScriptType
import org.opensearch.script.TemplateScript
import org.opensearch.test.OpenSearchTestCase
import java.nio.file.Files
import java.nio.file.Path
import org.mockito.Mockito.`when` as whenever

class MustacheTemplateServiceTests : OpenSearchTestCase() {

    private fun createService(multiTenancyEnabled: Boolean): MustacheTemplateService {
        val settings = Settings.builder()
            .put("plugins.alerting.multi_tenancy_enabled", multiTenancyEnabled)
            .build()
        return MustacheTemplateService(Mockito.mock(ScriptService::class.java), settings)
    }

    private fun createServiceWithMockScriptService(
        multiTenancyEnabled: Boolean
    ): Pair<MustacheTemplateService, ScriptService> {
        val scriptService = Mockito.mock(ScriptService::class.java)
        val settings = Settings.builder()
            .put("plugins.alerting.multi_tenancy_enabled", multiTenancyEnabled)
            .build()
        return Pair(MustacheTemplateService(scriptService, settings), scriptService)
    }

    private fun renderWithBothMethods(template: String, params: Map<String, Any> = emptyMap()): List<String> {
        val service = createService(multiTenancyEnabled = true)
        val script = Script(ScriptType.INLINE, Script.DEFAULT_TEMPLATE_LANG, template, params)
        return listOf(service.renderTemplate(template, params), service.renderScript(script, emptyMap()))
    }

    private fun assertRenderingFails(template: String, params: Map<String, Any> = emptyMap(), message: String) {
        val (service, scriptService) = createServiceWithMockScriptService(multiTenancyEnabled = true)
        val script = Script(ScriptType.INLINE, Script.DEFAULT_TEMPLATE_LANG, template, params)
        val renderers = listOf<() -> String>(
            { service.renderTemplate(template, params) },
            { service.renderScript(script, emptyMap()) }
        )
        for (render in renderers) {
            val exception = expectThrows(Exception::class.java) { render() }
            assertTrue(
                "Expected rejection containing '$message', got $exception",
                generateSequence<Throwable>(exception) { it.cause }.any { it.message?.contains(message) == true }
            )
        }
        verifyNoInteractions(scriptService)
    }

    private fun createPartialFile(): Path = Files.writeString(
        createTempDir().resolve("partial.mustache"),
        "alerting-test-file-content"
    )

    // --- Restricted resource resolution (both rendering entry points) ---

    fun `test render methods reject all partial resource names`() {
        val partial = createPartialFile()
        val relativePath = partial.fileSystem.getPath("").toAbsolutePath().relativize(partial.toAbsolutePath())
        val resources = listOf(
            "named-partial",
            partial.toString(),
            partial.toUri().toString(),
            relativePath.toString(),
            "mockito-extensions/org.mockito.plugins.MockMaker"
        )

        for (resource in resources) {
            assertRenderingFails(
                "before {{>$resource}} after",
                message = "Partial templates are not supported in Alerting"
            )
        }
    }

    fun `test render methods reject partials with alternate delimiters or inactive sections`() {
        val resource = createPartialFile().toUri().toString()
        val templates = listOf(
            "{{=<% %>=}}<%>$resource%>",
            "{{#missing}}{{>$resource}}{{/missing}}"
        )

        for (template in templates) {
            assertRenderingFails(template, message = "Partial templates are not supported in Alerting")
        }
    }

    fun `test render methods reject dynamic partials`() {
        val partial = createPartialFile()
        for (resource in listOf(partial.toString(), partial.toUri().toString())) {
            assertRenderingFails(
                "{{>*partialName}}",
                mapOf("partialName" to resource),
                "Partial templates are not supported in Alerting"
            )
        }
    }

    fun `test render methods reject inherited templates`() {
        val resource = createPartialFile().toUri().toString()
        assertRenderingFails(
            "{{<$resource}}{{\$body}}replacement{{/body}}{{/$resource}}",
            message = "Partial templates are not supported in Alerting"
        )
    }

    // --- Escaping and supported Mustache behavior ---

    fun `test render methods JSON escape special characters`() {
        val value = "\"quotes\"\\path\n\r\t\b\u000c\u0000\u001f<>&=' / café 雪 \uD83D\uDE00"
        val mapper = ObjectMapper()
        val expected = mapper.writeValueAsString(mapOf("value" to value))

        for (rendered in renderWithBothMethods("""{"value":"{{value}}"}""", mapOf("value" to value))) {
            assertEquals(expected, rendered)
            assertEquals(value, mapper.readTree(rendered).get("value").asText())
        }
    }

    fun `test render methods preserve nested maps and sections`() {
        val params = mapOf<String, Any>(
            "ctx" to mapOf(
                "monitor" to mapOf("name" to "cpu-monitor"),
                "results" to mapOf("0" to mapOf("count" to 2)),
                "alerts" to listOf(mapOf("name" to "first"), mapOf("name" to "second"))
            )
        )
        val template = "{{ctx.monitor.name}}={{ctx.results.0.count}}:" +
            "{{#ctx.alerts}}[{{name}}]{{/ctx.alerts}}{{^missing}}none{{/missing}}"

        for (rendered in renderWithBothMethods(template, params)) {
            assertEquals("cpu-monitor=2:[first][second]none", rendered)
        }
    }

    fun `test render methods preserve explicitly unescaped values`() {
        val value = "\"quoted\"\\path\n<>&"
        for (template in listOf("{{{value}}}", "{{&value}}")) {
            for (rendered in renderWithBothMethods(template, mapOf("value" to value))) {
                assertEquals(value, rendered)
            }
        }
    }

    fun `test render methods do not interpret partial syntax in parameter values`() {
        val value = "{{>named-partial}}"
        for (rendered in renderWithBothMethods("{{value}}", mapOf("value" to value))) {
            assertEquals(value, rendered)
        }
    }

    fun `test renderScript additional parameters take precedence`() {
        val service = createService(multiTenancyEnabled = true)
        val script = Script(
            ScriptType.INLINE,
            Script.DEFAULT_TEMPLATE_LANG,
            "{{name}} {{ctx.value}} {{retained}}",
            mapOf("name" to "original", "ctx" to mapOf("value" to "original-context"), "retained" to "retained")
        )

        val rendered = service.renderScript(script, mapOf("name" to "replacement", "ctx" to mapOf("value" to "injected")))

        assertEquals("replacement injected retained", rendered)
    }

    // --- Rendered output limits (both rendering entry points) ---

    fun `test render methods enforce output limit for literal templates`() {
        val limit = MustacheTemplateService.MAX_TEMPLATE_OUTPUT_CHARS
        val atLimit = "a".repeat(limit)
        for (rendered in renderWithBothMethods(atLimit)) {
            assertEquals(atLimit, rendered)
        }

        assertRenderingFails(
            atLimit + "a",
            message = "Rendered template exceeds the maximum output size of $limit UTF-16 code units"
        )
    }

    fun `test render methods enforce cumulative output limit for variable values`() {
        val limit = MustacheTemplateService.MAX_TEMPLATE_OUTPUT_CHARS
        val value = "a".repeat(limit - 2)
        val template = "[{{value}}]"
        for (rendered in renderWithBothMethods(template, mapOf("value" to value))) {
            assertEquals("[$value]", rendered)
        }

        assertRenderingFails(
            template,
            mapOf("value" to value + "a"),
            "Rendered template exceeds the maximum output size of $limit UTF-16 code units"
        )
    }

    fun `test render methods enforce output limit during section expansion`() {
        val limit = MustacheTemplateService.MAX_TEMPLATE_OUTPUT_CHARS
        val value = "a".repeat(4096)
        val items = List(limit / value.length) { mapOf("value" to value) }
        val template = "{{#items}}{{value}}{{/items}}"
        for (rendered in renderWithBothMethods(template, mapOf("items" to items))) {
            assertEquals("a".repeat(limit), rendered)
        }

        assertRenderingFails(
            template,
            mapOf("items" to items + mapOf("value" to "a")),
            "Rendered template exceeds the maximum output size of $limit UTF-16 code units"
        )
    }

    fun `test render methods count JSON escape expansion toward output limit`() {
        val limit = MustacheTemplateService.MAX_TEMPLATE_OUTPUT_CHARS
        val value = "\n".repeat(limit / 2)
        for (rendered in renderWithBothMethods("{{value}}", mapOf("value" to value))) {
            assertEquals("\\n".repeat(limit / 2), rendered)
        }

        assertRenderingFails(
            "{{value}}",
            mapOf("value" to value + "\n"),
            "Rendered template exceeds the maximum output size of $limit UTF-16 code units"
        )
    }

    fun `test render methods enforce output limit for unescaped values`() {
        val limit = MustacheTemplateService.MAX_TEMPLATE_OUTPUT_CHARS
        val value = "\uD83D\uDE00".repeat(limit / 2)
        for (template in listOf("{{{value}}}", "{{&value}}")) {
            for (rendered in renderWithBothMethods(template, mapOf("value" to value))) {
                assertEquals(value, rendered)
            }

            assertRenderingFails(
                template,
                mapOf("value" to value + "a"),
                "Rendered template exceeds the maximum output size of $limit UTF-16 code units"
            )
        }
    }

    // --- renderTemplate tests (multi-tenancy enabled = direct mustache) ---

    fun `test renderTemplate replaces period_start and period_end`() {
        val service = createService(multiTenancyEnabled = true)
        val template = """{"query":{"range":{"timestamp":{"gte":"{{period_start}}","lte":"{{period_end}}"}}}}"""
        val params = mapOf<String, Any>("period_start" to 1780507716780L, "period_end" to 1780507776780L)

        val result = service.renderTemplate(template, params)

        assertEquals(
            """{"query":{"range":{"timestamp":{"gte":"1780507716780","lte":"1780507776780"}}}}""",
            result
        )
    }

    fun `test renderTemplate with no placeholders returns unchanged`() {
        val service = createService(multiTenancyEnabled = true)
        val template = """{"query":{"match_all":{"boost":1.0}}}"""
        val params = mapOf<String, Any>("period_start" to 1000L, "period_end" to 2000L)

        val result = service.renderTemplate(template, params)

        assertEquals(template, result)
    }

    fun `test renderTemplate replaces multiple occurrences`() {
        val service = createService(multiTenancyEnabled = true)
        val template = """{"from":"{{period_start}}","to":"{{period_end}}","check":"{{period_start}}"}"""
        val params = mapOf<String, Any>("period_start" to 100L, "period_end" to 200L)

        val result = service.renderTemplate(template, params)

        assertEquals("""{"from":"100","to":"200","check":"100"}""", result)
    }

    fun `test renderTemplate with nested context object`() {
        val service = createService(multiTenancyEnabled = true)
        val template = "Monitor {{ctx.monitor.name}} triggered alert {{ctx.alert.id}}"
        val ctx = mapOf(
            "monitor" to mapOf("name" to "test-monitor"),
            "alert" to mapOf("id" to "abc-123")
        )
        val params = mapOf<String, Any>("ctx" to ctx)

        val result = service.renderTemplate(template, params)

        assertEquals("Monitor test-monitor triggered alert abc-123", result)
    }

    fun `test renderTemplate with empty params`() {
        val service = createService(multiTenancyEnabled = true)
        val template = """{"query":{"match_all":{}}}"""
        val params = emptyMap<String, Any>()

        val result = service.renderTemplate(template, params)

        assertEquals(template, result)
    }

    fun `test renderTemplate with empty template`() {
        val service = createService(multiTenancyEnabled = true)
        val result = service.renderTemplate("", mapOf("period_start" to 100L))
        assertEquals("", result)
    }

    // --- renderScript tests (multi-tenancy enabled = direct mustache) ---

    fun `test renderScript merges script params with additional params`() {
        val service = createService(multiTenancyEnabled = true)
        val script = Script(
            ScriptType.INLINE,
            Script.DEFAULT_TEMPLATE_LANG,
            "Alert {{name}} fired at {{ctx.time}}",
            mapOf("name" to "my-alert")
        )
        val additionalParams = mapOf<String, Any>("ctx" to mapOf("time" to "2026-06-03"))

        val result = service.renderScript(script, additionalParams)

        assertEquals("Alert my-alert fired at 2026-06-03", result)
    }

    fun `test renderScript with only additional params`() {
        val service = createService(multiTenancyEnabled = true)
        val script = Script(
            ScriptType.INLINE,
            Script.DEFAULT_TEMPLATE_LANG,
            "Monitor: {{ctx.monitor.name}}",
            emptyMap()
        )
        val additionalParams = mapOf<String, Any>(
            "ctx" to mapOf("monitor" to mapOf("name" to "cpu-monitor"))
        )

        val result = service.renderScript(script, additionalParams)

        assertEquals("Monitor: cpu-monitor", result)
    }

    // --- Delegation tests (multi-tenancy disabled = ScriptService) ---

    fun `test renderTemplate delegates to scriptService when multi-tenancy disabled`() {
        val (service, scriptService) = createServiceWithMockScriptService(multiTenancyEnabled = false)
        val mockFactory = Mockito.mock(TemplateScript.Factory::class.java)
        val mockInstance = Mockito.mock(TemplateScript::class.java)
        val template = """{"query":{"match_all":{}}}"""
        val params = mapOf<String, Any>("period_start" to 100L)

        whenever(scriptService.compile(Mockito.any(Script::class.java), Mockito.eq(TemplateScript.CONTEXT)))
            .thenReturn(mockFactory)
        whenever(mockFactory.newInstance(params)).thenReturn(mockInstance)
        whenever(mockInstance.execute()).thenReturn("rendered-output")

        val result = service.renderTemplate(template, params)

        assertEquals("rendered-output", result)
        verify(scriptService).compile(Mockito.any(Script::class.java), Mockito.eq(TemplateScript.CONTEXT))
    }

    fun `test renderTemplate does not call scriptService when multi-tenancy enabled`() {
        val (service, scriptService) = createServiceWithMockScriptService(multiTenancyEnabled = true)
        val template = """{"query":{"match_all":{}}}"""

        service.renderTemplate(template, mapOf("period_start" to 100L))

        verifyNoInteractions(scriptService)
    }

    fun `test renderScript delegates to scriptService when multi-tenancy disabled`() {
        val (service, scriptService) = createServiceWithMockScriptService(multiTenancyEnabled = false)
        val mockFactory = Mockito.mock(TemplateScript.Factory::class.java)
        val mockInstance = Mockito.mock(TemplateScript::class.java)
        val script = Script(ScriptType.INLINE, Script.DEFAULT_TEMPLATE_LANG, "hello {{ctx.name}}", emptyMap())
        val additionalParams = mapOf<String, Any>("ctx" to mapOf("name" to "world"))

        whenever(scriptService.compile(Mockito.eq(script), Mockito.eq(TemplateScript.CONTEXT)))
            .thenReturn(mockFactory)
        whenever(mockFactory.newInstance(additionalParams)).thenReturn(mockInstance)
        whenever(mockInstance.execute()).thenReturn("hello world")

        val result = service.renderScript(script, additionalParams)

        assertEquals("hello world", result)
        verify(scriptService).compile(Mockito.eq(script), Mockito.eq(TemplateScript.CONTEXT))
    }
}

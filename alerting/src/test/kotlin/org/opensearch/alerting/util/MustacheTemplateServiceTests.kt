/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import org.mockito.Mockito
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoInteractions
import org.opensearch.common.settings.Settings
import org.opensearch.script.Script
import org.opensearch.script.ScriptService
import org.opensearch.script.ScriptType
import org.opensearch.script.TemplateScript
import org.opensearch.test.OpenSearchTestCase
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

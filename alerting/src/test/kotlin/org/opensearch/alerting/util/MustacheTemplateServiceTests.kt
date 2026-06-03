/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import org.mockito.Mockito
import org.opensearch.common.settings.Settings
import org.opensearch.script.ScriptService
import org.opensearch.test.OpenSearchTestCase

class MustacheTemplateServiceTests : OpenSearchTestCase() {

    private fun createService(multiTenancyEnabled: Boolean): MustacheTemplateService {
        val settings = Settings.builder()
            .put("plugins.alerting.multi_tenancy_enabled", multiTenancyEnabled)
            .build()
        return MustacheTemplateService(Mockito.mock(ScriptService::class.java), settings)
    }

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
}

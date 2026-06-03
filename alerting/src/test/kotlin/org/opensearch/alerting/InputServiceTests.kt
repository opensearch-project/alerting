/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.mockito.Mockito
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.core.common.io.stream.NamedWriteableRegistry
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.script.ScriptService
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.transport.client.Client

class InputServiceTests : OpenSearchTestCase() {

    private fun createInputService(multiTenancyEnabled: Boolean): InputService {
        val settings = Settings.builder()
            .put("plugins.alerting.multi_tenancy_enabled", multiTenancyEnabled)
            .build()
        return InputService(
            Mockito.mock(Client::class.java),
            Mockito.mock(ScriptService::class.java),
            Mockito.mock(NamedWriteableRegistry::class.java),
            Mockito.mock(NamedXContentRegistry::class.java),
            Mockito.mock(ClusterService::class.java),
            settings,
            Mockito.mock(IndexNameExpressionResolver::class.java)
        )
    }

    fun `test resolveTemplateParams replaces period_start and period_end`() {
        val inputService = createInputService(multiTenancyEnabled = true)
        val template = """{"query":{"range":{"timestamp":{"gte":"{{period_start}}","lte":"{{period_end}}"}}}}"""
        val params = mapOf<String, Any>("period_start" to 1780507716780L, "period_end" to 1780507776780L)

        val result = inputService.resolveTemplateParams(template, params)

        assertEquals(
            """{"query":{"range":{"timestamp":{"gte":"1780507716780","lte":"1780507776780"}}}}""",
            result
        )
    }

    fun `test resolveTemplateParams with no placeholders returns unchanged`() {
        val inputService = createInputService(multiTenancyEnabled = true)
        val template = """{"query":{"match_all":{"boost":1.0}}}"""
        val params = mapOf<String, Any>("period_start" to 1000L, "period_end" to 2000L)

        val result = inputService.resolveTemplateParams(template, params)

        assertEquals(template, result)
    }

    fun `test resolveTemplateParams replaces multiple occurrences`() {
        val inputService = createInputService(multiTenancyEnabled = true)
        val template = """{"from":"{{period_start}}","to":"{{period_end}}","check":"{{period_start}}"}"""
        val params = mapOf<String, Any>("period_start" to 100L, "period_end" to 200L)

        val result = inputService.resolveTemplateParams(template, params)

        assertEquals("""{"from":"100","to":"200","check":"100"}""", result)
    }

    fun `test resolveTemplateParams with empty params`() {
        val inputService = createInputService(multiTenancyEnabled = true)
        val template = """{"query":{"match_all":{}}}"""
        val params = emptyMap<String, Any>()

        val result = inputService.resolveTemplateParams(template, params)

        assertEquals(template, result)
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import com.github.mustachejava.DefaultMustacheFactory
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.common.settings.Settings
import org.opensearch.script.Script
import org.opensearch.script.ScriptService
import org.opensearch.script.ScriptType
import org.opensearch.script.TemplateScript
import java.io.StringReader
import java.io.StringWriter

/**
 * Handles mustache template compilation and rendering.
 *
 * When multi-tenancy is enabled, inline scripting is disabled to prevent painless execution.
 * This class uses the mustache-java library directly in that case, bypassing ScriptService.
 * When multi-tenancy is disabled, it delegates to ScriptService as before.
 */
class MustacheTemplateService(
    private val scriptService: ScriptService,
    settings: Settings
) {
    private val multiTenancyEnabled = AlertingSettings.MULTI_TENANCY_ENABLED.get(settings)
    private val mustacheFactory = DefaultMustacheFactory()

    /**
     * Compiles and renders an inline mustache template with the given parameters.
     * Equivalent to: scriptService.compile(Script(INLINE, DEFAULT_TEMPLATE_LANG, template, params), TemplateScript.CONTEXT)
     *     .newInstance(params).execute()
     */
    fun renderTemplate(template: String, params: Map<String, Any>): String {
        return if (multiTenancyEnabled) {
            renderDirectly(template, params)
        } else {
            scriptService.compile(
                Script(ScriptType.INLINE, Script.DEFAULT_TEMPLATE_LANG, template, params),
                TemplateScript.CONTEXT
            )
                .newInstance(params)
                .execute()
        }
    }

    /**
     * Compiles and renders a Script object with additional context parameters merged in.
     * Used for notification action templates where ctx is injected at render time.
     */
    fun renderScript(script: Script, additionalParams: Map<String, Any>): String {
        val allParams = script.params + additionalParams
        return if (multiTenancyEnabled) {
            renderDirectly(script.idOrCode, allParams)
        } else {
            scriptService.compile(script, TemplateScript.CONTEXT)
                .newInstance(allParams)
                .execute()
        }
    }

    private fun renderDirectly(template: String, params: Map<String, Any>): String {
        val mustache = mustacheFactory.compile(StringReader(template), "template")
        val writer = StringWriter()
        mustache.execute(writer, params)
        return writer.toString()
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.common.settings.Settings
import org.opensearch.script.Script
import org.opensearch.script.ScriptService
import org.opensearch.script.ScriptType
import org.opensearch.script.TemplateScript
import java.io.StringReader

/**
 * Handles mustache template compilation and rendering.
 *
 * When multi-tenancy is enabled, inline scripting is disabled to prevent painless execution.
 * This class uses a restricted mustache-java factory in that case, bypassing ScriptService.
 * Direct rendering rejects external template resources, escapes values as JSON string content,
 * and limits the rendered output size.
 * When multi-tenancy is disabled, it delegates to ScriptService as before.
 */
class MustacheTemplateService(
    private val scriptService: ScriptService,
    settings: Settings
) {
    private val multiTenancyEnabled = AlertingSettings.MULTI_TENANCY_ENABLED.get(settings)
    private val mustacheFactory = RestrictedMustacheFactory()

    companion object {
        // Per rendered template, measured in UTF-16 code units rather than encoded bytes.
        internal const val MAX_TEMPLATE_OUTPUT_CHARS = 1_048_576
    }

    /**
     * Compiles and renders an inline mustache template with the given parameters.
     * Uses restricted direct rendering in multi-tenant mode and ScriptService otherwise.
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
        val writer = BoundedTemplateWriter(MAX_TEMPLATE_OUTPUT_CHARS)
        mustache.execute(writer, params).flush()
        return writer.toString()
    }
}

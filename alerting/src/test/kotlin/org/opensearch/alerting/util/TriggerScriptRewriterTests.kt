/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import org.opensearch.test.OpenSearchTestCase

class TriggerScriptRewriterTests : OpenSearchTestCase() {

    fun `test rewrite simple threshold`() {
        val source = "ctx.results[0].hits.total.value > 100"
        val rewritten = TriggerScriptRewriter.rewriteScript(source)
        assertEquals("params.results_0.hits.total.value > 100", rewritten)
    }

    fun `test rewrite aggregation access`() {
        val source = "ctx.results[0].aggregations.avg_cpu.value > 90"
        val rewritten = TriggerScriptRewriter.rewriteScript(source)
        assertEquals("params.results_0.aggregations.avg_cpu.value > 90", rewritten)
    }

    fun `test rewrite multiple occurrences`() {
        val source = "ctx.results[0].hits.total.value > 0 && ctx.results[0].hits.hits.size() > 0"
        val rewritten = TriggerScriptRewriter.rewriteScript(source)
        assertEquals("params.results_0.hits.total.value > 0 && params.results_0.hits.hits.size() > 0", rewritten)
    }

    fun `test rewrite preserves non-ctx content`() {
        val source = "def x = ctx.results[0].hits.total.value; return x > 0"
        val rewritten = TriggerScriptRewriter.rewriteScript(source)
        assertEquals("def x = params.results_0.hits.total.value; return x > 0", rewritten)
    }

    fun `test rewrite with no ctx reference`() {
        val source = "return true"
        val rewritten = TriggerScriptRewriter.rewriteScript(source)
        assertEquals("return true", rewritten)
    }

    fun `test rewrite loop over hits`() {
        val source = "for (def hit : ctx.results[0].hits.hits) { if (hit._source.status == 500) return true } return false"
        val rewritten = TriggerScriptRewriter.rewriteScript(source)
        assertEquals(
            "for (def hit : params.results_0.hits.hits) { if (hit._source.status == 500) return true } return false",
            rewritten
        )
    }
}

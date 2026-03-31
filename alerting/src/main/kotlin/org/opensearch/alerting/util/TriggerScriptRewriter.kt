/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

/**
 * Rewrites Painless trigger scripts for remote evaluation on the customer's cluster.
 *
 * In multi-tenant mode, trigger scripts cannot be executed on the Oasis node.
 * Instead, they are sent to the customer's cluster via a filter aggregation.
 * The script context on the customer's cluster uses `params` instead of `ctx`,
 * so all references to `ctx.results[0]` must be replaced with `params.results_0`.
 */
object TriggerScriptRewriter {

    private const val CTX_RESULTS_0 = "ctx.results[0]"
    private const val PARAMS_RESULTS_0 = "params.results_0"

    /**
     * Replaces all occurrences of `ctx.results[0]` with `params.results_0` in the given script source.
     * MONITOR_MAX_INPUTS = 1, so only `ctx.results[0]` ever exists.
     */
    fun rewriteScript(source: String): String {
        return source.replace(CTX_RESULTS_0, PARAMS_RESULTS_0)
    }
}

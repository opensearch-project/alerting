/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.chainedAlertCondition.resolvers

sealed class TriggerExpression {

    fun resolve(): Boolean = when (this) {
        is And -> resolveAnd(boolean1, boolean2)
        is Or -> resolveOr(boolean1, boolean2)
        is Not -> resolveNot(result, boolean2)
    }

    private fun resolveAnd(boolean1: Boolean, boolean2: Boolean): Boolean {
        return boolean1 && boolean2
    }

    private fun resolveOr(boolean1: Boolean, boolean2: Boolean): Boolean {
        return boolean1 || boolean2
    }

    private fun resolveNot(result: Boolean, boolean2: Boolean): Boolean {
            return result && !boolean2
    }

    // Operators implemented as operator functions
    class And(val boolean1: Boolean, val boolean2: Boolean) : TriggerExpression()
    class Or(val boolean1: Boolean, val boolean2: Boolean) : TriggerExpression()
    class Not(val result: Boolean, val boolean2: Boolean) : TriggerExpression()
}

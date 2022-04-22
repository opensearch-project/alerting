/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.triggercondition.resolvers

sealed class TriggerExpression {

    fun resolve(): Set<String> = when (this) {
        is And -> resolveAnd(docSet1, docSet2)
        is Or -> resolveOr(docSet1, docSet2)
        is Not -> resolveNot(allDocs, docSet2)
    }

    private fun resolveAnd(documentSet1: Set<String>, documentSet2: Set<String>): Set<String> {
        return documentSet1.intersect(documentSet2)
    }

    private fun resolveOr(documentSet1: Set<String>, documentSet2: Set<String>): Set<String> {
        return documentSet1.union(documentSet2)
    }

    private fun resolveNot(allDocs: Set<String>, documentSet2: Set<String>): Set<String> {
        return allDocs.subtract(documentSet2)
    }

    // Operators implemented as operator functions
    class And(val docSet1: Set<String>, val docSet2: Set<String>) : TriggerExpression()
    class Or(val docSet1: Set<String>, val docSet2: Set<String>) : TriggerExpression()
    class Not(val allDocs: Set<String>, val docSet2: Set<String>) : TriggerExpression()
}

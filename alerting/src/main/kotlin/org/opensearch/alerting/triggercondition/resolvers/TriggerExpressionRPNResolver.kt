/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.triggercondition.resolvers

import org.opensearch.alerting.triggercondition.tokens.ExpressionToken
import org.opensearch.alerting.triggercondition.tokens.TriggerExpressionConstant
import org.opensearch.alerting.triggercondition.tokens.TriggerExpressionOperator
import org.opensearch.alerting.triggercondition.tokens.TriggerExpressionToken
import org.opensearch.commons.alerting.model.DocLevelQuery
import java.util.*

/**
 * Solves the Trigger Expression using the Reverse Polish Notation (RPN) based solver
 * @param polishNotation an array of expression tokens organized in the RPN order
 */
class TriggerExpressionRPNResolver(
    private val polishNotation: ArrayList<ExpressionToken>
) : TriggerExpressionResolver {

    private val eqString by lazy {
        val stringBuilder = StringBuilder()
        for (expToken in polishNotation) {
            when (expToken) {
                is TriggerExpressionToken -> stringBuilder.append(expToken.value)
                is TriggerExpressionOperator -> stringBuilder.append(expToken.value)
                is TriggerExpressionConstant -> stringBuilder.append(expToken.type.ident)
                else -> throw Exception()
            }
            stringBuilder.append(" ")
        }
        stringBuilder.toString()
    }

    override fun toString(): String = eqString

    /**
     * Evaluates the trigger expression expressed provided in form of the RPN token array.
     * @param queryToDocIds Map to hold the resultant document id per query id
     * @return evaluates the final set of document id
     */
    override fun evaluate(queryToDocIds: Map<DocLevelQuery, Set<String>>): Set<String> {
        val tokenStack = Stack<Set<String>>()

        val allDocIds = mutableSetOf<String>()
        for (value in queryToDocIds.values) {
            allDocIds.addAll(value)
        }

        for (expToken in polishNotation) {
            when (expToken) {
                is TriggerExpressionToken -> tokenStack.push(resolveQueryExpression(expToken.value, queryToDocIds))
                is TriggerExpressionOperator -> {
                    val right = tokenStack.pop()
                    val expr = when (expToken) {
                        TriggerExpressionOperator.AND -> TriggerExpression.And(tokenStack.pop(), right)
                        TriggerExpressionOperator.OR -> TriggerExpression.Or(tokenStack.pop(), right)
                        TriggerExpressionOperator.NOT -> TriggerExpression.Not(allDocIds, right)
                        else -> throw IllegalArgumentException("No matching operator.")
                    }
                    tokenStack.push(expr.resolve())
                }
            }
        }
        return tokenStack.pop()
    }

    private fun resolveQueryExpression(queryExpString: String, queryToDocIds: Map<DocLevelQuery, Set<String>>): Set<String> {
        if (!queryExpString.startsWith(TriggerExpressionConstant.ConstantType.QUERY.ident)) return emptySet()
        val token = queryExpString.substringAfter(TriggerExpressionConstant.ConstantType.BRACKET_LEFT.ident)
            .substringBefore(TriggerExpressionConstant.ConstantType.BRACKET_RIGHT.ident)
        if (token.isEmpty()) return emptySet()

        val tokens = token.split(TriggerExpressionConstant.ConstantType.EQUALS.ident)
        if (tokens.isEmpty() || tokens.size != 2) return emptySet()

        val identifier = tokens[0]
        val value = tokens[1]
        val documents = mutableSetOf<String>()
        when (identifier) {
            TriggerExpressionConstant.ConstantType.NAME.ident -> {
                val key: Optional<DocLevelQuery> = queryToDocIds.keys.stream().filter { it.name == value }.findFirst()
                if (key.isPresent) queryToDocIds[key.get()]?.let { doc -> documents.addAll(doc) }
            }

            TriggerExpressionConstant.ConstantType.ID.ident -> {
                val key: Optional<DocLevelQuery> = queryToDocIds.keys.stream().filter { it.id == value }.findFirst()
                if (key.isPresent) queryToDocIds[key.get()]?.let { doc -> documents.addAll(doc) }
            }

            // Iterate through all the queries with the same Tag
            TriggerExpressionConstant.ConstantType.TAG.ident -> {
                queryToDocIds.keys.stream().forEach {
                    if (it.tags.contains(value)) queryToDocIds[it]?.let { it1 -> documents.addAll(it1) }
                }
            }
        }
        return documents
    }
}

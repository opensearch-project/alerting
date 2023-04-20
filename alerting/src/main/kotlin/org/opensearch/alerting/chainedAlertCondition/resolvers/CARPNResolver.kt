/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.chainedAlertCondition.resolvers

import org.opensearch.alerting.chainedAlertCondition.tokens.ExpressionToken
import org.opensearch.alerting.chainedAlertCondition.tokens.CAExpressionConstant
import org.opensearch.alerting.chainedAlertCondition.tokens.CAExpressionOperator
import org.opensearch.alerting.chainedAlertCondition.tokens.CAExpressionToken
import java.util.Optional
import java.util.Stack

/**
 * Solves the Trigger Expression using the Reverse Polish Notation (RPN) based solver
 * @param polishNotation an array of expression tokens organized in the RPN order
 */
class CARPNResolver(
    private val polishNotation: ArrayList<ExpressionToken>
) : CAResolver {

    private val eqString by lazy {
        val stringBuilder = StringBuilder()
        for (expToken in polishNotation) {
            when (expToken) {
                is CAExpressionToken -> stringBuilder.append(expToken.value)
                is CAExpressionOperator -> stringBuilder.append(expToken.value)
                is CAExpressionConstant -> stringBuilder.append(expToken.type.ident)
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
    override fun evaluate(monitorIdAlertPresentMap: Map<String, Boolean>): Boolean {
        val tokenStack = Stack<Boolean>()
        if(monitorIdAlertPresentMap.size == 0)
            return false;
        val res = true
        for (expToken in polishNotation) {
            when (expToken) {
                is CAExpressionToken -> tokenStack.push(resolveMonitorExpression(expToken.value, monitorIdAlertPresentMap))
                is CAExpressionOperator -> {
                    val right = tokenStack.pop()
                    val expr = when (expToken) {
                        CAExpressionOperator.AND -> TriggerExpression.And(tokenStack.pop(), right)
                        CAExpressionOperator.OR -> TriggerExpression.Or(tokenStack.pop(), right)
                        CAExpressionOperator.NOT -> TriggerExpression.Not(res, right)
                        else -> throw IllegalArgumentException("No matching operator.")
                    }
                    tokenStack.push(expr.resolve())
                }
            }
        }
        return tokenStack.pop()
    }

    private fun resolveMonitorExpression(monitorExpString: String, monitorIdAlertPresentMap: Map<String, Boolean>): Boolean {
        if (!monitorExpString.startsWith(CAExpressionConstant.ConstantType.MONITOR.ident)) return false
        val token = monitorExpString.substringAfter(CAExpressionConstant.ConstantType.BRACKET_LEFT.ident)
            .substringBefore(CAExpressionConstant.ConstantType.BRACKET_RIGHT.ident)
        if (token.isEmpty()) return false

        val tokens = token.split(CAExpressionConstant.ConstantType.EQUALS.ident)
        if (tokens.isEmpty() || tokens.size != 2) return false

        val identifier = tokens[0]
        val value = tokens[1]
        var isAlertPresent = false
        when (identifier) {
            CAExpressionConstant.ConstantType.ID.ident -> {
                val key: Optional<String> = monitorIdAlertPresentMap.keys.stream().filter { it == value }.findFirst()
                if (key.isPresent) {
                    isAlertPresent = monitorIdAlertPresentMap[key.get()]!!
                }
            }
        }
        return isAlertPresent
    }
}

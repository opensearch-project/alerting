/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.chainedAlertCondition.resolvers

import org.opensearch.alerting.chainedAlertCondition.tokens.CAExpressionOperator
import org.opensearch.alerting.chainedAlertCondition.tokens.CAExpressionToken
import org.opensearch.alerting.chainedAlertCondition.tokens.ChainedAlertExpressionConstant
import org.opensearch.alerting.chainedAlertCondition.tokens.ExpressionToken
import java.util.Stack

/**
 * Solves the Trigger Expression using the Reverse Polish Notation (RPN) based solver
 * @param polishNotation an array of expression tokens organized in the RPN order
 */
class ChainedAlertRPNResolver(
    private val polishNotation: ArrayList<ExpressionToken>,
) : ChainedAlertTriggerResolver {

    private val eqString by lazy {
        val stringBuilder = StringBuilder()
        for (expToken in polishNotation) {
            when (expToken) {
                is CAExpressionToken -> stringBuilder.append(expToken.value)
                is CAExpressionOperator -> stringBuilder.append(expToken.value)
                is ChainedAlertExpressionConstant -> stringBuilder.append(expToken.type.ident)
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
    override fun evaluate(alertGeneratingMonitors: Set<String>): Boolean {
        val tokenStack = Stack<Boolean>()
        val res = true
        for (expToken in polishNotation) {
            when (expToken) {
                is CAExpressionToken -> tokenStack.push(resolveMonitorExpression(expToken.value, alertGeneratingMonitors))
                is CAExpressionOperator -> {
                    val right = tokenStack.pop()
                    val expr = when (expToken) {
                        CAExpressionOperator.AND -> ChainedAlertTriggerExpression.And(tokenStack.pop(), right)
                        CAExpressionOperator.OR -> ChainedAlertTriggerExpression.Or(tokenStack.pop(), right)
                        CAExpressionOperator.NOT -> ChainedAlertTriggerExpression.Not(res, right)
                        else -> throw IllegalArgumentException("No matching operator.")
                    }
                    tokenStack.push(expr.resolve())
                }
            }
        }
        return tokenStack.pop()
    }

    override fun getMonitorIds(parsedTriggerCondition: ChainedAlertRPNResolver): Set<String> {
        val monitorIds = mutableSetOf<String>()
        for (expToken in polishNotation) {
            when (expToken) {
                is CAExpressionToken -> {
                    val monitorExpString = expToken.value
                    if (!monitorExpString.startsWith(ChainedAlertExpressionConstant.ConstantType.MONITOR.ident))
                        continue
                    val token = monitorExpString.substringAfter(ChainedAlertExpressionConstant.ConstantType.BRACKET_LEFT.ident)
                        .substringBefore(ChainedAlertExpressionConstant.ConstantType.BRACKET_RIGHT.ident)
                    if (token.isEmpty()) continue
                    val tokens = token.split(ChainedAlertExpressionConstant.ConstantType.EQUALS.ident)
                    if (tokens.isEmpty() || tokens.size != 2) continue
                    val identifier = tokens[0]
                    val value = tokens[1]
                    when (identifier) {
                        ChainedAlertExpressionConstant.ConstantType.ID.ident -> {
                            monitorIds.add(value)
                        }
                    }
                }
                is CAExpressionOperator -> {
                    continue
                }
            }
        }
        return monitorIds
    }

    private fun resolveMonitorExpression(monitorExpString: String, alertGeneratingMonitors: Set<String>): Boolean {
        if (!monitorExpString.startsWith(ChainedAlertExpressionConstant.ConstantType.MONITOR.ident)) return false
        val token = monitorExpString.substringAfter(ChainedAlertExpressionConstant.ConstantType.BRACKET_LEFT.ident)
            .substringBefore(ChainedAlertExpressionConstant.ConstantType.BRACKET_RIGHT.ident)
        if (token.isEmpty()) return false

        val tokens = token.split(ChainedAlertExpressionConstant.ConstantType.EQUALS.ident)
        if (tokens.isEmpty() || tokens.size != 2) return false

        val identifier = tokens[0]
        val value = tokens[1]

        return when (identifier) {
            ChainedAlertExpressionConstant.ConstantType.ID.ident -> alertGeneratingMonitors.contains(value)
            else -> false
        }
    }
}

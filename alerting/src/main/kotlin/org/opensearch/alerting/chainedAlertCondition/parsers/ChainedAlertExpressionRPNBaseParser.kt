/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.chainedAlertCondition.parsers

import org.opensearch.alerting.chainedAlertCondition.tokens.CAExpressionOperator
import org.opensearch.alerting.chainedAlertCondition.tokens.CAExpressionToken
import org.opensearch.alerting.chainedAlertCondition.tokens.ChainedAlertExpressionConstant
import org.opensearch.alerting.chainedAlertCondition.tokens.ExpressionToken
import java.util.Stack

/**
 * This is the abstract base class which holds the trigger expression parsing logic;
 * using the Infix to Postfix a.k.a. Reverse Polish Notation (RPN) parser.
 * It also uses the Shunting-Yard algorithm to parse the given trigger expression.
 *
 * @param expressionToParse Complete string containing the trigger expression
 */
abstract class ChainedAlertExpressionRPNBaseParser(
    protected val expressionToParse: String,
) : ExpressionParser {
    /**
     * To perform the Infix-to-postfix conversion of the trigger expression
     */
    protected fun convertInfixToPostfix(expTokens: List<String>): ArrayList<ExpressionToken> {
        val expTokenStack = Stack<ExpressionToken>()
        val outputExpTokens = ArrayList<ExpressionToken>()

        for (tokenString in expTokens) {
            if (tokenString.isEmpty()) continue
            when (val expToken = assignToken(tokenString)) {
                is CAExpressionToken -> {
                    outputExpTokens.add(expToken)
                }

                is CAExpressionOperator -> {
                    when (expToken) {
                        CAExpressionOperator.PAR_LEFT -> {
                            expTokenStack.push(expToken)
                        }

                        CAExpressionOperator.PAR_RIGHT -> {
                            var topExpToken = expTokenStack.popExpTokenOrNull<CAExpressionOperator>()
                            while (topExpToken != null && topExpToken != CAExpressionOperator.PAR_LEFT) {
                                outputExpTokens.add(topExpToken)
                                topExpToken = expTokenStack.popExpTokenOrNull<CAExpressionOperator>()
                            }
                            if (topExpToken != CAExpressionOperator.PAR_LEFT) {
                                throw java.lang.IllegalArgumentException("No matching left parenthesis.")
                            }
                        }

                        else -> {
                            var op2 = expTokenStack.peekExpTokenOrNull<CAExpressionOperator>()
                            while (op2 != null) {
                                val c = expToken.precedence.compareTo(op2.precedence)
                                if (c < 0 || (!expToken.rightAssociative && c <= 0)) {
                                    outputExpTokens.add(expTokenStack.pop())
                                } else {
                                    break
                                }
                                op2 = expTokenStack.peekExpTokenOrNull<CAExpressionOperator>()
                            }
                            expTokenStack.push(expToken)
                        }
                    }
                }
            }
        }

        while (!expTokenStack.isEmpty()) {
            expTokenStack.peekExpTokenOrNull<CAExpressionOperator>()?.let {
                if (it == CAExpressionOperator.PAR_LEFT) {
                    throw java.lang.IllegalArgumentException("No matching right parenthesis.")
                }
            }
            val top = expTokenStack.pop()
            outputExpTokens.add(top)
        }

        return outputExpTokens
    }

    /**
     * Looks up and maps the expression token that matches the string version of that expression unit
     */
    private fun assignToken(tokenString: String): ExpressionToken {
        // Check "query" string in trigger expression such as in 'query[name="abc"]'
        if (tokenString.startsWith(ChainedAlertExpressionConstant.ConstantType.MONITOR.ident)) {
            return CAExpressionToken(tokenString)
        }

        // Check operators in trigger expression such as in [&&, ||, !]
        for (op in CAExpressionOperator.values()) {
            if (op.value == tokenString) return op
        }

        // Check any constants in trigger expression such as in ["name, "id", "tag", [", "]", "="]
        for (con in ChainedAlertExpressionConstant.ConstantType.values()) {
            if (tokenString == con.ident) return ChainedAlertExpressionConstant(con)
        }

        throw IllegalArgumentException("Error while processing the trigger expression '$tokenString'")
    }

    private inline fun <reified T> Stack<ExpressionToken>.popExpTokenOrNull(): T? =
        try {
            pop() as T
        } catch (e: java.lang.Exception) {
            null
        }

    private inline fun <reified T> Stack<ExpressionToken>.peekExpTokenOrNull(): T? =
        try {
            peek() as T
        } catch (e: java.lang.Exception) {
            null
        }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.triggercondition.parsers

import org.opensearch.alerting.triggercondition.tokens.ExpressionToken
import org.opensearch.alerting.triggercondition.tokens.TriggerExpressionConstant
import org.opensearch.alerting.triggercondition.tokens.TriggerExpressionOperator
import org.opensearch.alerting.triggercondition.tokens.TriggerExpressionToken
import java.util.Stack

/**
 * This is the abstract base class which holds the trigger expression parsing logic;
 * using the Infix to Postfix a.k.a. Reverse Polish Notation (RPN) parser.
 * It also uses the Shunting-Yard algorithm to parse the given trigger expression.
 *
 * @param expressionToParse Complete string containing the trigger expression
 */
abstract class TriggerExpressionRPNBaseParser(
    protected val expressionToParse: String
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
                is TriggerExpressionToken -> outputExpTokens.add(expToken)
                is TriggerExpressionOperator -> {
                    when (expToken) {
                        TriggerExpressionOperator.PAR_LEFT -> expTokenStack.push(expToken)
                        TriggerExpressionOperator.PAR_RIGHT -> {
                            var topExpToken = expTokenStack.popExpTokenOrNull<TriggerExpressionOperator>()
                            while (topExpToken != null && topExpToken != TriggerExpressionOperator.PAR_LEFT) {
                                outputExpTokens.add(topExpToken)
                                topExpToken = expTokenStack.popExpTokenOrNull<TriggerExpressionOperator>()
                            }
                            if (topExpToken != TriggerExpressionOperator.PAR_LEFT) {
                                throw java.lang.IllegalArgumentException("No matching left parenthesis.")
                            }
                        }
                        else -> {
                            var op2 = expTokenStack.peekExpTokenOrNull<TriggerExpressionOperator>()
                            while (op2 != null) {
                                val c = expToken.precedence.compareTo(op2.precedence)
                                if (c < 0 || !expToken.rightAssociative && c <= 0) {
                                    outputExpTokens.add(expTokenStack.pop())
                                } else {
                                    break
                                }
                                op2 = expTokenStack.peekExpTokenOrNull<TriggerExpressionOperator>()
                            }
                            expTokenStack.push(expToken)
                        }
                    }
                }
            }
        }

        while (!expTokenStack.isEmpty()) {
            expTokenStack.peekExpTokenOrNull<TriggerExpressionOperator>()?.let {
                if (it == TriggerExpressionOperator.PAR_LEFT) {
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
        if (tokenString.startsWith(TriggerExpressionConstant.ConstantType.QUERY.ident)) {
            return TriggerExpressionToken(tokenString)
        }

        // Check operators in trigger expression such as in [&&, ||, !]
        for (op in TriggerExpressionOperator.values()) {
            if (op.value == tokenString) return op
        }

        // Check any constants in trigger expression such as in ["name, "id", "tag", [", "]", "="]
        for (con in TriggerExpressionConstant.ConstantType.values()) {
            if (tokenString == con.ident) return TriggerExpressionConstant(con)
        }

        throw IllegalArgumentException("Error while processing the trigger expression '$tokenString'")
    }

    private inline fun <reified T> Stack<ExpressionToken>.popExpTokenOrNull(): T? {
        return try {
            pop() as T
        } catch (e: java.lang.Exception) {
            null
        }
    }

    private inline fun <reified T> Stack<ExpressionToken>.peekExpTokenOrNull(): T? {
        return try {
            peek() as T
        } catch (e: java.lang.Exception) {
            null
        }
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.triggercondition.parsers

import org.opensearch.alerting.triggercondition.resolvers.TriggerExpressionRPNResolver
import org.opensearch.alerting.triggercondition.tokens.TriggerExpressionOperator

/**
 * The postfix (Reverse Polish Notation) parser.
 * Uses the Shunting-yard algorithm to parse a mathematical expression
 * @param triggerExpression String containing the trigger expression for the monitor
 */
class TriggerExpressionParser(
    triggerExpression: String
) : TriggerExpressionRPNBaseParser(triggerExpression) {

    override fun parse(): TriggerExpressionRPNResolver {
        val expression = expressionToParse.replace(" ", "")

        val splitters = ArrayList<String>()
        TriggerExpressionOperator.values().forEach { splitters.add(it.value) }

        val breaks = ArrayList<String>().apply { add(expression) }
        for (s in splitters) {
            val a = ArrayList<String>()
            for (ind in 0 until breaks.size) {
                breaks[ind].let {
                    if (it.length > 1) {
                        a.addAll(breakString(breaks[ind], s))
                    } else a.add(it)
                }
            }
            breaks.clear()
            breaks.addAll(a)
        }

        return TriggerExpressionRPNResolver(convertInfixToPostfix(breaks))
    }

    private fun breakString(input: String, delimeter: String): ArrayList<String> {
        val tokens = input.split(delimeter)
        val array = ArrayList<String>()
        for (t in tokens) {
            array.add(t)
            array.add(delimeter)
        }
        array.removeAt(array.size - 1)
        return array
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.chainedAlertCondition

import org.junit.Assert
import org.opensearch.alerting.chainedAlertCondition.parsers.ChainedAlertExpressionParser
import org.opensearch.test.OpenSearchTestCase

class ChainedAlertsExpressionResolveTests : OpenSearchTestCase() {

    fun `test chained alert trigger expression evaluation simple AND`() {
        val eqString = "(monitor[id=123] && monitor[id=456])"
        val equation = ChainedAlertExpressionParser(eqString).parse()
        Assert.assertEquals("monitor[id=123] monitor[id=456] && ", equation.toString())
        val monitorHasAlertsMap: Map<String, Boolean> = mapOf(
            "123" to true,
            "456" to true
        )
        Assert.assertTrue(equation.evaluate(monitorHasAlertsMap))
        val monitorHasAlertsMap2: Map<String, Boolean> = mapOf(
            "123" to true,
            "456" to false
        )
        Assert.assertFalse(equation.evaluate(monitorHasAlertsMap2))
    }

    fun `test chained alert trigger expression evaluation AND with NOT`() {
        val eqString = "(monitor[id=123] && !monitor[id=456])"
        val equation = ChainedAlertExpressionParser(eqString).parse()
        Assert.assertEquals("monitor[id=123] monitor[id=456] ! && ", equation.toString())
        val monitorHasAlertsMap: Map<String, Boolean> = mapOf(
            "123" to true,
            "456" to false
        )
        Assert.assertTrue(equation.evaluate(monitorHasAlertsMap))
    }

    fun `test chained alert trigger expression evaluation simple OR`() {
        val eqString = "(monitor[id=123] || monitor[id=456])"
        val equation = ChainedAlertExpressionParser(eqString).parse()
        Assert.assertEquals("monitor[id=123] monitor[id=456] || ", equation.toString())
        val monitorHasAlertsMap: Map<String, Boolean> = mapOf(
            "123" to true,
            "456" to false
        )
        Assert.assertTrue(equation.evaluate(monitorHasAlertsMap))
        val monitorHasAlertsMap2: Map<String, Boolean> = mapOf(
            "123" to false,
            "456" to false
        )
        Assert.assertFalse(equation.evaluate(monitorHasAlertsMap2))
    }

    fun `test chained alert trigger expression evaluation OR with NOT`() {
        val eqString = "(monitor[id=123] || !monitor[id=456])"
        val equation = ChainedAlertExpressionParser(eqString).parse()
        Assert.assertEquals("monitor[id=123] monitor[id=456] ! || ", equation.toString())
        val monitorHasAlertsMap: Map<String, Boolean> = mapOf(
            "123" to false,
            "456" to false
        )
        Assert.assertTrue(equation.evaluate(monitorHasAlertsMap))
        val monitorHasAlertsMap2: Map<String, Boolean> = mapOf(
            "456" to true
        )
        Assert.assertFalse(equation.evaluate(monitorHasAlertsMap2))
    }

    fun `test chained alert trigger expression evaluation simple NOT`() {
        val eqString = "!(monitor[id=456])"
        val equation = ChainedAlertExpressionParser(eqString).parse()
        Assert.assertEquals("monitor[id=456] ! ", equation.toString())
        val monitorHasAlertsMap: Map<String, Boolean> = mapOf(
            "123" to true
        )
        Assert.assertTrue(equation.evaluate(monitorHasAlertsMap))
        val monitorHasAlertsMap2: Map<String, Boolean> = mapOf(
            "456" to true
        )
        Assert.assertFalse(equation.evaluate(monitorHasAlertsMap2))
    }

    fun `test chained alert trigger expression evaluation with multiple operators with parenthesis`() {
        val eqString = "(monitor[id=123] && monitor[id=456]) || !(!monitor[id=789] || monitor[id=abc])"
        val equation = ChainedAlertExpressionParser(eqString).parse()
        Assert.assertEquals(
            "monitor[id=123] monitor[id=456] && monitor[id=789] ! monitor[id=abc] || ! || ",
            equation.toString()
        )
        // part 1 evaluates to true, part 2 evaluates to false
        val monitorHasAlertsMap1: Map<String, Boolean> = mapOf(
            "123" to true,
            "456" to true,
            "789" to false,
            "abc" to false
        )
        Assert.assertTrue(equation.evaluate(monitorHasAlertsMap1))
        // part 1 evaluates to false, part 2 evaluates to false
        val monitorHasAlertsMap2: Map<String, Boolean> = mapOf(
            "123" to false,
            "456" to true,
            "789" to false,
            "abc" to false
        )
        Assert.assertFalse(equation.evaluate(monitorHasAlertsMap2))
        // part 1 evaluates to false, part 2 evaluates to true
        val monitorHasAlertsMap3: Map<String, Boolean> = mapOf(
            "123" to false,
            "456" to true,
            "789" to true,
            "abc" to false
        )
        Assert.assertTrue(equation.evaluate(monitorHasAlertsMap3))
    }
}

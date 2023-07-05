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
        val alertGeneratingMonitors: Set<String> = setOf(
            "123",
            "456"
        )
        Assert.assertTrue(equation.evaluate(alertGeneratingMonitors))
        val alertGeneratingMonitors2: Set<String> = setOf(
            "123",
            "456"
        )
        Assert.assertFalse(equation.evaluate(alertGeneratingMonitors2))
    }

    fun `test chained alert trigger expression evaluation AND with NOT`() {
        val eqString = "(monitor[id=123] && !monitor[id=456])"
        val equation = ChainedAlertExpressionParser(eqString).parse()
        Assert.assertEquals("monitor[id=123] monitor[id=456] ! && ", equation.toString())
        val alertGeneratingMonitors: Set<String> = setOf(
            "123",
            "456"
        )
        Assert.assertTrue(equation.evaluate(alertGeneratingMonitors))
    }

    fun `test chained alert trigger expression evaluation simple OR`() {
        val eqString = "(monitor[id=123] || monitor[id=456])"
        val equation = ChainedAlertExpressionParser(eqString).parse()
        Assert.assertEquals("monitor[id=123] monitor[id=456] || ", equation.toString())
        val alertGeneratingMonitors: Set<String> = setOf(
            "123",
            "456"
        )
        Assert.assertTrue(equation.evaluate(alertGeneratingMonitors))
        val alertGeneratingMonitors2: Set<String> = setOf(
            "123",
            "456"
        )
        Assert.assertFalse(equation.evaluate(alertGeneratingMonitors2))
    }

    fun `test chained alert trigger expression evaluation OR with NOT`() {
        val eqString = "(monitor[id=123] || !monitor[id=456])"
        val equation = ChainedAlertExpressionParser(eqString).parse()
        Assert.assertEquals("monitor[id=123] monitor[id=456] ! || ", equation.toString())
        val alertGeneratingMonitors: Set<String> = setOf(
            "123",
            "456"
        )
        Assert.assertTrue(equation.evaluate(alertGeneratingMonitors))
        val alertGeneratingMonitors2: Set<String> = setOf(
            "456"
        )
        Assert.assertFalse(equation.evaluate(alertGeneratingMonitors2))
    }

    fun `test chained alert trigger expression evaluation simple NOT`() {
        val eqString = "!(monitor[id=456])"
        val equation = ChainedAlertExpressionParser(eqString).parse()
        Assert.assertEquals("monitor[id=456] ! ", equation.toString())
        val alertGeneratingMonitors: Set<String> = setOf(
            "123"
        )
        Assert.assertTrue(equation.evaluate(alertGeneratingMonitors))
        val alertGeneratingMonitors2: Set<String> = setOf(
            "456"
        )
        Assert.assertFalse(equation.evaluate(alertGeneratingMonitors2))
    }

    fun `test chained alert trigger expression evaluation with multiple operators with parenthesis`() {
        val eqString = "(monitor[id=123] && monitor[id=456]) || !(!monitor[id=789] || monitor[id=abc])"
        val equation = ChainedAlertExpressionParser(eqString).parse()
        Assert.assertEquals(
            "monitor[id=123] monitor[id=456] && monitor[id=789] ! monitor[id=abc] || ! || ",
            equation.toString()
        )
        // part 1 evaluates, part 2 evaluates
        val alertGeneratingMonitors1: Set<String> = setOf(
            "123",
            "456",
            "789",
            "abc"
        )
        Assert.assertTrue(equation.evaluate(alertGeneratingMonitors1))
        // part 1 evaluates, part 2 evaluates
        val alertGeneratingMonitors2: Set<String> = setOf(
            "123",
            "456",
            "789",
            "abc"
        )
        Assert.assertFalse(equation.evaluate(alertGeneratingMonitors2))
        // part 1 evaluates, part 2 evaluates
        val alertGeneratingMonitors3: Set<String> = setOf(
            "123",
            "456",
            "789",
            "abc"
        )
        Assert.assertTrue(equation.evaluate(alertGeneratingMonitors3))
    }
}

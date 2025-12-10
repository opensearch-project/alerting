/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.chainedAlertCondition

import org.junit.Assert
import org.opensearch.alerting.chainedAlertCondition.parsers.ChainedAlertExpressionParser
import org.opensearch.test.OpenSearchTestCase
import kotlin.test.Test

class ChainedAlertsExpressionParserTests : OpenSearchTestCase() {
    @Test
    fun `test trigger expression posix parsing simple AND`() {
        val eqString = "(monitor[id=abc] && monitor[id=xyz])"
        val equation = ChainedAlertExpressionParser(eqString).parse()
        val expectedEquation = "monitor[id=abc] monitor[id=xyz] && "
        Assert.assertTrue(expectedEquation == equation.toString())
    }

    @Test
    fun `test trigger expression posix parsing simple AND without parentheses`() {
        val eqString = "monitor[id=abc] && monitor[id=xyz]"
        val equation = ChainedAlertExpressionParser(eqString).parse()
        val expectedEquation = "monitor[id=abc] monitor[id=xyz] && "
        Assert.assertTrue(expectedEquation == equation.toString())
    }

    @Test
    fun `test trigger expression posix parsing multiple AND`() {
        val eqString = "(monitor[id=abc] && monitor[id=def]) && monitor[id=ghi]"
        val equation = ChainedAlertExpressionParser(eqString).parse()
        Assert.assertEquals("monitor[id=abc] monitor[id=def] && monitor[id=ghi] && ", equation.toString())
    }

    @Test
    fun `test trigger expression posix parsing multiple AND with parenthesis`() {
        val eqString = "(monitor[id=sigma-123] && monitor[id=sigma-456]) && (monitor[id=sigma-789] && monitor[id=id-2aw34])"
        val equation = ChainedAlertExpressionParser(eqString).parse()
        Assert.assertEquals(
            "monitor[id=sigma-123] monitor[id=sigma-456] && monitor[id=sigma-789] monitor[id=id-2aw34] && && ",
            equation.toString(),
        )
    }

    @Test
    fun `test trigger expression posix parsing simple OR`() {
        val eqString = "(monitor[id=sigma-123] || monitor[id=sigma-456])"
        val equation = ChainedAlertExpressionParser(eqString).parse()
        Assert.assertEquals("monitor[id=sigma-123] monitor[id=sigma-456] || ", equation.toString())
    }

    @Test
    fun `test trigger expression posix parsing multiple OR`() {
        val eqString = "(monitor[id=sigma-123] || monitor[id=sigma-456]) || monitor[id=sigma-789]"
        val equation = ChainedAlertExpressionParser(eqString).parse()
        Assert.assertEquals("monitor[id=sigma-123] monitor[id=sigma-456] || monitor[id=sigma-789] || ", equation.toString())
    }

    @Test
    fun `test trigger expression posix parsing multiple OR with parenthesis`() {
        val eqString = "(monitor[id=sigma-123] || monitor[id=sigma-456]) || (monitor[id=sigma-789] || monitor[id=id-2aw34])"
        val equation = ChainedAlertExpressionParser(eqString).parse()
        Assert.assertEquals(
            "monitor[id=sigma-123] monitor[id=sigma-456] || monitor[id=sigma-789] monitor[id=id-2aw34] || || ",
            equation.toString(),
        )
    }

    @Test
    fun `test trigger expression posix parsing simple NOT`() {
        val eqString = "(monitor[id=sigma-123] || !monitor[id=sigma-456])"
        val equation = ChainedAlertExpressionParser(eqString).parse()
        Assert.assertEquals("monitor[id=sigma-123] monitor[id=sigma-456] ! || ", equation.toString())
    }

    @Test
    fun `test trigger expression posix parsing multiple NOT`() {
        val eqString = "(monitor[id=sigma-123] && !monitor[tag=tag-456]) && !(monitor[id=sigma-789])"
        val equation = ChainedAlertExpressionParser(eqString).parse()
        Assert.assertEquals("monitor[id=sigma-123] monitor[tag=tag-456] ! && monitor[id=sigma-789] ! && ", equation.toString())
    }

    @Test
    fun `test trigger expression posix parsing multiple operators with parenthesis`() {
        val eqString = "(monitor[id=sigma-123] && monitor[tag=sev1]) || !(!monitor[id=sigma-789] || monitor[id=id-2aw34])"
        val equation = ChainedAlertExpressionParser(eqString).parse()
        Assert.assertEquals(
            "monitor[id=sigma-123] monitor[tag=sev1] && monitor[id=sigma-789] ! monitor[id=id-2aw34] || ! || ",
            equation.toString(),
        )
    }
}

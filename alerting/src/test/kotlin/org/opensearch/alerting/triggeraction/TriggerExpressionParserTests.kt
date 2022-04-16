package org.opensearch.alerting.triggeraction

import org.junit.Assert
import org.opensearch.alerting.triggercondition.parsers.TriggerExpressionParser
import org.opensearch.test.OpenSearchTestCase

class TriggerExpressionParserTests : OpenSearchTestCase() {

    fun `test trigger expression posix parsing simple AND`() {
        val eqString = "(query[name=sigma-123] && query[name=sigma-456])"
        val equation = TriggerExpressionParser(eqString).parse()
        Assert.assertEquals("query[name=sigma-123] query[name=sigma-456] && ", equation.toString())
    }

    fun `test trigger expression posix parsing multiple AND`() {
        val eqString = "(query[name=sigma-123] && query[name=sigma-456]) && query[name=sigma-789]"
        val equation = TriggerExpressionParser(eqString).parse()
        Assert.assertEquals("query[name=sigma-123] query[name=sigma-456] && query[name=sigma-789] && ", equation.toString())
    }

    fun `test trigger expression posix parsing multiple AND with parenthesis`() {
        val eqString = "(query[name=sigma-123] && query[name=sigma-456]) && (query[name=sigma-789] && query[name=id-2aw34])"
        val equation = TriggerExpressionParser(eqString).parse()
        Assert.assertEquals(
            "query[name=sigma-123] query[name=sigma-456] && query[name=sigma-789] query[name=id-2aw34] && && ",
            equation.toString()
        )
    }

    fun `test trigger expression posix parsing simple OR`() {
        val eqString = "(query[name=sigma-123] || query[name=sigma-456])"
        val equation = TriggerExpressionParser(eqString).parse()
        Assert.assertEquals("query[name=sigma-123] query[name=sigma-456] || ", equation.toString())
    }

    fun `test trigger expression posix parsing multiple OR`() {
        val eqString = "(query[name=sigma-123] || query[name=sigma-456]) || query[name=sigma-789]"
        val equation = TriggerExpressionParser(eqString).parse()
        Assert.assertEquals("query[name=sigma-123] query[name=sigma-456] || query[name=sigma-789] || ", equation.toString())
    }

    fun `test trigger expression posix parsing multiple OR with parenthesis`() {
        val eqString = "(query[name=sigma-123] || query[name=sigma-456]) || (query[name=sigma-789] || query[name=id-2aw34])"
        val equation = TriggerExpressionParser(eqString).parse()
        Assert.assertEquals(
            "query[name=sigma-123] query[name=sigma-456] || query[name=sigma-789] query[name=id-2aw34] || || ",
            equation.toString()
        )
    }

    fun `test trigger expression posix parsing simple NOT`() {
        val eqString = "(query[name=sigma-123] || !query[name=sigma-456])"
        val equation = TriggerExpressionParser(eqString).parse()
        Assert.assertEquals("query[name=sigma-123] query[name=sigma-456] ! || ", equation.toString())
    }

    fun `test trigger expression posix parsing multiple NOT`() {
        val eqString = "(query[name=sigma-123] && !query[tag=tag-456]) && !(query[name=sigma-789])"
        val equation = TriggerExpressionParser(eqString).parse()
        Assert.assertEquals("query[name=sigma-123] query[tag=tag-456] ! && query[name=sigma-789] ! && ", equation.toString())
    }

    fun `test trigger expression posix parsing multiple operators with parenthesis`() {
        val eqString = "(query[name=sigma-123] && query[tag=sev1]) || !(!query[name=sigma-789] || query[name=id-2aw34])"
        val equation = TriggerExpressionParser(eqString).parse()
        Assert.assertEquals(
            "query[name=sigma-123] query[tag=sev1] && query[name=sigma-789] ! query[name=id-2aw34] || ! || ",
            equation.toString()
        )
    }
}

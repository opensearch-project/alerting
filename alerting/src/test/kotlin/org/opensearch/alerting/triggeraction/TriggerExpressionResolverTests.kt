/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.triggeraction

import org.junit.Assert
import org.opensearch.alerting.triggercondition.parsers.TriggerExpressionParser
import org.opensearch.commons.alerting.model.DocLevelQuery
import org.opensearch.test.OpenSearchTestCase
import kotlin.test.Test

class TriggerExpressionResolverTests : OpenSearchTestCase() {
    @Test
    fun `test trigger expression evaluation simple AND`() {
        val eqString = "(query[name=sigma-123] && query[name=sigma-456])"
        val equation = TriggerExpressionParser(eqString).parse()
        val queryToDocIds = mutableMapOf<DocLevelQuery, Set<String>>()
        queryToDocIds[DocLevelQuery("", "sigma-123", listOf(), "", emptyList())] = mutableSetOf("1", "2", "3")
        queryToDocIds[DocLevelQuery("", "sigma-456", listOf(), "", emptyList())] = mutableSetOf("1", "2", "3")
        Assert.assertEquals("query[name=sigma-123] query[name=sigma-456] && ", equation.toString())
        Assert.assertEquals(mutableSetOf("1", "2", "3"), equation.evaluate(queryToDocIds))
    }

    @Test
    fun `test trigger expression evaluation simple AND scenario2`() {
        val eqString = "(query[name=sigma-123] && query[id=id1456])"
        val equation = TriggerExpressionParser(eqString).parse()
        val queryToDocIds = mutableMapOf<DocLevelQuery, Set<String>>()
        queryToDocIds[DocLevelQuery("", "sigma-123", listOf(), "", emptyList())] = mutableSetOf("6", "3", "7")
        queryToDocIds[DocLevelQuery("id1456", "sigma-456", listOf(), "", emptyList())] = mutableSetOf("1", "2", "3")
        Assert.assertEquals("query[name=sigma-123] query[id=id1456] && ", equation.toString())
        Assert.assertEquals(mutableSetOf("3"), equation.evaluate(queryToDocIds))
    }

    @Test
    fun `test trigger expression evaluation simple AND scenario3`() {
        val eqString = "(query[name=sigma-123] && query[tag=sev2])"
        val equation = TriggerExpressionParser(eqString).parse()
        val queryToDocIds = mutableMapOf<DocLevelQuery, Set<String>>()
        queryToDocIds[DocLevelQuery("", "sigma-123", listOf(), "", emptyList())] = mutableSetOf("6", "8", "7")
        queryToDocIds[DocLevelQuery("", "sigma-456", listOf(), "", mutableListOf("tag=sev2"))] = mutableSetOf("1", "2", "3")
        Assert.assertEquals("query[name=sigma-123] query[tag=sev2] && ", equation.toString())
        Assert.assertEquals(emptySet<String>(), equation.evaluate(queryToDocIds))
    }

    @Test
    fun `test trigger expression evaluation simple OR`() {
        val eqString = "(query[name=sigma-123] || query[name=sigma-456])"
        val equation = TriggerExpressionParser(eqString).parse()
        val queryToDocIds = mutableMapOf<DocLevelQuery, Set<String>>()
        queryToDocIds[DocLevelQuery("", "sigma-123", listOf(), "", emptyList())] = mutableSetOf("1", "2", "3")
        queryToDocIds[DocLevelQuery("", "sigma-456", listOf(), "", emptyList())] = mutableSetOf("1", "2", "3")
        Assert.assertEquals("query[name=sigma-123] query[name=sigma-456] || ", equation.toString())
        Assert.assertEquals(mutableSetOf("1", "2", "3"), equation.evaluate(queryToDocIds))
    }

    @Test
    fun `test trigger expression evaluation simple OR scenario2`() {
        val eqString = "(query[name=sigma-123] || query[id=id1456])"
        val equation = TriggerExpressionParser(eqString).parse()
        val queryToDocIds = mutableMapOf<DocLevelQuery, Set<String>>()
        queryToDocIds[DocLevelQuery("", "sigma-123", listOf(), "", emptyList())] = mutableSetOf("6", "3", "7")
        queryToDocIds[DocLevelQuery("id1456", "sigma-456", listOf(), "", emptyList())] = mutableSetOf("1", "2", "3")
        Assert.assertEquals("query[name=sigma-123] query[id=id1456] || ", equation.toString())
        Assert.assertEquals(mutableSetOf("6", "3", "7", "1", "2", "3"), equation.evaluate(queryToDocIds))
    }

    @Test
    fun `test trigger expression evaluation simple OR scenario3`() {
        val eqString = "(query[name=sigma-123] || query[tag=sev2])"
        val equation = TriggerExpressionParser(eqString).parse()
        val queryToDocIds = mutableMapOf<DocLevelQuery, Set<String>>()
        queryToDocIds[DocLevelQuery("", "sigma-123", listOf(), "", emptyList())] = mutableSetOf("6", "8", "7")
        queryToDocIds[DocLevelQuery("", "sigma-456", listOf(), "", mutableListOf("tag=sev2"))] = emptySet()
        Assert.assertEquals("query[name=sigma-123] query[tag=sev2] || ", equation.toString())
        Assert.assertEquals(mutableSetOf("6", "8", "7"), equation.evaluate(queryToDocIds))
    }

    @Test
    fun `test trigger expression evaluation simple NOT`() {
        val eqString = "!(query[name=sigma-456])"
        val equation = TriggerExpressionParser(eqString).parse()
        val queryToDocIds = mutableMapOf<DocLevelQuery, Set<String>>()
        queryToDocIds[DocLevelQuery("", "sigma-123", listOf(), "", emptyList())] = mutableSetOf("1", "2", "3")
        queryToDocIds[DocLevelQuery("", "sigma-456", listOf(), "", emptyList())] = mutableSetOf("4", "5", "6")
        Assert.assertEquals("query[name=sigma-456] ! ", equation.toString())
        Assert.assertEquals(mutableSetOf("1", "2", "3"), equation.evaluate(queryToDocIds))
    }

    @Test
    fun `test trigger expression evaluation AND with NOT`() {
        val eqString = "(query[name=sigma-123] && !query[name=sigma-456])"
        val equation = TriggerExpressionParser(eqString).parse()
        val queryToDocIds = mutableMapOf<DocLevelQuery, Set<String>>()
        queryToDocIds[DocLevelQuery("", "sigma-123", listOf(), "", emptyList())] = mutableSetOf("1", "2", "3", "11")
        queryToDocIds[DocLevelQuery("", "sigma-456", listOf(), "", emptyList())] = mutableSetOf("3", "4", "5")
        queryToDocIds[DocLevelQuery("id_new", "sigma-789", listOf(), "", emptyList())] = mutableSetOf("11", "12", "13")
        Assert.assertEquals("query[name=sigma-123] query[name=sigma-456] ! && ", equation.toString())
        Assert.assertEquals(mutableSetOf("1", "2", "11"), equation.evaluate(queryToDocIds))
    }

    @Test
    fun `test trigger expression evaluation OR with NOT`() {
        val eqString = "(query[name=sigma-123] || !query[id=id1456])"
        val equation = TriggerExpressionParser(eqString).parse()
        val queryToDocIds = mutableMapOf<DocLevelQuery, Set<String>>()
        queryToDocIds[DocLevelQuery("", "sigma-123", listOf(), "", emptyList())] = mutableSetOf("6", "3", "7")
        queryToDocIds[DocLevelQuery("id1456", "sigma-456", listOf(), "", emptyList())] = mutableSetOf("11", "12", "15")
        queryToDocIds[DocLevelQuery("id_new", "sigma-789", listOf(), "", emptyList())] = mutableSetOf("11", "12", "13")
        Assert.assertEquals("query[name=sigma-123] query[id=id1456] ! || ", equation.toString())
        Assert.assertEquals(mutableSetOf("6", "3", "7", "13"), equation.evaluate(queryToDocIds))
    }

    @Test
    fun `test trigger expression evaluation with multiple operators with parenthesis`() {
        val eqString = "(query[name=sigma-123] && query[tag=sev1]) || !(!query[name=sigma-789] || query[id=id-2aw34])"
        val equation = TriggerExpressionParser(eqString).parse()

        val queryToDocIds = mutableMapOf<DocLevelQuery, Set<String>>()
        queryToDocIds[DocLevelQuery("", "sigma-123", listOf(), "", emptyList())] = mutableSetOf("1", "2", "3")
        queryToDocIds[DocLevelQuery("id_random1", "sigma-456", listOf(), "", mutableListOf("sev1"))] = mutableSetOf("2", "3", "4")
        queryToDocIds[DocLevelQuery("", "sigma-789", listOf(), "", emptyList())] = mutableSetOf("11", "12", "13")
        queryToDocIds[DocLevelQuery("id-2aw34", "sigma-101112", listOf(), "", emptyList())] = mutableSetOf("13", "14", "15")

        Assert.assertEquals(
            "query[name=sigma-123] query[tag=sev1] && query[name=sigma-789] ! query[id=id-2aw34] || ! || ",
            equation.toString(),
        )

        Assert.assertEquals(mutableSetOf("2", "3", "11", "12"), equation.evaluate(queryToDocIds))
    }
}

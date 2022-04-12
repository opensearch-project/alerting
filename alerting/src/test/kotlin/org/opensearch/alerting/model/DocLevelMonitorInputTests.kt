/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model

import org.opensearch.alerting.core.model.DocLevelMonitorInput
import org.opensearch.alerting.core.model.DocLevelQuery
import org.opensearch.alerting.elasticapi.string
import org.opensearch.alerting.randomDocLevelMonitorInput
import org.opensearch.alerting.randomDocLevelQuery
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentType
import org.opensearch.test.OpenSearchTestCase

class DocLevelMonitorInputTests : OpenSearchTestCase() {
    fun `testing DocLevelQuery asTemplateArgs`() {
        // GIVEN
        val query = randomDocLevelQuery()

        // WHEN
        val templateArgs = query.asTemplateArg()

        // THEN
        assertEquals("Template args 'id' field does not match:", templateArgs[DocLevelQuery.QUERY_ID_FIELD], query.id)
        assertEquals("Template args 'query' field does not match:", templateArgs[DocLevelQuery.QUERY_FIELD], query.query)
        assertEquals("Template args 'name' field does not match:", templateArgs[DocLevelQuery.NAME_FIELD], query.name)
        assertEquals("Template args 'tags' field does not match:", templateArgs[DocLevelQuery.TAGS_FIELD], query.tags)
    }

    fun `testing DocLevelMonitorInput asTemplateArgs`() {
        // GIVEN
        val input = randomDocLevelMonitorInput()

        // test
        val inputString = input.toXContent(XContentBuilder.builder(XContentType.JSON.xContent()), ToXContent.EMPTY_PARAMS).string()
        // assertEquals("test", inputString)
        // test end
        // WHEN
        val templateArgs = input.asTemplateArg()

        // THEN
        assertEquals(
            "Template args 'description' field does not match:",
            templateArgs[DocLevelMonitorInput.DESCRIPTION_FIELD],
            input.description
        )
        assertEquals(
            "Template args 'indices' field does not match:",
            templateArgs[DocLevelMonitorInput.INDICES_FIELD],
            input.indices
        )
        assertEquals(
            "Template args 'queries' field does not contain the expected number of queries:",
            input.queries.size,
            (templateArgs[DocLevelMonitorInput.QUERIES_FIELD] as List<*>).size
        )
        input.queries.forEach {
            assertTrue(
                "Template args 'queries' field does not match:",
                (templateArgs[DocLevelMonitorInput.QUERIES_FIELD] as List<*>).contains(it.asTemplateArg())
            )
        }
    }
}

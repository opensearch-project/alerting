/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model

import org.opensearch.alerting.model.action.Action
import org.opensearch.alerting.model.docLevelInput.DocLevelMonitorInput
import org.opensearch.alerting.model.docLevelInput.DocLevelQuery
import org.opensearch.alerting.randomDocLevelMonitorInput
import org.opensearch.alerting.randomDocLevelQuery
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
        assertEquals("Template args 'severity' field does not match:", templateArgs[DocLevelQuery.SEVERITY_FIELD], query.severity)
        assertEquals("Template args 'tags' field does not match:", templateArgs[DocLevelQuery.TAGS_FIELD], query.tags)

        val expectedActions = query.actions.map { mapOf(Action.NAME_FIELD to it.name) }
        assertEquals("Template args 'actions' field does not match:", templateArgs[DocLevelQuery.ACTIONS_FIELD], expectedActions)
    }

    fun `testing DocLevelMonitorInput asTemplateArgs`() {
        // GIVEN
        val input = randomDocLevelMonitorInput()

        // WHEN
        val templateArgs = input.asTemplateArg()

        // THEN
        assertEquals("Template args 'description' field does not match:", templateArgs[DocLevelMonitorInput.DESCRIPTION_FIELD], input.description)
        assertEquals("Template args 'indices' field does not match:", templateArgs[DocLevelMonitorInput.INDICES_FIELD], input.indices)
        assertEquals("Template args 'queries' field does not contain the expected number of queries:", input.queries.size, (templateArgs[DocLevelMonitorInput.QUERIES_FIELD] as List<*>).size)
        input.queries.forEach {
            assertTrue("Template args 'queries' field does not match:", (templateArgs[DocLevelMonitorInput.QUERIES_FIELD] as List<*>).contains(it.asTemplateArg()))
        }
    }
}

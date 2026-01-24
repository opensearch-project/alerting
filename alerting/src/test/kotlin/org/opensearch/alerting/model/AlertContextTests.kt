/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model

import org.opensearch.alerting.randomAlertContext
import org.opensearch.alerting.randomDocLevelQuery
import org.opensearch.alerting.randomFinding
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.commons.alerting.model.DocLevelQuery
import org.opensearch.test.OpenSearchTestCase
import kotlin.test.Test

@Suppress("UNCHECKED_CAST")
class AlertContextTests : OpenSearchTestCase() {
    @Test
    fun `test AlertContext asTemplateArg with null associatedQueries and null sampleDocs`() {
        val associatedQueries: List<DocLevelQuery>? = null
        val sampleDocs: List<Map<String, Any?>>? = null
        val alertContext: AlertContext =
            randomAlertContext(
                associatedQueries = associatedQueries,
                sampleDocs = sampleDocs,
            )

        val templateArgs = alertContext.asTemplateArg()

        assertAlertIsEqual(alertContext = alertContext, templateArgs = templateArgs)
        assertNull("Template associated queries should be null", templateArgs[AlertContext.ASSOCIATED_QUERIES_FIELD])
        assertNull("Template sample docs should be null", templateArgs[AlertContext.SAMPLE_DOCS_FIELD])
    }

    @Test
    fun `test AlertContext asTemplateArg with null associatedQueries and 0 sampleDocs`() {
        val associatedQueries: List<DocLevelQuery>? = null
        val sampleDocs: List<Map<String, Any?>> = listOf()
        val alertContext: AlertContext =
            randomAlertContext(
                associatedQueries = associatedQueries,
                sampleDocs = sampleDocs,
            )

        val templateArgs = alertContext.asTemplateArg()

        assertAlertIsEqual(alertContext = alertContext, templateArgs = templateArgs)
        assertNull("Template associated queries should be null", templateArgs[AlertContext.ASSOCIATED_QUERIES_FIELD])
        assertEquals(
            "Template args sample docs should have size ${sampleDocs!!.size}",
            sampleDocs!!.size,
            (templateArgs[AlertContext.SAMPLE_DOCS_FIELD] as List<Map<String, Any?>>).size,
        )
        assertEquals("Template args sample docs do not match", alertContext.sampleDocs, templateArgs[AlertContext.SAMPLE_DOCS_FIELD])
    }

    @Test
    fun `test AlertContext asTemplateArg with null associatedQueries and 1 sampleDocs`() {
        val associatedQueries: List<DocLevelQuery>? = null
        val sampleDocs: List<Map<String, Any?>> = listOf(randomFinding().asTemplateArg())
        val alertContext: AlertContext =
            randomAlertContext(
                associatedQueries = associatedQueries,
                sampleDocs = sampleDocs,
            )

        val templateArgs = alertContext.asTemplateArg()

        assertAlertIsEqual(alertContext = alertContext, templateArgs = templateArgs)
        assertNull("Template associated queries should be null", templateArgs[AlertContext.ASSOCIATED_QUERIES_FIELD])
        assertEquals(
            "Template args sample docs should have size ${sampleDocs.size}",
            sampleDocs.size,
            (templateArgs[AlertContext.SAMPLE_DOCS_FIELD] as List<Map<String, Any?>>).size,
        )
        assertEquals("Template args sample docs do not match", alertContext.sampleDocs, templateArgs[AlertContext.SAMPLE_DOCS_FIELD])
    }

    @Test
    fun `test AlertContext asTemplateArg with null associatedQueries and multiple sampleDocs`() {
        val associatedQueries: List<DocLevelQuery>? = null
        val sampleDocs: List<Map<String, Any?>> = (0..2).map { randomFinding().asTemplateArg() }
        val alertContext: AlertContext =
            randomAlertContext(
                associatedQueries = associatedQueries,
                sampleDocs = sampleDocs,
            )

        val templateArgs = alertContext.asTemplateArg()

        assertAlertIsEqual(alertContext = alertContext, templateArgs = templateArgs)
        assertNull("Template associated queries should be null", templateArgs[AlertContext.ASSOCIATED_QUERIES_FIELD])
        assertEquals(
            "Template args sample docs should have size ${sampleDocs.size}",
            sampleDocs.size,
            (templateArgs[AlertContext.SAMPLE_DOCS_FIELD] as List<Map<String, Any?>>).size,
        )
        assertEquals("Template args sample docs do not match", alertContext.sampleDocs, templateArgs[AlertContext.SAMPLE_DOCS_FIELD])
    }

    @Test
    fun `test AlertContext asTemplateArg with 0 associatedQueries and null sampleDocs`() {
        val associatedQueries: List<DocLevelQuery> = listOf()
        val sampleDocs: List<Map<String, Any?>>? = null
        val alertContext: AlertContext =
            randomAlertContext(
                associatedQueries = associatedQueries,
                sampleDocs = sampleDocs,
            )

        val templateArgs = alertContext.asTemplateArg()

        assertAlertIsEqual(alertContext = alertContext, templateArgs = templateArgs)
        assertEquals(
            "Template args associated queries should have size ${associatedQueries.size}",
            associatedQueries.size,
            (templateArgs[AlertContext.ASSOCIATED_QUERIES_FIELD] as List<DocLevelQuery>).size,
        )
        assertEquals(
            "Template associated queries do not match",
            formatAssociatedQueries(alertContext),
            templateArgs[AlertContext.ASSOCIATED_QUERIES_FIELD],
        )
        assertNull("Template sample docs should be null", templateArgs[AlertContext.SAMPLE_DOCS_FIELD])
    }

    @Test
    fun `test AlertContext asTemplateArg with 1 associatedQueries and null sampleDocs`() {
        val associatedQueries: List<DocLevelQuery> = listOf(randomDocLevelQuery())
        val sampleDocs: List<Map<String, Any?>>? = null
        val alertContext: AlertContext =
            randomAlertContext(
                associatedQueries = associatedQueries,
                sampleDocs = sampleDocs,
            )

        val templateArgs = alertContext.asTemplateArg()

        assertAlertIsEqual(alertContext = alertContext, templateArgs = templateArgs)
        assertEquals(
            "Template args associated queries should have size ${associatedQueries.size}",
            associatedQueries.size,
            (templateArgs[AlertContext.ASSOCIATED_QUERIES_FIELD] as List<DocLevelQuery>).size,
        )
        assertEquals(
            "Template associated queries do not match",
            formatAssociatedQueries(alertContext),
            templateArgs[AlertContext.ASSOCIATED_QUERIES_FIELD],
        )
        assertNull("Template sample docs should be null", templateArgs[AlertContext.SAMPLE_DOCS_FIELD])
    }

    @Test
    fun `test AlertContext asTemplateArg with multiple associatedQueries and null sampleDocs`() {
        val associatedQueries: List<DocLevelQuery> = (0..2).map { randomDocLevelQuery() }
        val sampleDocs: List<Map<String, Any?>>? = null
        val alertContext: AlertContext =
            randomAlertContext(
                associatedQueries = associatedQueries,
                sampleDocs = sampleDocs,
            )

        val templateArgs = alertContext.asTemplateArg()

        assertAlertIsEqual(alertContext = alertContext, templateArgs = templateArgs)
        assertEquals(
            "Template args associated queries should have size ${associatedQueries.size}",
            associatedQueries.size,
            (templateArgs[AlertContext.ASSOCIATED_QUERIES_FIELD] as List<DocLevelQuery>).size,
        )
        assertEquals(
            "Template associated queries do not match",
            formatAssociatedQueries(alertContext),
            templateArgs[AlertContext.ASSOCIATED_QUERIES_FIELD],
        )
        assertNull("Template sample docs should be null", templateArgs[AlertContext.SAMPLE_DOCS_FIELD])
    }

    @Test
    fun `test AlertContext asTemplateArg with 0 associatedQueries and 0 sampleDocs`() {
        val associatedQueries: List<DocLevelQuery> = listOf()
        val sampleDocs: List<Map<String, Any?>> = listOf()
        val alertContext: AlertContext =
            randomAlertContext(
                associatedQueries = associatedQueries,
                sampleDocs = sampleDocs,
            )

        val templateArgs = alertContext.asTemplateArg()

        assertAlertIsEqual(alertContext = alertContext, templateArgs = templateArgs)
        assertEquals(
            "Template args associated queries should have size ${associatedQueries.size}",
            associatedQueries.size,
            (templateArgs[AlertContext.ASSOCIATED_QUERIES_FIELD] as List<DocLevelQuery>).size,
        )
        assertEquals(
            "Template associated queries do not match",
            formatAssociatedQueries(alertContext),
            templateArgs[AlertContext.ASSOCIATED_QUERIES_FIELD],
        )

        assertEquals(
            "Template args sample docs should have size ${sampleDocs.size}",
            sampleDocs.size,
            (templateArgs[AlertContext.SAMPLE_DOCS_FIELD] as List<Map<String, Any?>>).size,
        )
        assertEquals("Template args sample docs do not match", alertContext.sampleDocs, templateArgs[AlertContext.SAMPLE_DOCS_FIELD])
    }

    @Test
    fun `test AlertContext asTemplateArg with 0 associatedQueries and 1 sampleDocs`() {
        val associatedQueries: List<DocLevelQuery> = listOf()
        val sampleDocs: List<Map<String, Any?>> = listOf(randomFinding().asTemplateArg())
        val alertContext: AlertContext =
            randomAlertContext(
                associatedQueries = associatedQueries,
                sampleDocs = sampleDocs,
            )

        val templateArgs = alertContext.asTemplateArg()

        assertAlertIsEqual(alertContext = alertContext, templateArgs = templateArgs)
        assertEquals(
            "Template args associated queries should have size ${associatedQueries.size}",
            associatedQueries.size,
            (templateArgs[AlertContext.ASSOCIATED_QUERIES_FIELD] as List<DocLevelQuery>).size,
        )
        assertEquals(
            "Template associated queries do not match",
            formatAssociatedQueries(alertContext),
            templateArgs[AlertContext.ASSOCIATED_QUERIES_FIELD],
        )

        assertEquals(
            "Template args sample docs should have size ${sampleDocs.size}",
            sampleDocs.size,
            (templateArgs[AlertContext.SAMPLE_DOCS_FIELD] as List<Map<String, Any?>>).size,
        )
        assertEquals("Template args sample docs do not match", alertContext.sampleDocs, templateArgs[AlertContext.SAMPLE_DOCS_FIELD])
    }

    @Test
    fun `test AlertContext asTemplateArg with 0 associatedQueries and multiple sampleDocs`() {
        val associatedQueries: List<DocLevelQuery> = listOf()
        val sampleDocs: List<Map<String, Any?>> = (0..2).map { randomFinding().asTemplateArg() }
        val alertContext: AlertContext =
            randomAlertContext(
                associatedQueries = associatedQueries,
                sampleDocs = sampleDocs,
            )

        val templateArgs = alertContext.asTemplateArg()

        assertAlertIsEqual(alertContext = alertContext, templateArgs = templateArgs)
        assertEquals(
            "Template args associated queries should have size ${associatedQueries.size}",
            associatedQueries.size,
            (templateArgs[AlertContext.ASSOCIATED_QUERIES_FIELD] as List<DocLevelQuery>).size,
        )
        assertEquals(
            "Template associated queries do not match",
            formatAssociatedQueries(alertContext),
            templateArgs[AlertContext.ASSOCIATED_QUERIES_FIELD],
        )

        assertEquals(
            "Template args sample docs should have size ${sampleDocs.size}",
            sampleDocs.size,
            (templateArgs[AlertContext.SAMPLE_DOCS_FIELD] as List<Map<String, Any?>>).size,
        )
        assertEquals("Template args sample docs do not match", alertContext.sampleDocs, templateArgs[AlertContext.SAMPLE_DOCS_FIELD])
    }

    @Test
    fun `test AlertContext asTemplateArg with 1 associatedQueries and 0 sampleDocs`() {
        val associatedQueries: List<DocLevelQuery> = listOf(randomDocLevelQuery())
        val sampleDocs: List<Map<String, Any?>> = listOf()
        val alertContext: AlertContext =
            randomAlertContext(
                associatedQueries = associatedQueries,
                sampleDocs = sampleDocs,
            )

        val templateArgs = alertContext.asTemplateArg()

        assertAlertIsEqual(alertContext = alertContext, templateArgs = templateArgs)
        assertEquals(
            "Template args associated queries should have size ${associatedQueries.size}",
            associatedQueries.size,
            (templateArgs[AlertContext.ASSOCIATED_QUERIES_FIELD] as List<DocLevelQuery>).size,
        )
        assertEquals(
            "Template associated queries do not match",
            formatAssociatedQueries(alertContext),
            templateArgs[AlertContext.ASSOCIATED_QUERIES_FIELD],
        )

        assertEquals(
            "Template args sample docs should have size ${sampleDocs.size}",
            sampleDocs.size,
            (templateArgs[AlertContext.SAMPLE_DOCS_FIELD] as List<Map<String, Any?>>).size,
        )
        assertEquals("Template args sample docs do not match", alertContext.sampleDocs, templateArgs[AlertContext.SAMPLE_DOCS_FIELD])
    }

    @Test
    fun `test AlertContext asTemplateArg with multiple associatedQueries and 0 sampleDocs`() {
        val associatedQueries: List<DocLevelQuery> = (0..2).map { randomDocLevelQuery() }
        val sampleDocs: List<Map<String, Any?>> = listOf()
        val alertContext: AlertContext =
            randomAlertContext(
                associatedQueries = associatedQueries,
                sampleDocs = sampleDocs,
            )

        val templateArgs = alertContext.asTemplateArg()

        assertAlertIsEqual(alertContext = alertContext, templateArgs = templateArgs)
        assertEquals(
            "Template args associated queries should have size ${associatedQueries.size}",
            associatedQueries.size,
            (templateArgs[AlertContext.ASSOCIATED_QUERIES_FIELD] as List<DocLevelQuery>).size,
        )
        assertEquals(
            "Template associated queries do not match",
            formatAssociatedQueries(alertContext),
            templateArgs[AlertContext.ASSOCIATED_QUERIES_FIELD],
        )

        assertEquals(
            "Template args sample docs should have size ${sampleDocs.size}",
            sampleDocs.size,
            (templateArgs[AlertContext.SAMPLE_DOCS_FIELD] as List<Map<String, Any?>>).size,
        )
        assertEquals("Template args sample docs do not match", alertContext.sampleDocs, templateArgs[AlertContext.SAMPLE_DOCS_FIELD])
    }

    @Test
    fun `test AlertContext asTemplateArg with 1 associatedQueries and 1 sampleDocs`() {
        val associatedQueries: List<DocLevelQuery> = listOf(randomDocLevelQuery())
        val sampleDocs: List<Map<String, Any?>> = listOf(randomFinding().asTemplateArg())
        val alertContext: AlertContext =
            randomAlertContext(
                associatedQueries = associatedQueries,
                sampleDocs = sampleDocs,
            )

        val templateArgs = alertContext.asTemplateArg()

        assertAlertIsEqual(alertContext = alertContext, templateArgs = templateArgs)
        assertEquals(
            "Template args associated queries should have size ${associatedQueries.size}",
            associatedQueries.size,
            (templateArgs[AlertContext.ASSOCIATED_QUERIES_FIELD] as List<DocLevelQuery>).size,
        )
        assertEquals(
            "Template associated queries do not match",
            formatAssociatedQueries(alertContext),
            templateArgs[AlertContext.ASSOCIATED_QUERIES_FIELD],
        )

        assertEquals(
            "Template args sample docs should have size ${sampleDocs.size}",
            sampleDocs.size,
            (templateArgs[AlertContext.SAMPLE_DOCS_FIELD] as List<Map<String, Any?>>).size,
        )
        assertEquals("Template args sample docs do not match", alertContext.sampleDocs, templateArgs[AlertContext.SAMPLE_DOCS_FIELD])
    }

    @Test
    fun `test AlertContext asTemplateArg with multiple associatedQueries and multiple sampleDocs`() {
        val associatedQueries: List<DocLevelQuery> = (0..2).map { randomDocLevelQuery() }
        val sampleDocs: List<Map<String, Any?>> = (0..2).map { randomFinding().asTemplateArg() }
        val alertContext: AlertContext =
            randomAlertContext(
                associatedQueries = associatedQueries,
                sampleDocs = sampleDocs,
            )

        val templateArgs = alertContext.asTemplateArg()

        assertAlertIsEqual(alertContext = alertContext, templateArgs = templateArgs)
        assertEquals(
            "Template args associated queries should have size ${associatedQueries.size}",
            associatedQueries.size,
            (templateArgs[AlertContext.ASSOCIATED_QUERIES_FIELD] as List<DocLevelQuery>).size,
        )
        assertEquals(
            "Template associated queries do not match",
            formatAssociatedQueries(alertContext),
            templateArgs[AlertContext.ASSOCIATED_QUERIES_FIELD],
        )

        assertEquals(
            "Template args sample docs should have size ${sampleDocs.size}",
            sampleDocs.size,
            (templateArgs[AlertContext.SAMPLE_DOCS_FIELD] as List<Map<String, Any?>>).size,
        )
        assertEquals("Template args sample docs do not match", alertContext.sampleDocs, templateArgs[AlertContext.SAMPLE_DOCS_FIELD])
    }

    private fun assertAlertIsEqual(
        alertContext: AlertContext,
        templateArgs: Map<String, Any?>,
    ) {
        assertEquals("Template args id does not match", alertContext.alert.id, templateArgs[Alert.ALERT_ID_FIELD])
        assertEquals("Template args version does not match", alertContext.alert.version, templateArgs[Alert.ALERT_VERSION_FIELD])
        assertEquals("Template args state does not match", alertContext.alert.state.toString(), templateArgs[Alert.STATE_FIELD])
        assertEquals("Template args error message does not match", alertContext.alert.errorMessage, templateArgs[Alert.ERROR_MESSAGE_FIELD])
        assertEquals("Template args acknowledged time does not match", null, templateArgs[Alert.ACKNOWLEDGED_TIME_FIELD])
        assertEquals("Template args end time does not", alertContext.alert.endTime?.toEpochMilli(), templateArgs[Alert.END_TIME_FIELD])
        assertEquals("Template args start time does not", alertContext.alert.startTime.toEpochMilli(), templateArgs[Alert.START_TIME_FIELD])
        assertEquals("Template args last notification time does not match", templateArgs[Alert.LAST_NOTIFICATION_TIME_FIELD], null)
        assertEquals("Template args severity does not match", alertContext.alert.severity, templateArgs[Alert.SEVERITY_FIELD])
        assertEquals(
            "Template args clusters does not match",
            alertContext.alert.clusters?.joinToString(","),
            templateArgs[Alert.CLUSTERS_FIELD],
        )
    }

    private fun formatAssociatedQueries(alertContext: AlertContext): List<Map<String, Any>>? =
        alertContext.associatedQueries?.map {
            mapOf(
                DocLevelQuery.QUERY_ID_FIELD to it.id,
                DocLevelQuery.NAME_FIELD to it.name,
                DocLevelQuery.TAGS_FIELD to it.tags,
            )
        }
}

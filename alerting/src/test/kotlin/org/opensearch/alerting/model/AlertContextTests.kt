/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model

import org.opensearch.alerting.randomAlertContext
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.commons.alerting.model.DocLevelQuery
import org.opensearch.test.OpenSearchTestCase

class AlertContextTests : OpenSearchTestCase() {

    fun `test AlertContext asTemplateArg`() {
        val alertContext: AlertContext = randomAlertContext()
        val templateArgs = alertContext.asTemplateArg()

        assertEquals("Template args id does not match", templateArgs[Alert.ALERT_ID_FIELD], alertContext.alert.id)
        assertEquals("Template args version does not match", templateArgs[Alert.ALERT_VERSION_FIELD], alertContext.alert.version)
        assertEquals("Template args state does not match", templateArgs[Alert.STATE_FIELD], alertContext.alert.state.toString())
        assertEquals("Template args error message does not match", templateArgs[Alert.ERROR_MESSAGE_FIELD], alertContext.alert.errorMessage)
        assertEquals("Template args acknowledged time does not match", templateArgs[Alert.ACKNOWLEDGED_TIME_FIELD], null)
        assertEquals("Template args end time does not", templateArgs[Alert.END_TIME_FIELD], alertContext.alert.endTime?.toEpochMilli())
        assertEquals("Template args start time does not", templateArgs[Alert.START_TIME_FIELD], alertContext.alert.startTime.toEpochMilli())
        assertEquals("Template args last notification time does not match", templateArgs[Alert.LAST_NOTIFICATION_TIME_FIELD], null)
        assertEquals("Template args severity does not match", templateArgs[Alert.SEVERITY_FIELD], alertContext.alert.severity)
        assertEquals("Template args clusters does not match", templateArgs[Alert.CLUSTERS_FIELD], alertContext.alert.clusters?.joinToString(","))
        val formattedQueries = alertContext.associatedQueries?.map {
            mapOf(
                DocLevelQuery.QUERY_ID_FIELD to it.id,
                DocLevelQuery.NAME_FIELD to it.name,
                DocLevelQuery.TAGS_FIELD to it.tags
            )
        }
        assertEquals("Template associated queries do not match", templateArgs[AlertContext.ASSOCIATED_QUERIES_FIELD], formattedQueries)
        assertEquals("Template args sample docs do not match", templateArgs[AlertContext.SAMPLE_DOCS_FIELD], alertContext.sampleDocs)
    }
}
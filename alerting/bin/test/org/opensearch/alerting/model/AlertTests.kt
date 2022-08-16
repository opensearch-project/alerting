/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model

import org.junit.Assert
import org.opensearch.alerting.randomAlert
import org.opensearch.alerting.randomAlertWithAggregationResultBucket
import org.opensearch.test.OpenSearchTestCase

class AlertTests : OpenSearchTestCase() {
    fun `test alert as template args`() {
        val alert = randomAlert().copy(acknowledgedTime = null, lastNotificationTime = null)

        val templateArgs = alert.asTemplateArg()

        assertEquals("Template args id does not match", templateArgs[Alert.ALERT_ID_FIELD], alert.id)
        assertEquals("Template args version does not match", templateArgs[Alert.ALERT_VERSION_FIELD], alert.version)
        assertEquals("Template args state does not match", templateArgs[Alert.STATE_FIELD], alert.state.toString())
        assertEquals("Template args error message does not match", templateArgs[Alert.ERROR_MESSAGE_FIELD], alert.errorMessage)
        assertEquals("Template args acknowledged time does not match", templateArgs[Alert.ACKNOWLEDGED_TIME_FIELD], null)
        assertEquals("Template args end time does not", templateArgs[Alert.END_TIME_FIELD], alert.endTime?.toEpochMilli())
        assertEquals("Template args start time does not", templateArgs[Alert.START_TIME_FIELD], alert.startTime.toEpochMilli())
        assertEquals("Template args last notification time does not match", templateArgs[Alert.LAST_NOTIFICATION_TIME_FIELD], null)
        assertEquals("Template args severity does not match", templateArgs[Alert.SEVERITY_FIELD], alert.severity)
    }

    fun `test agg alert as template args`() {
        val alert = randomAlertWithAggregationResultBucket().copy(acknowledgedTime = null, lastNotificationTime = null)

        val templateArgs = alert.asTemplateArg()

        assertEquals("Template args id does not match", templateArgs[Alert.ALERT_ID_FIELD], alert.id)
        assertEquals("Template args version does not match", templateArgs[Alert.ALERT_VERSION_FIELD], alert.version)
        assertEquals("Template args state does not match", templateArgs[Alert.STATE_FIELD], alert.state.toString())
        assertEquals("Template args error message does not match", templateArgs[Alert.ERROR_MESSAGE_FIELD], alert.errorMessage)
        assertEquals("Template args acknowledged time does not match", templateArgs[Alert.ACKNOWLEDGED_TIME_FIELD], null)
        assertEquals("Template args end time does not", templateArgs[Alert.END_TIME_FIELD], alert.endTime?.toEpochMilli())
        assertEquals("Template args start time does not", templateArgs[Alert.START_TIME_FIELD], alert.startTime.toEpochMilli())
        assertEquals("Template args last notification time does not match", templateArgs[Alert.LAST_NOTIFICATION_TIME_FIELD], null)
        assertEquals("Template args severity does not match", templateArgs[Alert.SEVERITY_FIELD], alert.severity)
        Assert.assertEquals(
            "Template args bucketKeys do not match",
            templateArgs[Alert.BUCKET_KEYS], alert.aggregationResultBucket?.bucketKeys?.joinToString(",")
        )
        Assert.assertEquals(
            "Template args parentBucketPath does not match",
            templateArgs[Alert.PARENTS_BUCKET_PATH], alert.aggregationResultBucket?.parentBucketPath
        )
    }

    fun `test alert acknowledged`() {
        val ackAlert = randomAlert().copy(state = Alert.State.ACKNOWLEDGED)
        assertTrue("Alert is not acknowledged", ackAlert.isAcknowledged())

        val activeAlert = randomAlert().copy(state = Alert.State.ACTIVE)
        assertFalse("Alert is acknowledged", activeAlert.isAcknowledged())
    }
}

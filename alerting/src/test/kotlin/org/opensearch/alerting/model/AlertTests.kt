/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
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

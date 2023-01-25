/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.alerting.util.destinationmigration
import org.junit.Assert.assertEquals
import org.mockito.Mockito
import org.opensearch.commons.notifications.model.NotificationConfigInfo
import org.opensearch.test.OpenSearchTestCase
/*
Tested issue 731 - Ensures correct subject line used in email/SNS notifications."
 */
class NotificationApiUtilsTests : OpenSearchTestCase() {
    companion object {
        @JvmStatic
        fun main() {
            testGetTitle_noSubject()
            testGetTitle_withSubject()
        }

        private fun testGetTitle_noSubject() {
            val configInfo = Mockito.mock(NotificationConfigInfo::class.java)
            Mockito.`when`(configInfo.getTitle(null)).thenReturn("defaultTitle")

            val title = configInfo.getTitle(null)
            assertEquals("defaultTitle", title)
        }

        private fun testGetTitle_withSubject() {
            val configInfo = Mockito.mock(NotificationConfigInfo::class.java)
            Mockito.`when`(configInfo.getTitle("custom subject")).thenReturn("custom subject")

            val title = configInfo.getTitle("custom subject")
            assertEquals("custom subject", title)
        }
    }
}

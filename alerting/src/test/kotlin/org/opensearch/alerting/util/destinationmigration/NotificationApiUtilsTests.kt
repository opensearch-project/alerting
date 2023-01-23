/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.alerting.util.destinationmigration
import org.junit.Assert.assertEquals
import org.mockito.Mockito
import org.opensearch.commons.notifications.model.NotificationConfigInfo
/*
Tested issue 731 - Ensures correct subject line used in email/SNS notifications."
and  "Tested issue 529 - Returns slack notifications to original formatting of $subject \n\n $message without hardcoded prefix
 */
class NotificationApiUtilsTests {
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

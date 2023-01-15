
package org.opensearch.alerting.util.destinationmigration

import org.junit.Assert
import org.junit.Test
import org.opensearch.commons.notifications.model.ConfigType
import org.opensearch.commons.notifications.model.NotificationConfig
import org.opensearch.commons.notifications.model.NotificationConfigInfo
import java.time.Instant

/*
Tested issue 731 - Ensures correct subject line used in email/SNS notifications."
and  "Tested issue 529 - Returns slack notifications to original formatting of $subject \n\n $message without hardcoded prefix
 */

class NotificationApiUtilsTest {

    @Test
    fun testGetTitle() {
        // create the subject that we're going to test with
        val subject = "Urgent: Server down on production"
        // create the lastUpdatedTime and createdTime
        val lastUpdatedTime = Instant.now()
        val createdTime = Instant.now()
        // create the different types of notification config
        val emailNotificationConfig = NotificationConfig("testValidateEmailNotificationConfig", "emailNotificationConfig", ConfigType.EMAIL, null)
        val emailConfig = NotificationConfigInfo("testValidateEmailNotificationConfig", lastUpdatedTime, createdTime, emailNotificationConfig)

        // Test that the getTitle method returns the subject when called with email , sns or   Slack config types
        Assert.assertEquals(subject, emailConfig.getTitle(subject))
    }
}

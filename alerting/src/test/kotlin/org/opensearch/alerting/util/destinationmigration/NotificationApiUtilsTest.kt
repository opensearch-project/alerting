/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

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
//        val emailNotificationConfig = NotificationConfig("testValidateEmailNotificationConfig", "emailNotificationConfig", ConfigType.EMAIL, null)
        val snsNotificationConfig = NotificationConfig("testValidateSNSNotificationConfig", "snsNotificationConfig", ConfigType.SNS, null)
        val slackNotificationConfig = NotificationConfig("testslackNotificationConfig", "slackNotificationConfig", ConfigType.SLACK, null)
        val otherNotificationConfig = NotificationConfig("testValidateOtherNotificationConfig", "otherNotificationConfig", ConfigType.NONE, null)

// create the notification config infos using  the different types of notification config
        val emailConfig = NotificationConfigInfo("testValidateEmailNotificationConfig", lastUpdatedTime, createdTime, emailNotificationConfig)
        val snsConfig = NotificationConfigInfo("testValidateSNSNotificationConfig", lastUpdatedTime, createdTime, snsNotificationConfig)
        val slackConfig = NotificationConfigInfo("testSlackConfig", lastUpdatedTime, createdTime, slackNotificationConfig)
        val otherConfig = NotificationConfigInfo("testValidateOtherNotificationConfig", lastUpdatedTime, createdTime, otherNotificationConfig)

        // Test that the getTitle method returns the subject when called with email , sns or   Slack config types
        Assert.assertEquals("Alerting-Notification Action", slackConfig.getTitle(subject))
        Assert.assertEquals(subject, snsConfig.getTitle(subject))
        Assert.assertEquals("Alerting-Notification Action", slackConfig.getTitle(subject))
        // Test that the getTitle method returns "Alerting-Notification Action"  when the configType is other than email and sns
        Assert.assertEquals("Alerting-Notification Action", otherConfig.getTitle(subject))
        // Test for Slack with null subject
        Assert.assertEquals("Alerting-Notification Action", slackConfig.getTitle(null))
        // Test that the getTitle method returns "Alerting-Notification Action" when the subject is null
        Assert.assertEquals("Alerting-Notification Action", emailConfig.getTitle(null))
    }
}

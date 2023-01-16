/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util.destinationmigration

import org.junit.Assert
import org.opensearch.commons.notifications.model.ConfigType
import org.opensearch.commons.notifications.model.NotificationConfig
import org.opensearch.commons.notifications.model.NotificationConfigInfo
import java.time.Instant

/*
Tested issue 731 - Ensures correct subject line used in email/SNS notifications."
and  "Tested issue 529 - Returns slack notifications to original formatting of $subject \n\n $message without hardcoded prefix
 */

class NotificationApiUtilsTests {
    fun  testgetTitle(){
    	 val subject = "Urgent: Server down on production"
   		 // create the lastUpdatedTime and createdTime
        val lastUpdatedTime = Instant.now()
        val createdTime = Instant.now()
        // create the different types of notification config
        val nMail = "testValidateEmailNotificationConfig"
        val eNConfig = "emailNotificationConfig"
        val emailNotificationConfig = NotificationConfig(nMail, eNConfig, ConfigType.EMAIL, null)
        val nsns = "testValidateSNSNotificationConfig"
        val snsNConfig = "snsNotificationConfig"
        val snsNotificationConfig = NotificationConfig(nsns, snsNConfig, ConfigType.SNS, null)
        val nslack = "testslackNotificationConfig"
        val slackNConfig = "slackNotificationConfig"
        val slackNotificationConfig = NotificationConfig(nslack, slackNConfig, ConfigType.SLACK, null)
        val others = "testValidateOtherNotificationConfig"
        val otherConfigNotif = "otherNotificationConfig"
        val testSlack = "testSlackConfig"

        val otherNotificationConfig = NotificationConfig(others, otherConfigNotif, ConfigType.NONE, null)
        // create the notification config infos using  the different types of notification config
        val emailConfig = NotificationConfigInfo(nMail, lastUpdatedTime, createdTime, emailNotificationConfig)
        val snsConfig = NotificationConfigInfo(nsns, lastUpdatedTime, createdTime, snsNotificationConfig)

        val slackConfig = NotificationConfigInfo(testSlack, lastUpdatedTime, createdTime, slackNotificationConfig)
        val otherConfig = NotificationConfigInfo(others, lastUpdatedTime, createdTime, otherNotificationConfig)

        // Test that the getTitle method returns the subject when called with email , sns or   Slack config types
        Assert.assertEquals(subject, emailConfig.getTitle(subject))
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

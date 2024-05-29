/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model

import org.opensearch.alerting.builder
import org.opensearch.alerting.model.destination.email.EmailAccount
import org.opensearch.alerting.model.destination.email.EmailGroup
import org.opensearch.alerting.parser
import org.opensearch.alerting.randomActionExecutionResult
import org.opensearch.alerting.randomAlert
import org.opensearch.alerting.randomEmailAccount
import org.opensearch.alerting.randomEmailGroup
import org.opensearch.alerting.toJsonString
import org.opensearch.commons.alerting.model.ActionExecutionResult
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.commons.alerting.util.string
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.test.OpenSearchTestCase

class XContentTests : OpenSearchTestCase() {

    fun `test alert parsing`() {
        val alert = randomAlert()

        val alertString = alert.toXContentWithUser(builder()).string()
        val parsedAlert = Alert.parse(parser(alertString))

        assertEquals("Round tripping alert doesn't work", alert, parsedAlert)
    }

    fun `test alert parsing without user`() {
        val alertStr = "{\"id\":\"\",\"version\":-1,\"monitor_id\":\"\",\"schema_version\":0,\"monitor_version\":1," +
            "\"monitor_name\":\"ARahqfRaJG\",\"trigger_id\":\"fhe1-XQBySl0wQKDBkOG\",\"trigger_name\":\"ffELMuhlro\"," +
            "\"state\":\"ACTIVE\",\"error_message\":null,\"alert_history\":[],\"severity\":\"1\",\"action_execution_results\"" +
            ":[{\"action_id\":\"ghe1-XQBySl0wQKDBkOG\",\"last_execution_time\":1601917224583,\"throttled_count\":-1478015168}," +
            "{\"action_id\":\"gxe1-XQBySl0wQKDBkOH\",\"last_execution_time\":1601917224583,\"throttled_count\":-768533744}]," +
            "\"start_time\":1601917224599,\"last_notification_time\":null,\"end_time\":null,\"acknowledged_time\":null}"
        val parsedAlert = Alert.parse(parser(alertStr))
        assertNull(parsedAlert.monitorUser)
    }

    fun `test alert parsing with user as null`() {
        val alertStr = "{\"id\":\"\",\"version\":-1,\"monitor_id\":\"\",\"schema_version\":0,\"monitor_version\":1,\"monitor_user\":null," +
            "\"monitor_name\":\"ARahqfRaJG\",\"trigger_id\":\"fhe1-XQBySl0wQKDBkOG\",\"trigger_name\":\"ffELMuhlro\"," +
            "\"state\":\"ACTIVE\",\"error_message\":null,\"alert_history\":[],\"severity\":\"1\",\"action_execution_results\"" +
            ":[{\"action_id\":\"ghe1-XQBySl0wQKDBkOG\",\"last_execution_time\":1601917224583,\"throttled_count\":-1478015168}," +
            "{\"action_id\":\"gxe1-XQBySl0wQKDBkOH\",\"last_execution_time\":1601917224583,\"throttled_count\":-768533744}]," +
            "\"start_time\":1601917224599,\"last_notification_time\":null,\"end_time\":null,\"acknowledged_time\":null}"
        val parsedAlert = Alert.parse(parser(alertStr))
        assertNull(parsedAlert.monitorUser)
    }

    fun `test action execution result parsing`() {
        val actionExecutionResult = randomActionExecutionResult()

        val actionExecutionResultString = actionExecutionResult.toXContent(builder(), ToXContent.EMPTY_PARAMS).string()
        val parsedActionExecutionResultString = ActionExecutionResult.parse(parser(actionExecutionResultString))

        assertEquals("Round tripping alert doesn't work", actionExecutionResult, parsedActionExecutionResultString)
    }

    fun `test email account parsing`() {
        val emailAccount = randomEmailAccount()

        val emailAccountString = emailAccount.toJsonString()
        val parsedEmailAccount = EmailAccount.parse(parser(emailAccountString))
        assertEquals("Round tripping EmailAccount doesn't work", emailAccount, parsedEmailAccount)
    }

    fun `test email group parsing`() {
        val emailGroup = randomEmailGroup()

        val emailGroupString = emailGroup.toJsonString()
        val parsedEmailGroup = EmailGroup.parse(parser(emailGroupString))
        assertEquals("Round tripping EmailGroup doesn't work", emailGroup, parsedEmailGroup)
    }
}

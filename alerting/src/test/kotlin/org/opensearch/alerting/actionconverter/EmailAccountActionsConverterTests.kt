package org.opensearch.alerting.actionconverter

import org.opensearch.OpenSearchStatusException
import org.opensearch.action.support.WriteRequest
import org.opensearch.alerting.action.DeleteEmailAccountRequest
import org.opensearch.alerting.action.GetEmailAccountRequest
import org.opensearch.alerting.action.IndexEmailAccountRequest
import org.opensearch.alerting.actionconverter.EmailAccountActionsConverter.Companion.convertAlertingToNotificationMethodType
import org.opensearch.alerting.actionconverter.EmailAccountActionsConverter.Companion.convertDeleteEmailAccountRequestToDeleteNotificationConfigRequest
import org.opensearch.alerting.actionconverter.EmailAccountActionsConverter.Companion.convertDeleteNotificationConfigResponseToDeleteResponse
import org.opensearch.alerting.actionconverter.EmailAccountActionsConverter.Companion.convertGetEmailAccountRequestToGetNotificationConfigRequest
import org.opensearch.alerting.actionconverter.EmailAccountActionsConverter.Companion.convertGetNotificationConfigResponseToGetEmailAccountResponse
import org.opensearch.alerting.actionconverter.EmailAccountActionsConverter.Companion.convertIndexEmailAccountRequestToCreateNotificationConfigRequest
import org.opensearch.alerting.actionconverter.EmailAccountActionsConverter.Companion.convertIndexEmailAccountRequestToUpdateNotificationConfigRequest
import org.opensearch.alerting.actionconverter.EmailAccountActionsConverter.Companion.convertNotificationToAlertingMethodType
import org.opensearch.alerting.actionconverter.EmailAccountActionsConverter.Companion.convertToIndexEmailAccountResponse
import org.opensearch.alerting.model.destination.email.EmailAccount
import org.opensearch.commons.notifications.NotificationConstants.FEATURE_ALERTING
import org.opensearch.commons.notifications.action.DeleteNotificationConfigResponse
import org.opensearch.commons.notifications.action.GetNotificationConfigResponse
import org.opensearch.commons.notifications.model.ConfigType
import org.opensearch.commons.notifications.model.MethodType
import org.opensearch.commons.notifications.model.NotificationConfig
import org.opensearch.commons.notifications.model.NotificationConfigInfo
import org.opensearch.commons.notifications.model.NotificationConfigSearchResult
import org.opensearch.commons.notifications.model.SmtpAccount
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestStatus
import org.opensearch.search.fetch.subphase.FetchSourceContext
import org.opensearch.test.OpenSearchTestCase
import java.time.Instant

class EmailAccountActionsConverterTests : OpenSearchTestCase() {

    fun `test convertGetEmailAccountRequestToGetNotificationConfigRequest`() {
        val getEmailAccountRequest = GetEmailAccountRequest("emailAccountId", 0L, RestRequest.Method.GET, FetchSourceContext.FETCH_SOURCE)
        val getNotificationConfigRequest = convertGetEmailAccountRequestToGetNotificationConfigRequest(getEmailAccountRequest)

        assertEquals(1, getNotificationConfigRequest.configIds.size)
        assertEquals(getEmailAccountRequest.emailAccountID, getNotificationConfigRequest.configIds.elementAt(0))
        assertTrue(getNotificationConfigRequest.filterParams.isEmpty())
        assertEquals(0, getNotificationConfigRequest.fromIndex)
        assertEquals(1, getNotificationConfigRequest.maxItems)
        assertNull(getNotificationConfigRequest.sortOrder)
        assertNull(getNotificationConfigRequest.sortField)
    }

    fun `test convertGetNotificationConfigResponseToGetEmailAccountResponse`() {
        val smtpAccount = SmtpAccount("host", 80, MethodType.SSL, "test@email.com")
        val notificationConfig = NotificationConfig(
            "notificationConfig",
            "description",
            ConfigType.SMTP_ACCOUNT,
            setOf(FEATURE_ALERTING),
            smtpAccount,
            true
        )
        val notificationConfigInfo = NotificationConfigInfo("configId", Instant.now(), Instant.now(), notificationConfig)
        val notificationConfigSearchResult = NotificationConfigSearchResult(notificationConfigInfo)
        val getNotificationConfigResponse = GetNotificationConfigResponse(notificationConfigSearchResult)
        val getEmailAccountResponse = convertGetNotificationConfigResponseToGetEmailAccountResponse(getNotificationConfigResponse)

        assertEquals(notificationConfigInfo.configId, getEmailAccountResponse.id)
        assertEquals(notificationConfig.name, getEmailAccountResponse.emailAccount?.name)
        assertEquals(smtpAccount.host, getEmailAccountResponse.emailAccount?.host)
        assertEquals(smtpAccount.fromAddress, getEmailAccountResponse.emailAccount?.email)
        assertEquals(notificationConfigInfo.configId, getEmailAccountResponse.emailAccount?.id)
        assertEquals(EmailAccount.MethodType.SSL, getEmailAccountResponse.emailAccount?.method)
        assertEquals(smtpAccount.port, getEmailAccountResponse.emailAccount?.port)
    }

    fun `test convertGetNotificationConfigResponseToGetEmailAccountResponse with no result`() {
        val notificationConfigSearchResult = NotificationConfigSearchResult(emptyList())
        val getNotificationConfigResponse = GetNotificationConfigResponse(notificationConfigSearchResult)
        try {
            convertGetNotificationConfigResponseToGetEmailAccountResponse(getNotificationConfigResponse)
            fail("Expecting OpenSearchStatusException")
        } catch (e: OpenSearchStatusException) {
            assertEquals("Email Account not found.", e.localizedMessage)
            assertEquals(RestStatus.NOT_FOUND, e.status())
        }
    }

    fun `test convertIndexEmailAccountRequestToCreateNotificationConfigRequest`() {
        val emailAccount = EmailAccount(
            "accountId",
            0L,
            0,
            "accountName",
            "test@email.com",
            "host",
            80,
            EmailAccount.MethodType.SSL,
            null,
            null
        )
        val indexEmailAccountRequest = IndexEmailAccountRequest(
            "emailAccountId",
            0L,
            0L,
            WriteRequest.RefreshPolicy.NONE,
            RestRequest.Method.GET,
            emailAccount
        )

        val createNotificationConfigRequest = convertIndexEmailAccountRequestToCreateNotificationConfigRequest(indexEmailAccountRequest)

        assertEquals(indexEmailAccountRequest.emailAccountID, createNotificationConfigRequest.configId)
        val smtpAccount = createNotificationConfigRequest.notificationConfig.configData as SmtpAccount
        assertEquals(emailAccount.host, smtpAccount.host)
        assertEquals(emailAccount.email, smtpAccount.fromAddress)
        assertEquals(emailAccount.port, smtpAccount.port)
        assertEquals(MethodType.SSL, smtpAccount.method)
        assertEquals(emailAccount.name, createNotificationConfigRequest.notificationConfig.name)
        assertEquals(setOf(FEATURE_ALERTING), createNotificationConfigRequest.notificationConfig.features)
        assertEquals(ConfigType.SMTP_ACCOUNT, createNotificationConfigRequest.notificationConfig.configType)
        assertEquals("Email account created from the Alerting plugin", createNotificationConfigRequest.notificationConfig.description)
    }

    fun `test convertIndexEmailAccountRequestToCreateNotificationConfigRequest with no email account id`() {
        val emailAccount = EmailAccount(
            "",
            0L,
            0,
            "accountName",
            "test@email.com",
            "host",
            80,
            EmailAccount.MethodType.SSL,
            null,
            null
        )
        val indexEmailAccountRequest = IndexEmailAccountRequest(
            "",
            0L,
            0L,
            WriteRequest.RefreshPolicy.NONE,
            RestRequest.Method.GET,
            emailAccount
        )

        val createNotificationConfigRequest = convertIndexEmailAccountRequestToCreateNotificationConfigRequest(indexEmailAccountRequest)

        assertNull(createNotificationConfigRequest.configId)
        val smtpAccount = createNotificationConfigRequest.notificationConfig.configData as SmtpAccount
        assertEquals(emailAccount.host, smtpAccount.host)
        assertEquals(emailAccount.email, smtpAccount.fromAddress)
        assertEquals(emailAccount.port, smtpAccount.port)
        assertEquals(MethodType.SSL, smtpAccount.method)
        assertEquals(emailAccount.name, createNotificationConfigRequest.notificationConfig.name)
        assertEquals(setOf(FEATURE_ALERTING), createNotificationConfigRequest.notificationConfig.features)
        assertEquals(ConfigType.SMTP_ACCOUNT, createNotificationConfigRequest.notificationConfig.configType)
        assertEquals("Email account created from the Alerting plugin", createNotificationConfigRequest.notificationConfig.description)
    }

    fun `test convertIndexEmailAccountRequestToUpdateNotificationConfigRequest`() {
        val emailAccount = EmailAccount(
            "accountId",
            0L,
            0,
            "accountName",
            "test@email.com",
            "host",
            80,
            EmailAccount.MethodType.SSL,
            null,
            null
        )
        val indexEmailAccountRequest = IndexEmailAccountRequest(
            "emailAccountId",
            0L,
            0L,
            WriteRequest.RefreshPolicy.NONE,
            RestRequest.Method.GET,
            emailAccount
        )

        val updateNotificationConfigRequest = convertIndexEmailAccountRequestToUpdateNotificationConfigRequest(indexEmailAccountRequest)

        assertEquals(indexEmailAccountRequest.emailAccountID, updateNotificationConfigRequest.configId)
        val smtpAccount = updateNotificationConfigRequest.notificationConfig.configData as SmtpAccount
        assertEquals(emailAccount.host, smtpAccount.host)
        assertEquals(emailAccount.email, smtpAccount.fromAddress)
        assertEquals(emailAccount.port, smtpAccount.port)
        assertEquals(MethodType.SSL, smtpAccount.method)
        assertEquals(emailAccount.name, updateNotificationConfigRequest.notificationConfig.name)
        assertEquals(setOf(FEATURE_ALERTING), updateNotificationConfigRequest.notificationConfig.features)
        assertEquals(ConfigType.SMTP_ACCOUNT, updateNotificationConfigRequest.notificationConfig.configType)
        assertEquals("Email account created from the Alerting plugin", updateNotificationConfigRequest.notificationConfig.description)
    }

    fun `test convertToIndexEmailAccountResponse`() {
        val smtpAccount = SmtpAccount("host", 80, MethodType.SSL, "test@email.com")
        val notificationConfig = NotificationConfig(
            "notificationConfig",
            "description",
            ConfigType.SMTP_ACCOUNT,
            setOf(FEATURE_ALERTING),
            smtpAccount,
            true
        )
        val notificationConfigInfo = NotificationConfigInfo("configId", Instant.now(), Instant.now(), notificationConfig)
        val notificationConfigSearchResult = NotificationConfigSearchResult(notificationConfigInfo)
        val getNotificationConfigResponse = GetNotificationConfigResponse(notificationConfigSearchResult)
        val emailAccountResponse = convertToIndexEmailAccountResponse("configId", getNotificationConfigResponse)

        assertEquals(notificationConfigInfo.configId, emailAccountResponse.id)
        assertEquals(RestStatus.OK, emailAccountResponse.status)
        assertEquals(notificationConfig.name, emailAccountResponse.emailAccount.name)
        assertEquals(smtpAccount.port, emailAccountResponse.emailAccount.port)
        assertEquals(smtpAccount.fromAddress, emailAccountResponse.emailAccount.email)
        assertEquals(smtpAccount.host, emailAccountResponse.emailAccount.host)
        assertEquals(EmailAccount.MethodType.SSL, emailAccountResponse.emailAccount.method)
        assertNull(emailAccountResponse.emailAccount.username)
        assertNull(emailAccountResponse.emailAccount.password)
    }

    fun `test convertToIndexEmailAccountResponse with null getResponse`() {
        try {
            convertToIndexEmailAccountResponse("configId", null)
            fail("Expecting OpenSearchStatusException")
        } catch (e: OpenSearchStatusException) {
            assertEquals("Email Account failed to be created/updated.", e.localizedMessage)
            assertEquals(RestStatus.NOT_FOUND, e.status())
        }
    }

    fun `test convertDeleteEmailAccountRequestToDeleteNotificationConfigRequest`() {
        val deleteEmailAccountRequest = DeleteEmailAccountRequest("emailAccountId", WriteRequest.RefreshPolicy.NONE)
        val deleteNotificationConfigRequest = convertDeleteEmailAccountRequestToDeleteNotificationConfigRequest(deleteEmailAccountRequest)

        assertEquals(1, deleteNotificationConfigRequest.configIds.size)
        assertEquals(deleteEmailAccountRequest.emailAccountID, deleteNotificationConfigRequest.configIds.elementAt(0))
    }

    fun `test convertDeleteNotificationConfigResponseToDeleteResponse`() {
        val deleteNotificationConfigResponse = DeleteNotificationConfigResponse(mapOf(Pair("configId", RestStatus.OK)))
        val deleteResponse = convertDeleteNotificationConfigResponseToDeleteResponse(deleteNotificationConfigResponse)
        assertEquals("configId", deleteResponse.id)
    }

    fun `test convertDeleteNotificationConfigResponseToDeleteResponse with delete failure`() {
        val deleteNotificationConfigResponse = DeleteNotificationConfigResponse(emptyMap())
        try {
            convertDeleteNotificationConfigResponseToDeleteResponse(deleteNotificationConfigResponse)
            fail("Expecting OpenSearchStatusException")
        } catch (e: OpenSearchStatusException) {
            assertEquals(RestStatus.NOT_FOUND, e.status())
            assertEquals("Email Account failed to be deleted.", e.localizedMessage)
        }
    }

    fun `test convertAlertingToNotificationMethodType type none`() {
        val methodType = convertAlertingToNotificationMethodType(EmailAccount.MethodType.NONE)
        assertEquals(MethodType.NONE, methodType)
    }

    fun `test convertAlertingToNotificationMethodType type TLS`() {
        val methodType = convertAlertingToNotificationMethodType(EmailAccount.MethodType.TLS)
        assertEquals(MethodType.START_TLS, methodType)
    }

    fun `test convertNotificationToAlertingMethodType type none`() {
        val methodType = convertNotificationToAlertingMethodType(MethodType.NONE)
        assertEquals(EmailAccount.MethodType.NONE, methodType)
    }

    fun `test convertNotificationToAlertingMethodType type start tls`() {
        val methodType = convertNotificationToAlertingMethodType(MethodType.START_TLS)
        assertEquals(EmailAccount.MethodType.TLS, methodType)
    }
}

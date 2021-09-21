package org.opensearch.alerting.actionconverter

import org.opensearch.OpenSearchStatusException
import org.opensearch.action.support.WriteRequest
import org.opensearch.alerting.action.DeleteEmailGroupRequest
import org.opensearch.alerting.action.GetEmailGroupRequest
import org.opensearch.alerting.action.IndexEmailGroupRequest
import org.opensearch.alerting.actionconverter.EmailGroupActionsConverter.Companion.convertDeleteEmailGroupRequestToDeleteNotificationConfigRequest
import org.opensearch.alerting.actionconverter.EmailGroupActionsConverter.Companion.convertDeleteNotificationConfigResponseToDeleteResponse
import org.opensearch.alerting.actionconverter.EmailGroupActionsConverter.Companion.convertGetEmailGroupRequestToGetNotificationConfigRequest
import org.opensearch.alerting.actionconverter.EmailGroupActionsConverter.Companion.convertGetNotificationConfigResponseToGetEmailGroupResponse
import org.opensearch.alerting.actionconverter.EmailGroupActionsConverter.Companion.convertIndexEmailGroupRequestToCreateNotificationConfigRequest
import org.opensearch.alerting.actionconverter.EmailGroupActionsConverter.Companion.convertIndexEmailGroupRequestToUpdateNotificationConfigRequest
import org.opensearch.alerting.actionconverter.EmailGroupActionsConverter.Companion.convertToIndexEmailGroupResponse
import org.opensearch.alerting.model.destination.email.EmailAccount
import org.opensearch.alerting.model.destination.email.EmailEntry
import org.opensearch.alerting.util.IndexUtils
import org.opensearch.commons.notifications.NotificationConstants
import org.opensearch.commons.notifications.NotificationConstants.FEATURE_ALERTING
import org.opensearch.commons.notifications.action.DeleteNotificationConfigResponse
import org.opensearch.commons.notifications.action.GetNotificationConfigResponse
import org.opensearch.commons.notifications.model.ConfigType
import org.opensearch.commons.notifications.model.EmailGroup
import org.opensearch.commons.notifications.model.NotificationConfig
import org.opensearch.commons.notifications.model.NotificationConfigInfo
import org.opensearch.commons.notifications.model.NotificationConfigSearchResult
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestStatus
import org.opensearch.search.fetch.subphase.FetchSourceContext
import org.opensearch.test.OpenSearchTestCase
import java.time.Instant

class EmailGroupActionsConverterTests : OpenSearchTestCase() {

    fun `test convertGetEmailGroupRequestToGetNotificationConfigRequest`() {
        val getEmailGroupRequest = GetEmailGroupRequest("emailGroupId", 0L, RestRequest.Method.GET, FetchSourceContext.FETCH_SOURCE)
        val getNotificationConfigRequest = convertGetEmailGroupRequestToGetNotificationConfigRequest(getEmailGroupRequest)

        assertEquals(1, getNotificationConfigRequest.configIds.size)
        assertEquals(getEmailGroupRequest.emailGroupID, getNotificationConfigRequest.configIds.elementAt(0))
        assertTrue(getNotificationConfigRequest.filterParams.isEmpty())
        assertEquals(0, getNotificationConfigRequest.fromIndex)
        assertEquals(1, getNotificationConfigRequest.maxItems)
        assertNull(getNotificationConfigRequest.sortOrder)
        assertNull(getNotificationConfigRequest.sortField)
    }

    fun `test convertGetNotificationConfigResponseToGetEmailGroupResponse`() {
        val emailGroup = EmailGroup(listOf("test@email.com"))
        val notificationConfig = NotificationConfig(
            "notificationConfig",
            "description",
            ConfigType.EMAIL_GROUP,
            setOf(FEATURE_ALERTING),
            emailGroup,
            true
        )
        val notificationConfigInfo = NotificationConfigInfo("configId", Instant.now(), Instant.now(),notificationConfig)
        val notificationConfigSearchResult = NotificationConfigSearchResult(notificationConfigInfo)
        val getNotificationConfigResponse = GetNotificationConfigResponse(notificationConfigSearchResult)
        val getEmailGroupResponse = convertGetNotificationConfigResponseToGetEmailGroupResponse(getNotificationConfigResponse)

        assertEquals(notificationConfigInfo.configId, getEmailGroupResponse.id)
        assertEquals(notificationConfig.name, getEmailGroupResponse.emailGroup?.name)
        assertEquals(notificationConfigInfo.configId, getEmailGroupResponse.emailGroup?.id)
        assertEquals(1, getEmailGroupResponse.emailGroup?.emails?.size)
        assertEquals("test@email.com", getEmailGroupResponse.emailGroup?.emails?.get(0)?.email)
    }

    fun `test convertGetNotificationConfigResponseToGetEmailGroupResponse with no results`() {
        val notificationConfigSearchResult = NotificationConfigSearchResult(listOf())
        val getNotificationConfigResponse = GetNotificationConfigResponse(notificationConfigSearchResult)
        try {
            convertGetNotificationConfigResponseToGetEmailGroupResponse(getNotificationConfigResponse)
            fail("Expecting OpenSearchStatusException")
        } catch (e: OpenSearchStatusException) {
            assertEquals(RestStatus.NOT_FOUND, e.status())
            assertEquals("Email Group not found.", e.localizedMessage)
        }
    }

    fun `test convertIndexEmailGroupRequestToCreateNotificationConfigRequest`() {
        val recipients = listOf(EmailEntry("test@email.com"))
        val emailGroup = org.opensearch.alerting.model.destination.email.EmailGroup(
            "emailGroupId",
            EmailAccount.NO_VERSION,
            IndexUtils.NO_SCHEMA_VERSION,
            "emailGroupName",
            recipients
        )

        val indexEmailGroupRequest = IndexEmailGroupRequest(
            "emailGroupId",
            0L,
            0L,
            WriteRequest.RefreshPolicy.NONE,
            RestRequest.Method.GET,
            emailGroup
        )

        val createNotificationConfigRequest = convertIndexEmailGroupRequestToCreateNotificationConfigRequest(indexEmailGroupRequest)

        assertEquals(indexEmailGroupRequest.emailGroupID, createNotificationConfigRequest.configId)
        val notifEmailGroup = createNotificationConfigRequest.notificationConfig.configData as EmailGroup
        assertEquals(1, notifEmailGroup.recipients.size)
        assertEquals("test@email.com", notifEmailGroup.recipients[0])
        assertEquals(emailGroup.name, createNotificationConfigRequest.notificationConfig.name)
        assertEquals(setOf(NotificationConstants.FEATURE_ALERTING), createNotificationConfigRequest.notificationConfig.features)
        assertEquals(ConfigType.EMAIL_GROUP, createNotificationConfigRequest.notificationConfig.configType)
        assertEquals("Email group created from the Alerting plugin", createNotificationConfigRequest.notificationConfig.description)
    }

    fun `test convertIndexEmailGroupRequestToCreateNotificationConfigRequest with no email group id`() {
        val recipients = listOf(EmailEntry("test@email.com"))
        val emailGroup = org.opensearch.alerting.model.destination.email.EmailGroup(
            EmailAccount.NO_ID,
            EmailAccount.NO_VERSION,
            IndexUtils.NO_SCHEMA_VERSION,
            "emailGroupName",
            recipients
        )

        val indexEmailGroupRequest = IndexEmailGroupRequest(
            "",
            0L,
            0L,
            WriteRequest.RefreshPolicy.NONE,
            RestRequest.Method.GET,
            emailGroup
        )

        val createNotificationConfigRequest = convertIndexEmailGroupRequestToCreateNotificationConfigRequest(indexEmailGroupRequest)

        assertNull(createNotificationConfigRequest.configId)
        val notifEmailGroup = createNotificationConfigRequest.notificationConfig.configData as EmailGroup
        assertEquals(1, notifEmailGroup.recipients.size)
        assertEquals("test@email.com", notifEmailGroup.recipients[0])
        assertEquals(emailGroup.name, createNotificationConfigRequest.notificationConfig.name)
        assertEquals(setOf(NotificationConstants.FEATURE_ALERTING), createNotificationConfigRequest.notificationConfig.features)
        assertEquals(ConfigType.EMAIL_GROUP, createNotificationConfigRequest.notificationConfig.configType)
        assertEquals("Email group created from the Alerting plugin", createNotificationConfigRequest.notificationConfig.description)
    }

    fun `test convertIndexEmailGroupRequestToUpdateNotificationConfigRequest`() {
        val recipients = listOf(EmailEntry("test@email.com"))
        val emailGroup = org.opensearch.alerting.model.destination.email.EmailGroup(
            EmailAccount.NO_ID,
            EmailAccount.NO_VERSION,
            IndexUtils.NO_SCHEMA_VERSION,
            "emailGroupName",
            recipients
        )

        val indexEmailGroupRequest = IndexEmailGroupRequest(
            "emailGroupId",
            0L,
            0L,
            WriteRequest.RefreshPolicy.NONE,
            RestRequest.Method.GET,
            emailGroup
        )

        val updateNotificationConfigRequest = convertIndexEmailGroupRequestToUpdateNotificationConfigRequest(indexEmailGroupRequest)

        assertEquals(indexEmailGroupRequest.emailGroupID, updateNotificationConfigRequest.configId)
        val notifEmailGroup = updateNotificationConfigRequest.notificationConfig.configData as EmailGroup
        assertEquals(1, notifEmailGroup.recipients.size)
        assertEquals("test@email.com", notifEmailGroup.recipients[0])
        assertEquals(emailGroup.name, updateNotificationConfigRequest.notificationConfig.name)
        assertEquals(setOf(NotificationConstants.FEATURE_ALERTING), updateNotificationConfigRequest.notificationConfig.features)
        assertEquals(ConfigType.EMAIL_GROUP, updateNotificationConfigRequest.notificationConfig.configType)
        assertEquals("Email group created from the Alerting plugin", updateNotificationConfigRequest.notificationConfig.description)
    }

    fun `test convertToIndexEmailGroupResponse`() {
        val emailGroup = EmailGroup(listOf("test@email.com"))
        val notificationConfig = NotificationConfig(
            "notificationConfig",
            "description",
            ConfigType.EMAIL_GROUP,
            setOf(NotificationConstants.FEATURE_ALERTING),
            emailGroup,
            true
        )

        val notificationConfigInfo = NotificationConfigInfo("configId", Instant.now(), Instant.now(), notificationConfig)
        val notificationConfigSearchResult = NotificationConfigSearchResult(notificationConfigInfo)
        val getNotificationConfigResponse = GetNotificationConfigResponse(notificationConfigSearchResult)
        val emailGroupResponse = convertToIndexEmailGroupResponse("configId", getNotificationConfigResponse)

        assertEquals(notificationConfigInfo.configId, emailGroupResponse.id)
        assertEquals(RestStatus.OK, emailGroupResponse.status)
        assertEquals(notificationConfigInfo.configId, emailGroupResponse.emailGroup.id)
        assertEquals(notificationConfig.name, emailGroupResponse.emailGroup.name)
        assertEquals(1, emailGroupResponse.emailGroup.emails.size)
        assertEquals("test@email.com", emailGroupResponse.emailGroup.emails[0].email)
    }

    fun `test convertToIndexEmailGroupResponse with null getResponse`() {
        try {
            convertToIndexEmailGroupResponse("configId", null)
            fail("Expecting OpenSearchStatusException")
        } catch (e: OpenSearchStatusException) {
            assertEquals(RestStatus.NOT_FOUND, e.status())
            assertEquals("Email Group failed to be created/updated.", e.localizedMessage)
        }
    }

    fun `test convertDeleteEmailGroupRequestToDeleteNotificationConfigRequest`() {
        val deleteEmailGroupRequest = DeleteEmailGroupRequest("emailGroupId", WriteRequest.RefreshPolicy.NONE)
        val deleteNotificationConfigRequest = convertDeleteEmailGroupRequestToDeleteNotificationConfigRequest(deleteEmailGroupRequest)

        assertEquals(1, deleteNotificationConfigRequest.configIds.size)
        assertEquals(deleteEmailGroupRequest.emailGroupID, deleteNotificationConfigRequest.configIds.elementAt(0))
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
            assertEquals("Email Group failed to be deleted.", e.localizedMessage)
        }
    }
}

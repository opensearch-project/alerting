package org.opensearch.alerting.actionconverter

import org.opensearch.OpenSearchStatusException
import org.opensearch.action.delete.DeleteResponse
import org.opensearch.alerting.action.DeleteEmailGroupRequest
import org.opensearch.alerting.action.GetDestinationsRequest
import org.opensearch.alerting.action.GetDestinationsResponse
import org.opensearch.alerting.action.GetEmailGroupRequest
import org.opensearch.alerting.action.GetEmailGroupResponse
import org.opensearch.alerting.action.IndexEmailAccountResponse
import org.opensearch.alerting.action.IndexEmailGroupRequest
import org.opensearch.alerting.action.IndexEmailGroupResponse
import org.opensearch.alerting.model.destination.email.EmailAccount
import org.opensearch.alerting.model.destination.email.EmailAccount.Companion.NO_ID
import org.opensearch.alerting.model.destination.email.EmailAccount.Companion.NO_VERSION
import org.opensearch.alerting.model.destination.email.EmailEntry
import org.opensearch.alerting.util.IndexUtils
import org.opensearch.commons.notifications.action.CreateNotificationConfigRequest
import org.opensearch.commons.notifications.action.CreateNotificationConfigResponse
import org.opensearch.commons.notifications.action.DeleteNotificationConfigRequest
import org.opensearch.commons.notifications.action.DeleteNotificationConfigResponse
import org.opensearch.commons.notifications.action.GetNotificationConfigRequest
import org.opensearch.commons.notifications.action.GetNotificationConfigResponse
import org.opensearch.commons.notifications.model.ConfigType
import org.opensearch.commons.notifications.model.EmailGroup
import org.opensearch.commons.notifications.model.Feature
import org.opensearch.commons.notifications.model.NotificationConfig
import org.opensearch.rest.RestStatus
import org.opensearch.search.sort.SortOrder
import java.util.*

class GetEmailGroupConverter {

    companion object {
        fun convertGetEmailGroupRequestToGetNotificationConfigRequest(request: GetEmailGroupRequest): GetNotificationConfigRequest {
            val configIds: Set<String> = setOf(request.emailGroupID)

            return GetNotificationConfigRequest(configIds, 0, 1, null, null, emptyMap())
        }

        fun convertGetNotificationConfigResponseToGetEmailGroupResponse(response: GetNotificationConfigResponse): GetEmailGroupResponse {
            val searchResult = response.searchResult
            if (searchResult.totalHits == 0L) throw OpenSearchStatusException("Email Group not found.", RestStatus.NOT_FOUND)
            val notificationConfigInfo = searchResult.objectList[0]
            val notificationConfig = notificationConfigInfo.notificationConfig
            val emailGroup: EmailGroup = notificationConfig.configData as EmailGroup
            val recipients: List<EmailEntry> = listOf()
            emailGroup.recipients.forEach {
                recipients.plus(EmailEntry(it))
            }
            val alertEmailGroup = org.opensearch.alerting.model.destination.email.EmailGroup(
                NO_ID,
                NO_VERSION,
                IndexUtils.NO_SCHEMA_VERSION,
                notificationConfig.name,
                recipients
            )

            return GetEmailGroupResponse(notificationConfigInfo.configId, EmailAccount.NO_VERSION, 0L, 0L, RestStatus.OK, alertEmailGroup)
        }

        fun convertIndexEmailGroupRequestToCreateNotificationConfigRequest(request: IndexEmailGroupRequest): CreateNotificationConfigRequest {
            val emailGroup = request.emailGroup
            val recipients: List<String> = listOf()
            emailGroup.emails.forEach {
                recipients.plus(it.email)
            }
            val notificationEmailGroup = EmailGroup(recipients)

            val description = "Email group created from the Alerting plugin"
            val notificationConfig = NotificationConfig(
                emailGroup.name,
                description,
                ConfigType.EMAIL_GROUP,
                EnumSet.of(Feature.ALERTING),
                notificationEmailGroup
            )

            return CreateNotificationConfigRequest(notificationConfig, request.emailGroupID)
        }

        fun convertCreateNotificationConfigResponseToIndexEmailGroupResponse(createResponse: CreateNotificationConfigResponse, getResponse: GetNotificationConfigResponse): IndexEmailGroupResponse {
            val getEmailGroupResponse = convertGetNotificationConfigResponseToGetEmailGroupResponse(getResponse)
            val emailGroup = getEmailGroupResponse.emailGroup
                ?: throw OpenSearchStatusException("Email Group failed to be created.", RestStatus.NOT_FOUND)
            return IndexEmailGroupResponse(createResponse.configId, 0L, 0L, 0L, RestStatus.OK, emailGroup)
        }

        fun convertDeleteEmailGroupRequestToDeleteNotificationConfigRequest(request: DeleteEmailGroupRequest): DeleteNotificationConfigRequest {
            val configIds: Set<String> = setOf(request.emailGroupID)

            return DeleteNotificationConfigRequest(configIds)
        }

        fun convertDeleteNotificationConfigResponseToDeleteResponse(response: DeleteNotificationConfigResponse): DeleteResponse {
            val configIdToStatusList = response.configIdToStatus.entries
            if (configIdToStatusList.isEmpty()) throw OpenSearchStatusException("Email Group failed to be deleted.", RestStatus.NOT_FOUND)
            val configId = configIdToStatusList.elementAt(0).key
            return DeleteResponse(null, "_doc", configId, 0L, 0L, 0L, true)
        }
    }
}

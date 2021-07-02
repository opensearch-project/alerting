package org.opensearch.alerting.actionconverter

import org.opensearch.OpenSearchStatusException
import org.opensearch.action.delete.DeleteResponse
import org.opensearch.alerting.action.DeleteEmailAccountRequest
import org.opensearch.alerting.action.GetEmailAccountRequest
import org.opensearch.alerting.action.GetEmailAccountResponse
import org.opensearch.alerting.action.IndexEmailAccountRequest
import org.opensearch.alerting.action.IndexEmailAccountResponse
import org.opensearch.alerting.model.destination.Destination
import org.opensearch.alerting.model.destination.email.EmailAccount
import org.opensearch.alerting.util.IndexUtils
import org.opensearch.commons.notifications.action.CreateNotificationConfigRequest
import org.opensearch.commons.notifications.action.CreateNotificationConfigResponse
import org.opensearch.commons.notifications.action.DeleteNotificationConfigRequest
import org.opensearch.commons.notifications.action.DeleteNotificationConfigResponse
import org.opensearch.commons.notifications.action.GetNotificationConfigRequest
import org.opensearch.commons.notifications.action.GetNotificationConfigResponse
import org.opensearch.commons.notifications.model.ConfigType
import org.opensearch.commons.notifications.model.Feature
import org.opensearch.commons.notifications.model.MethodType
import org.opensearch.commons.notifications.model.NotificationConfig
import org.opensearch.commons.notifications.model.SmtpAccount
import org.opensearch.index.shard.ShardId
import org.opensearch.rest.RestStatus
import java.util.*

class GetEmailAccountConverter {

    companion object {
        fun convertGetEmailAccountRequestToNotificationRequest(request: GetEmailAccountRequest): GetNotificationConfigRequest {
            val configIds: Set<String> = setOf(request.emailAccountID)

            return GetNotificationConfigRequest(configIds, 0, 1, null, null, emptyMap())
        }

        fun convertNotificationResponseToGetEmailAccountResponse(response: GetNotificationConfigResponse): GetEmailAccountResponse {
            val searchResult = response.searchResult
            if (searchResult.totalHits == 0L) throw OpenSearchStatusException("Email Account not found.", RestStatus.NOT_FOUND)
            val notificationConfigInfo = searchResult.objectList[0]
            val notificationConfig = notificationConfigInfo.notificationConfig
            val smtpAccount: SmtpAccount = notificationConfig.configData as SmtpAccount
            val methodType = convertNotificationToAlertingMethodType(smtpAccount.method)
            val emailAccount = EmailAccount(
                EmailAccount.NO_ID,
                EmailAccount.NO_VERSION,
                IndexUtils.NO_SCHEMA_VERSION,
                notificationConfig.name,
                smtpAccount.fromAddress,
                smtpAccount.host,
                smtpAccount.port,
                methodType,
                null,
                null)

            return GetEmailAccountResponse(notificationConfigInfo.configId, EmailAccount.NO_VERSION, 0L, 0L, RestStatus.OK, emailAccount)
        }

        fun convertIndexEmailAccountRequestToNotificationRequest(request: IndexEmailAccountRequest): CreateNotificationConfigRequest {
            val emailAccount = request.emailAccount
            val methodType = convertAlertingToNotificationMethodType(emailAccount.method)
            val smtpAccount = SmtpAccount(emailAccount.host, emailAccount.port, methodType, emailAccount.email)
            val description = "Email account created from the Alerting plugin"
            val notificationConfig = NotificationConfig(
                emailAccount.name,
                description,
                ConfigType.SMTP_ACCOUNT,
                EnumSet.of(Feature.ALERTING),
                smtpAccount
            )
            return CreateNotificationConfigRequest(notificationConfig, request.emailAccountID)
        }

        fun convertCreateNotificationResponseToIndexEmailAccountResponse(createResponse: CreateNotificationConfigResponse, getResponse: GetNotificationConfigResponse): IndexEmailAccountResponse {
            val getEmailResponse = convertNotificationResponseToGetEmailAccountResponse(getResponse)
            val emailAccount = getEmailResponse.emailAccount
                ?: throw OpenSearchStatusException("Email Account failed to be created.", RestStatus.NOT_FOUND)
            return IndexEmailAccountResponse(createResponse.configId, 0L, 0L, 0L, RestStatus.OK, emailAccount)
        }

        fun convertDeleteEmailAccountRequestToNotificationRequest(request: DeleteEmailAccountRequest): DeleteNotificationConfigRequest {
            val configIds: Set<String> = setOf(request.emailAccountID)

            return DeleteNotificationConfigRequest(configIds)
        }

        fun convertDeleteNotificationResponseToDeleteEmailAccountResponse(response: DeleteNotificationConfigResponse): DeleteResponse {
            val configIdToStatusList = response.configIdToStatus.entries
            if (configIdToStatusList.size == 0) throw OpenSearchStatusException("Email Account failed to be deleted.", RestStatus.NOT_FOUND)
            val configId = configIdToStatusList.elementAt(0).key
            return DeleteResponse(null, "_doc", configId, 0L, 0L, 0L, true)
        }

        fun convertAlertingToNotificationMethodType(alertMethodType: EmailAccount.MethodType): MethodType {
            when (alertMethodType) {
                EmailAccount.MethodType.NONE -> return MethodType.NONE
                EmailAccount.MethodType.SSL -> return MethodType.SSL
                EmailAccount.MethodType.TLS -> return MethodType.START_TLS
                else -> return MethodType.NONE
            }
        }

        fun convertNotificationToAlertingMethodType(notificationMethodType: MethodType): EmailAccount.MethodType {
            when (notificationMethodType) {
                MethodType.NONE -> return EmailAccount.MethodType.NONE
                MethodType.SSL -> return EmailAccount.MethodType.SSL
                MethodType.START_TLS -> return EmailAccount.MethodType.TLS
                else -> return EmailAccount.MethodType.NONE
            }
        }
    }
}

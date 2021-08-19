/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.alerting.actionconverter

import org.opensearch.OpenSearchStatusException
import org.opensearch.action.delete.DeleteResponse
import org.opensearch.alerting.action.DeleteEmailAccountRequest
import org.opensearch.alerting.action.GetEmailAccountRequest
import org.opensearch.alerting.action.GetEmailAccountResponse
import org.opensearch.alerting.action.IndexEmailAccountRequest
import org.opensearch.alerting.action.IndexEmailAccountResponse
import org.opensearch.alerting.model.destination.email.EmailAccount
import org.opensearch.alerting.util.IndexUtils
import org.opensearch.commons.notifications.action.CreateNotificationConfigRequest
import org.opensearch.commons.notifications.action.DeleteNotificationConfigRequest
import org.opensearch.commons.notifications.action.DeleteNotificationConfigResponse
import org.opensearch.commons.notifications.action.GetNotificationConfigRequest
import org.opensearch.commons.notifications.action.GetNotificationConfigResponse
import org.opensearch.commons.notifications.action.UpdateNotificationConfigRequest
import org.opensearch.commons.notifications.model.ConfigType
import org.opensearch.commons.notifications.model.Feature
import org.opensearch.commons.notifications.model.MethodType
import org.opensearch.commons.notifications.model.NotificationConfig
import org.opensearch.commons.notifications.model.SmtpAccount
import org.opensearch.index.Index
import org.opensearch.index.shard.ShardId
import org.opensearch.rest.RestStatus
import java.util.EnumSet

class EmailAccountActionsConverter {

    companion object {
        fun convertGetEmailAccountRequestToGetNotificationConfigRequest(request: GetEmailAccountRequest): GetNotificationConfigRequest {
            val configIds: Set<String> = setOf(request.emailAccountID)

            return GetNotificationConfigRequest(configIds, 0, 1, null, null, emptyMap())
        }

        fun convertGetNotificationConfigResponseToGetEmailAccountResponse(
            response: GetNotificationConfigResponse
        ): GetEmailAccountResponse {
            val searchResult = response.searchResult
            if (searchResult.totalHits == 0L) throw OpenSearchStatusException("Email Account not found.", RestStatus.NOT_FOUND)
            val notificationConfigInfo = searchResult.objectList[0]
            val notificationConfig = notificationConfigInfo.notificationConfig
            val smtpAccount: SmtpAccount = notificationConfig.configData as SmtpAccount
            val methodType = convertNotificationToAlertingMethodType(smtpAccount.method)
            val emailAccount = EmailAccount(
                notificationConfigInfo.configId,
                EmailAccount.NO_VERSION,
                IndexUtils.NO_SCHEMA_VERSION,
                notificationConfig.name,
                smtpAccount.fromAddress,
                smtpAccount.host,
                smtpAccount.port,
                methodType,
                null,
                null
            )

            return GetEmailAccountResponse(notificationConfigInfo.configId, EmailAccount.NO_VERSION, 0L, 0L, RestStatus.OK, emailAccount)
        }

        //{"name":"Test_sender","email":"test@example.com","host":"example.com","port":25,"method":"none"}
        fun convertIndexEmailAccountRequestToCreateNotificationConfigRequest(
            request: IndexEmailAccountRequest
        ): CreateNotificationConfigRequest {
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
            val configId = if (request.emailAccountID == "") null else request.emailAccountID
            return CreateNotificationConfigRequest(notificationConfig, configId)
        }

        fun convertIndexEmailAccountRequestToUpdateNotificationConfigRequest(
            request: IndexEmailAccountRequest
        ): UpdateNotificationConfigRequest {
            val notificationConfig = convertEmailAccountToNotificationConfig(request.emailAccount)
            return UpdateNotificationConfigRequest(request.emailAccountID, notificationConfig)
        }

        fun convertToIndexEmailAccountResponse(
            configId: String,
            getResponse: GetNotificationConfigResponse?
        ): IndexEmailAccountResponse {
            val getEmailResponse = if (getResponse != null) {
                convertGetNotificationConfigResponseToGetEmailAccountResponse(getResponse)
            } else {
                throw OpenSearchStatusException("Email Account failed to be created/updated.", RestStatus.NOT_FOUND)
            }
            val emailAccount = getEmailResponse.emailAccount
                ?: throw OpenSearchStatusException("Email Account failed to be created/updated.", RestStatus.NOT_FOUND)
            return IndexEmailAccountResponse(configId, 0L, 0L, 0L, RestStatus.OK, emailAccount)
        }

        fun convertDeleteEmailAccountRequestToDeleteNotificationConfigRequest(
            request: DeleteEmailAccountRequest
        ): DeleteNotificationConfigRequest {
            val configIds: Set<String> = setOf(request.emailAccountID)

            return DeleteNotificationConfigRequest(configIds)
        }

        fun convertDeleteNotificationConfigResponseToDeleteResponse(response: DeleteNotificationConfigResponse): DeleteResponse {
            val configIdToStatusList = response.configIdToStatus.entries
            if (configIdToStatusList.isEmpty()) throw OpenSearchStatusException("Email Account failed to be deleted.", RestStatus.NOT_FOUND)
            val configId = configIdToStatusList.elementAt(0).key
            val index = Index("notification_index", "uuid")
            val shardId = ShardId(index, 0)
            return DeleteResponse(shardId, "_doc", configId, 0L, 0L, 0L, true)
        }

        internal fun convertAlertingToNotificationMethodType(alertMethodType: EmailAccount.MethodType): MethodType {
            return when (alertMethodType) {
                EmailAccount.MethodType.NONE -> MethodType.NONE
                EmailAccount.MethodType.SSL -> MethodType.SSL
                EmailAccount.MethodType.TLS -> MethodType.START_TLS
            }
        }

        internal fun convertNotificationToAlertingMethodType(notificationMethodType: MethodType): EmailAccount.MethodType {
            return when (notificationMethodType) {
                MethodType.NONE -> EmailAccount.MethodType.NONE
                MethodType.SSL -> EmailAccount.MethodType.SSL
                MethodType.START_TLS -> EmailAccount.MethodType.TLS
                else -> EmailAccount.MethodType.NONE
            }
        }

        fun convertEmailAccountToNotificationConfig(emailAccount: EmailAccount): NotificationConfig {
            val methodType = convertAlertingToNotificationMethodType(emailAccount.method)
            val smtpAccount = SmtpAccount(emailAccount.host, emailAccount.port, methodType, emailAccount.email)
            val description = "Email account created from the Alerting plugin"
            return NotificationConfig(
                emailAccount.name,
                description,
                ConfigType.SMTP_ACCOUNT,
                EnumSet.of(Feature.ALERTING),
                smtpAccount
            )
        }
    }
}

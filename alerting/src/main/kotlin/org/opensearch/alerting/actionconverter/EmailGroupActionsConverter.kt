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
import org.opensearch.alerting.action.DeleteEmailGroupRequest
import org.opensearch.alerting.action.GetEmailGroupRequest
import org.opensearch.alerting.action.GetEmailGroupResponse
import org.opensearch.alerting.action.IndexEmailGroupRequest
import org.opensearch.alerting.action.IndexEmailGroupResponse
import org.opensearch.alerting.model.destination.email.EmailAccount.Companion.NO_VERSION
import org.opensearch.alerting.model.destination.email.EmailEntry
import org.opensearch.alerting.util.IndexUtils
import org.opensearch.commons.notifications.NotificationConstants.FEATURE_ALERTING
import org.opensearch.commons.notifications.action.CreateNotificationConfigRequest
import org.opensearch.commons.notifications.action.DeleteNotificationConfigRequest
import org.opensearch.commons.notifications.action.DeleteNotificationConfigResponse
import org.opensearch.commons.notifications.action.GetNotificationConfigRequest
import org.opensearch.commons.notifications.action.GetNotificationConfigResponse
import org.opensearch.commons.notifications.action.UpdateNotificationConfigRequest
import org.opensearch.commons.notifications.model.ConfigType
import org.opensearch.commons.notifications.model.EmailGroup
import org.opensearch.commons.notifications.model.NotificationConfig
import org.opensearch.index.Index
import org.opensearch.index.shard.ShardId
import org.opensearch.rest.RestStatus

class EmailGroupActionsConverter {

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
            val recipients = mutableListOf<EmailEntry>()
            emailGroup.recipients.forEach {
                recipients.add(EmailEntry(it))
            }
            val alertEmailGroup = org.opensearch.alerting.model.destination.email.EmailGroup(
                notificationConfigInfo.configId,
                NO_VERSION,
                IndexUtils.NO_SCHEMA_VERSION,
                notificationConfig.name,
                recipients
            )

            return GetEmailGroupResponse(notificationConfigInfo.configId, NO_VERSION, 0L, 0L, RestStatus.OK, alertEmailGroup)
        }

        fun convertIndexEmailGroupRequestToCreateNotificationConfigRequest(
            request: IndexEmailGroupRequest
        ): CreateNotificationConfigRequest {
            val emailGroup = request.emailGroup
            val recipients = mutableListOf<String>()
            emailGroup.emails.forEach {
                recipients.add(it.email)
            }
            val notificationEmailGroup = EmailGroup(recipients)

            val description = "Email group created from the Alerting plugin"
            val notificationConfig = NotificationConfig(
                emailGroup.name,
                description,
                ConfigType.EMAIL_GROUP,
                setOf(FEATURE_ALERTING),
                notificationEmailGroup
            )
            val configId = if (request.emailGroupID == "") null else request.emailGroupID
            return CreateNotificationConfigRequest(notificationConfig, configId)
        }

        fun convertIndexEmailGroupRequestToUpdateNotificationConfigRequest(
            request: IndexEmailGroupRequest
        ): UpdateNotificationConfigRequest {
            val notificationConfig = convertEmailGroupToNotificationConfig(request.emailGroup)

            return UpdateNotificationConfigRequest(request.emailGroupID, notificationConfig)
        }

        fun convertToIndexEmailGroupResponse(
            configId: String,
            getResponse: GetNotificationConfigResponse?
        ): IndexEmailGroupResponse {
            val getEmailGroupResponse = if (getResponse != null) {
                convertGetNotificationConfigResponseToGetEmailGroupResponse(getResponse)
            } else {
                throw OpenSearchStatusException("Email Group failed to be created/updated.", RestStatus.NOT_FOUND)
            }
            val emailGroup = getEmailGroupResponse.emailGroup
                ?: throw OpenSearchStatusException("Email Group failed to be created/updated.", RestStatus.NOT_FOUND)
            return IndexEmailGroupResponse(configId, 0L, 0L, 0L, RestStatus.OK, emailGroup)
        }

        fun convertDeleteEmailGroupRequestToDeleteNotificationConfigRequest(
            request: DeleteEmailGroupRequest
        ): DeleteNotificationConfigRequest {
            val configIds: Set<String> = setOf(request.emailGroupID)

            return DeleteNotificationConfigRequest(configIds)
        }

        fun convertDeleteNotificationConfigResponseToDeleteResponse(response: DeleteNotificationConfigResponse): DeleteResponse {
            val configIdToStatusList = response.configIdToStatus.entries
            if (configIdToStatusList.isEmpty()) throw OpenSearchStatusException("Email Group failed to be deleted.", RestStatus.NOT_FOUND)
            val configId = configIdToStatusList.elementAt(0).key
            val index = Index("notification_index", "uuid")
            val shardId = ShardId(index, 0)
            return DeleteResponse(shardId, "_doc", configId, 0L, 0L, 0L, true)
        }

        fun convertEmailGroupToNotificationConfig(
            emailGroup: org.opensearch.alerting.model.destination.email.EmailGroup
        ): NotificationConfig {
            val recipients = mutableListOf<String>()
            emailGroup.emails.forEach {
                recipients.add(it.email)
            }
            val notificationEmailGroup = EmailGroup(recipients)

            val description = "Email group created from the Alerting plugin"
            return NotificationConfig(
                emailGroup.name,
                description,
                ConfigType.EMAIL_GROUP,
                setOf(FEATURE_ALERTING),
                notificationEmailGroup
            )
        }
    }
}

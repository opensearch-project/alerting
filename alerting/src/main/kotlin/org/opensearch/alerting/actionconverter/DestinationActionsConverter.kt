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

import org.apache.http.client.utils.URIBuilder
import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.delete.DeleteResponse
import org.opensearch.alerting.action.DeleteDestinationRequest
import org.opensearch.alerting.action.GetDestinationsRequest
import org.opensearch.alerting.action.GetDestinationsResponse
import org.opensearch.alerting.action.IndexDestinationRequest
import org.opensearch.alerting.action.IndexDestinationResponse
import org.opensearch.alerting.model.destination.CustomWebhook
import org.opensearch.alerting.model.destination.Destination
import org.opensearch.alerting.model.destination.email.Recipient
import org.opensearch.alerting.util.DestinationType
import org.opensearch.alerting.util.IndexUtils
import org.opensearch.common.Strings
import org.opensearch.commons.notifications.NotificationConstants.CONFIG_TYPE_TAG
import org.opensearch.commons.notifications.NotificationConstants.NAME_TAG
import org.opensearch.commons.notifications.NotificationConstants.UPDATED_TIME_TAG
import org.opensearch.commons.notifications.action.CreateNotificationConfigRequest
import org.opensearch.commons.notifications.action.CreateNotificationConfigResponse
import org.opensearch.commons.notifications.action.DeleteNotificationConfigRequest
import org.opensearch.commons.notifications.action.DeleteNotificationConfigResponse
import org.opensearch.commons.notifications.action.GetNotificationConfigRequest
import org.opensearch.commons.notifications.action.GetNotificationConfigResponse
import org.opensearch.commons.notifications.action.UpdateNotificationConfigRequest
import org.opensearch.commons.notifications.action.UpdateNotificationConfigResponse
import org.opensearch.commons.notifications.model.Chime
import org.opensearch.commons.notifications.model.ConfigType
import org.opensearch.commons.notifications.model.Email
import org.opensearch.commons.notifications.model.Feature
import org.opensearch.commons.notifications.model.NotificationConfig
import org.opensearch.commons.notifications.model.NotificationConfigInfo
import org.opensearch.commons.notifications.model.Slack
import org.opensearch.commons.notifications.model.Webhook
import org.opensearch.index.Index
import org.opensearch.index.shard.ShardId
import org.opensearch.rest.RestStatus
import org.opensearch.search.sort.SortOrder
import java.net.URI
import java.net.URISyntaxException
import java.util.EnumSet

// TODO: Force destinations to have https url if Notification plugin wont support it
class DestinationActionsConverter {

    companion object {
        private val logger = LogManager.getLogger(DestinationActionsConverter::class)

        fun convertGetDestinationsRequestToGetNotificationConfigRequest(request: GetDestinationsRequest): GetNotificationConfigRequest {
            val configIds: Set<String> = if (request.destinationId != null) setOf(request.destinationId) else emptySet()
            val table = request.table
            val fromIndex = table.startIndex
            val maxItems = table.size
            val sortOrder: SortOrder = SortOrder.fromString(table.sortOrder)
            val filterParams = HashMap<String, String>()
            if (request.destinationType == "ALL") {
                filterParams["config_type"] = "slack,chime,webhook,email"
            } else {
                if (request.destinationType == "custom_webhook") {
                    filterParams["config_type"] = "custom_webhook"
                } else {
                    filterParams["config_type"] = request.destinationType
                }
            }
            if (!table.searchString.isNullOrBlank()) {
                filterParams["name"] = table.searchString
            }

            val sortField = when(table.sortString) {
                "destination.name.keyword" -> NAME_TAG
                "destination.type" -> CONFIG_TYPE_TAG
                "destination.last_update_time" -> UPDATED_TIME_TAG
                else -> null
            }

            return GetNotificationConfigRequest(configIds, fromIndex, maxItems, sortField, sortOrder, filterParams)
        }

        fun convertGetNotificationConfigResponseToGetDestinationsResponse(
            response: GetNotificationConfigResponse?
        ): GetDestinationsResponse {
            if (response == null) throw OpenSearchStatusException("Destination cannot be found.", RestStatus.NOT_FOUND)
            val searchResult = response.searchResult
//            if (searchResult.objectList.isEmpty()) throw OpenSearchStatusException("Destinations not found.", RestStatus.NOT_FOUND)
            val destinations = mutableListOf<Destination>()
            searchResult.objectList.forEach {
                val destination = convertNotificationConfigToDestination(it)
                if (destination != null) {
                    destinations += destination
                } else {
                    logger.error("Destination was null and cannot be converted")
                }
            }
            return GetDestinationsResponse(RestStatus.OK, searchResult.totalHits.toInt(), destinations)
        }

        fun convertIndexDestinationRequestToCreateNotificationConfigRequest(
            request: IndexDestinationRequest
        ): CreateNotificationConfigRequest {
            val notificationConfig = convertDestinationToNotificationConfig(request.destination)
                ?: throw OpenSearchStatusException("Destination cannot be created.", RestStatus.NOT_FOUND)
            val configId = if (request.destinationId == "") null else request.destinationId
            return CreateNotificationConfigRequest(notificationConfig, configId)
        }

        fun convertCreateNotificationConfigResponseToIndexDestinationResponse(
            createResponse: CreateNotificationConfigResponse,
            getResponse: GetNotificationConfigResponse?
        ): IndexDestinationResponse {
            val destination = if (getResponse != null) {
                convertGetNotificationConfigResponseToGetDestinationsResponse(getResponse).destinations[0]
            } else {
                throw OpenSearchStatusException("Destination failed to be created.", RestStatus.NOT_FOUND)
            }

            return IndexDestinationResponse(createResponse.configId, 0L, 0L, 0L, RestStatus.OK, destination)
        }

        fun convertIndexDestinationRequestToUpdateNotificationConfigRequest(
            request: IndexDestinationRequest
        ): UpdateNotificationConfigRequest {
            val notificationConfig = convertDestinationToNotificationConfig(request.destination)
                ?: throw OpenSearchStatusException("Destination cannot be created.", RestStatus.NOT_FOUND)
            return UpdateNotificationConfigRequest(request.destinationId, notificationConfig)
        }

        fun convertUpdateNotificationConfigResponseToIndexDestinationResponse(
            updateResponse: UpdateNotificationConfigResponse,
            getResponse: GetNotificationConfigResponse?
        ): IndexDestinationResponse {
            val destination = if (getResponse != null) {
                convertGetNotificationConfigResponseToGetDestinationsResponse(getResponse).destinations[0]
            } else {
                throw OpenSearchStatusException("Destination failed to be created.", RestStatus.NOT_FOUND)
            }

            return IndexDestinationResponse(updateResponse.configId, 0L, 0L, 0L, RestStatus.OK, destination)
        }

        fun convertDeleteDestinationRequestToDeleteNotificationConfigRequest(
            request: DeleteDestinationRequest
        ): DeleteNotificationConfigRequest {
            val configIds: Set<String> = setOf(request.destinationId)
            return DeleteNotificationConfigRequest(configIds)
        }

        fun convertDeleteNotificationConfigResponseToDeleteResponse(response: DeleteNotificationConfigResponse): DeleteResponse {
            val configIdToStatusList = response.configIdToStatus.entries
            if (configIdToStatusList.isEmpty()) throw OpenSearchStatusException("Destinations failed to be deleted.", RestStatus.NOT_FOUND)
            val configId = configIdToStatusList.elementAt(0).key
            val index = Index("notification_index", "uuid")
            val shardId = ShardId(index, 0)
            return DeleteResponse(shardId, "_doc", configId, 0L, 0L, 0L, true)
        }

        private fun convertNotificationConfigToDestination(notificationConfigInfo: NotificationConfigInfo): Destination? {
            val notificationConfig = notificationConfigInfo.notificationConfig
            when (notificationConfig.configType) {
                ConfigType.SLACK -> {
                    val slack = notificationConfig.configData as Slack
                    val alertSlack = org.opensearch.alerting.model.destination.Slack(slack.url)
                    return Destination(
                        notificationConfigInfo.configId,
                        Destination.NO_VERSION,
                        IndexUtils.NO_SCHEMA_VERSION,
                        Destination.NO_SEQ_NO,
                        Destination.NO_PRIMARY_TERM,
                        DestinationType.SLACK,
                        notificationConfig.name,
                        null,
                        notificationConfigInfo.lastUpdatedTime,
                        null,
                        alertSlack,
                        null,
                        null
                    )
                }
                ConfigType.CHIME -> {
                    val chime = notificationConfig.configData as Chime
                    val alertChime = org.opensearch.alerting.model.destination.Chime(chime.url)
                    return Destination(
                        notificationConfigInfo.configId,
                        Destination.NO_VERSION,
                        IndexUtils.NO_SCHEMA_VERSION,
                        Destination.NO_SEQ_NO,
                        Destination.NO_PRIMARY_TERM,
                        DestinationType.CHIME,
                        notificationConfig.name,
                        null,
                        notificationConfigInfo.lastUpdatedTime,
                        alertChime,
                        null,
                        null,
                        null
                    )
                }
                ConfigType.WEBHOOK -> {
                    val webhook = notificationConfig.configData as Webhook
                    val scheme: String? = null
                    val host: String? = null
                    val port = -1
                    val path: String? = null
                    val method: String? = null
                    val username: String? = null
                    val password: String? = null
                    val alertWebhook = CustomWebhook(
                        webhook.url,
                        scheme,
                        host,
                        port,
                        path,
                        method,
                        emptyMap(),
                        webhook.headerParams,
                        username,
                        password
                    )
                    return Destination(
                        notificationConfigInfo.configId,
                        Destination.NO_VERSION,
                        IndexUtils.NO_SCHEMA_VERSION,
                        Destination.NO_SEQ_NO,
                        Destination.NO_PRIMARY_TERM,
                        DestinationType.CUSTOM_WEBHOOK,
                        notificationConfig.name,
                        null,
                        notificationConfigInfo.lastUpdatedTime,
                        null,
                        null,
                        alertWebhook,
                        null
                    )
                }
                ConfigType.EMAIL -> {
                    val email: Email = notificationConfig.configData as Email
                    val recipients = mutableListOf<Recipient>()
                    email.recipients.forEach {
                        val recipient = Recipient(Recipient.RecipientType.EMAIL, null, it)
                        recipients.add(recipient)
                    }
                    email.emailGroupIds.forEach {
                        val recipient = Recipient(Recipient.RecipientType.EMAIL_GROUP, it, null)
                        recipients.add(recipient)
                    }
                    val alertEmail = org.opensearch.alerting.model.destination.email.Email(email.emailAccountID, recipients)
                    return Destination(
                        notificationConfigInfo.configId,
                        Destination.NO_VERSION,
                        IndexUtils.NO_SCHEMA_VERSION,
                        Destination.NO_SEQ_NO,
                        Destination.NO_PRIMARY_TERM,
                        DestinationType.EMAIL,
                        notificationConfig.name,
                        null,
                        notificationConfigInfo.lastUpdatedTime,
                        null,
                        null,
                        null,
                        alertEmail
                    )
                }
                ConfigType.SMTP_ACCOUNT -> {
                    return null
                }
                ConfigType.EMAIL_GROUP -> {
                    return null
                }
                else -> {
                    logger.warn(
                        "Failed config match for type ${notificationConfig.configType} and configId " +
                            notificationConfigInfo.configId
                    )
                    return null
                }
            }
        }

        fun convertDestinationToNotificationConfig(destination: Destination): NotificationConfig? {
            when (destination.type) {
                DestinationType.CHIME -> {
                    val alertChime = destination.chime
                    val chime = if (alertChime == null) null else Chime(alertChime!!.url)
                    val description = "Chime destination created from the Alerting plugin"
                    return NotificationConfig(
                        destination.name,
                        description,
                        ConfigType.CHIME,
                        EnumSet.of(Feature.ALERTING),
                        chime
                    )
                }
                DestinationType.SLACK -> {
                    val alertSlack = destination.slack
                    val slack = if (alertSlack == null) null else Slack(alertSlack.url)
                    val description = "Slack destination created from the Alerting plugin"
                    return NotificationConfig(
                        destination.name,
                        description,
                        ConfigType.SLACK,
                        EnumSet.of(Feature.ALERTING),
                        slack
                    )
                }
                DestinationType.CUSTOM_WEBHOOK -> {
                    val alertWebhook = destination.customWebhook
                    var webhook: Webhook? = null
                    if (alertWebhook != null) {
                        val uri = buildUri(
                            alertWebhook.url,
                            alertWebhook.scheme,
                            alertWebhook.host,
                            alertWebhook.port,
                            alertWebhook.path,
                            alertWebhook.queryParams
                        ).toString()
                        webhook = Webhook(uri, alertWebhook.headerParams)
                    }
                    val description = "Webhook destination created from the Alerting plugin"
                    return NotificationConfig(
                        destination.name,
                        description,
                        ConfigType.WEBHOOK,
                        EnumSet.of(Feature.ALERTING),
                        webhook
                    )
                }
                DestinationType.EMAIL -> {
                    val alertEmail = destination.email
                    var email: Email? = null
                    if (alertEmail != null) {
                        val recipients = mutableListOf<String>()
                        val emailGroupIds = mutableListOf<String>()
                        alertEmail.recipients.forEach {
                            if (it.type == Recipient.RecipientType.EMAIL_GROUP) emailGroupIds.plus(it.emailGroupID)
                            else recipients.plus(it.email)
                        }
                        email = Email(alertEmail.emailAccountID, recipients, emailGroupIds)
                    }
                    val description = "Email destination created from the Alerting plugin"
                    return NotificationConfig(
                        destination.name,
                        description,
                        ConfigType.EMAIL,
                        EnumSet.of(Feature.ALERTING),
                        email
                    )
                }
            }
            return null
        }

        fun buildUri(
            endpoint: String?,
            scheme: String?,
            host: String?,
            port: Int,
            path: String?,
            queryParams: Map<String, String>
        ): URI? {
            var scheme = scheme
            return try {
                if (Strings.isNullOrEmpty(endpoint)) {
                    if (Strings.isNullOrEmpty(scheme)) {
                        scheme = "https"
                    }
                    val uriBuilder = URIBuilder()
                    if (queryParams != null) {
                        for ((key, value) in queryParams) uriBuilder.addParameter(key, value)
                    }
                    return uriBuilder.setScheme(scheme).setHost(host).setPort(port).setPath(path).build()
                }
                URIBuilder(endpoint).build()
            } catch (exception: URISyntaxException) {
                throw IllegalStateException("Error creating URI")
            }
        }
    }
}

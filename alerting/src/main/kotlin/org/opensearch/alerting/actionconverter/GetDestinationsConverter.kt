package org.opensearch.alerting.actionconverter

import org.apache.http.client.utils.URIBuilder
import org.opensearch.OpenSearchStatusException
import org.opensearch.alerting.action.GetDestinationsRequest
import org.opensearch.alerting.action.GetDestinationsResponse
import org.opensearch.alerting.action.IndexDestinationRequest
import org.opensearch.alerting.model.destination.CustomWebhook
import org.opensearch.alerting.model.destination.Destination
import org.opensearch.alerting.model.destination.email.Recipient
import org.opensearch.alerting.util.DestinationType
import org.opensearch.alerting.util.IndexUtils
import org.opensearch.common.Strings
import org.opensearch.commons.notifications.action.CreateNotificationConfigRequest
import org.opensearch.commons.notifications.action.GetNotificationConfigRequest
import org.opensearch.commons.notifications.action.GetNotificationConfigResponse
import org.opensearch.commons.notifications.model.Chime
import org.opensearch.commons.notifications.model.ConfigType
import org.opensearch.commons.notifications.model.Email
import org.opensearch.commons.notifications.model.Feature
import org.opensearch.commons.notifications.model.NotificationConfig
import org.opensearch.commons.notifications.model.Slack
import org.opensearch.commons.notifications.model.Webhook
import org.opensearch.rest.RestStatus
import org.opensearch.search.sort.SortOrder
import java.net.URI
import java.net.URISyntaxException
import java.time.Instant
import java.util.*

class GetDestinationsConverter {

    companion object {
        fun convertGetDestinationsRequestToGetNotificationConfigRequest(request: GetDestinationsRequest): GetNotificationConfigRequest {
            val configIds: Set<String> = if(request.destinationId != null) setOf(request.destinationId) else emptySet()
            val table = request.table
            val fromIndex = table.startIndex
            val maxItems = table.size
            val sortField = table.sortString
            val sortOrder: SortOrder = SortOrder.fromString(table.sortOrder)
            val filterParams: Map<String, String> = emptyMap()

            return GetNotificationConfigRequest(configIds, fromIndex, maxItems, sortField, sortOrder, filterParams)
        }

        fun convertGetNotificationConfigResponseToGetDestinationsResponse(response: GetNotificationConfigResponse): GetDestinationsResponse {
            val searchResult = response.searchResult
            if (searchResult.totalHits == 0L) throw OpenSearchStatusException("Email Account not found.", RestStatus.NOT_FOUND)
            val destinations = emptyList<Destination>()
            searchResult.objectList.forEach {
                val notificationConfig = it.notificationConfig
                val destination = convertNotificationConfigToDestination(notificationConfig)
                if (destination != null) destinations.plus(destination)
            }
            return GetDestinationsResponse(RestStatus.OK, destinations.size, destinations)
        }

        fun convertIndexDestinationRequestToCreateNotificationConfigRequest(request: IndexDestinationRequest): CreateNotificationConfigRequest {
            val notificationConfig = convertDestinationToNotificationConfig(request.destination)
                ?: throw OpenSearchStatusException("Destination cannot be created.", RestStatus.NOT_FOUND)
            return CreateNotificationConfigRequest(notificationConfig, request.destinationId)
        }

        private fun convertNotificationConfigToDestination(notificationConfig: NotificationConfig): Destination? {
            when(notificationConfig.configType) {
                ConfigType.SLACK -> {
                    val slack = notificationConfig.configData as Slack
                    val alertSlack = org.opensearch.alerting.model.destination.Slack(slack.url)
                    return Destination(Destination.NO_ID, Destination.NO_VERSION, IndexUtils.NO_SCHEMA_VERSION, Destination.NO_SEQ_NO,
                        Destination.NO_PRIMARY_TERM, DestinationType.SLACK, notificationConfig.name, null, Instant.MIN, null, alertSlack, null, null)
                }
                ConfigType.CHIME -> {
                    val chime = notificationConfig.configData as Chime
                    val alertChime = org.opensearch.alerting.model.destination.Chime(chime.url)
                    return Destination(Destination.NO_ID, Destination.NO_VERSION, IndexUtils.NO_SCHEMA_VERSION, Destination.NO_SEQ_NO,
                        Destination.NO_PRIMARY_TERM, DestinationType.CHIME, notificationConfig.name, null, Instant.MIN, alertChime, null, null, null)
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
                    val alertWebhook = CustomWebhook(webhook.url, scheme ,host, port, path, method, emptyMap(), webhook.headerParams, username, password)
                    return Destination(Destination.NO_ID, Destination.NO_VERSION, IndexUtils.NO_SCHEMA_VERSION, Destination.NO_SEQ_NO,
                        Destination.NO_PRIMARY_TERM, DestinationType.CHIME, notificationConfig.name, null, Instant.MIN, null, null, alertWebhook, null)
                }
                ConfigType.EMAIL -> {
                    val email: Email = notificationConfig.configData as Email
                    val recipients = emptyList<Recipient>()
                    email.recipients.forEach {
                        val recipient = Recipient(Recipient.RecipientType.EMAIL, null, it)
                        recipients.plus(recipient)
                    }
                    email.emailGroupIds.forEach {
                        val recipient = Recipient(Recipient.RecipientType.EMAIL_GROUP, it, null)
                        recipients.plus(recipient)
                    }
                    val alertEmail = org.opensearch.alerting.model.destination.email.Email(email.emailAccountID, recipients)
                    return Destination(Destination.NO_ID, Destination.NO_VERSION, IndexUtils.NO_SCHEMA_VERSION, Destination.NO_SEQ_NO,
                        Destination.NO_PRIMARY_TERM, DestinationType.EMAIL, notificationConfig.name, null, Instant.MIN, null, null, null, alertEmail)
                }
                else -> return null
            }
        }

        private fun convertDestinationToNotificationConfig(destination: Destination): NotificationConfig? {
            when(destination.type) {
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
                    val slack = if (alertSlack == null) null else Slack(alertSlack!!.url)
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
                        val uri = buildUri(alertWebhook.url!!, alertWebhook.scheme!!, alertWebhook.host, alertWebhook.port, alertWebhook.path, alertWebhook.queryParams).toString()
                        webhook = Webhook(uri, alertWebhook.headerParams)
                        val description = "Webhook destination created from the Alerting plugin"
                        return NotificationConfig(
                            destination.name,
                            description,
                            ConfigType.WEBHOOK,
                            EnumSet.of(Feature.ALERTING),
                            webhook
                        )
                    }
                }
            }
            return null
        }

        fun buildUri(endpoint: String, scheme: String, host: String?,
                     port: Int, path: String?, queryParams: Map<String, String>): URI? {
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

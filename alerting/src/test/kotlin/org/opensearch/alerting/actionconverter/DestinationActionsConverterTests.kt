package org.opensearch.alerting.actionconverter

import org.opensearch.action.support.WriteRequest
import org.opensearch.alerting.action.GetDestinationsRequest
import org.opensearch.alerting.action.IndexDestinationRequest
import org.opensearch.alerting.actionconverter.DestinationActionsConverter.Companion.convertGetDestinationsRequestToGetNotificationConfigRequest
import org.opensearch.alerting.actionconverter.DestinationActionsConverter.Companion.convertGetNotificationConfigResponseToGetDestinationsResponse
import org.opensearch.alerting.actionconverter.DestinationActionsConverter.Companion.convertIndexDestinationRequestToCreateNotificationConfigRequest
import org.opensearch.alerting.actionconverter.DestinationActionsConverter.Companion.convertIndexDestinationRequestToUpdateNotificationConfigRequest
import org.opensearch.alerting.actionconverter.DestinationActionsConverter.Companion.convertToIndexDestinationResponse
import org.opensearch.alerting.getChimeDestination
import org.opensearch.alerting.getCustomWebhookDestination
import org.opensearch.alerting.getEmailDestination
import org.opensearch.alerting.getSlackDestination
import org.opensearch.alerting.model.Table
import org.opensearch.alerting.util.DestinationType
import org.opensearch.commons.notifications.NotificationConstants
import org.opensearch.commons.notifications.action.GetNotificationConfigResponse
import org.opensearch.commons.notifications.model.Chime
import org.opensearch.commons.notifications.model.ConfigType
import org.opensearch.commons.notifications.model.Email
import org.opensearch.commons.notifications.model.Feature
import org.opensearch.commons.notifications.model.NotificationConfig
import org.opensearch.commons.notifications.model.NotificationConfigInfo
import org.opensearch.commons.notifications.model.NotificationConfigSearchResult
import org.opensearch.commons.notifications.model.Slack
import org.opensearch.commons.notifications.model.Webhook
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestStatus
import org.opensearch.search.fetch.subphase.FetchSourceContext
import org.opensearch.search.sort.SortOrder
import org.opensearch.test.OpenSearchTestCase
import java.time.Instant
import java.util.EnumSet

class DestinationActionsConverterTests : OpenSearchTestCase() {

    fun `test convertGetDestinationsRequestToGetNotificationConfigRequest with single destination type`() {
        val table = Table("asc", "destination.name.keyword", null, 0, 0, "searchString")
        val getDestinationsRequest = GetDestinationsRequest(
            "destinationId",
            0L,
            FetchSourceContext.DO_NOT_FETCH_SOURCE,
            table,
            "chime"
        )
        val getNotificationConfigRequest = convertGetDestinationsRequestToGetNotificationConfigRequest(getDestinationsRequest)

        assertEquals(1, getNotificationConfigRequest.configIds.size)
        assertEquals(getDestinationsRequest.destinationId, getNotificationConfigRequest.configIds.elementAt(0))
        assertEquals(table.startIndex, getNotificationConfigRequest.fromIndex)
        assertEquals(table.size, getNotificationConfigRequest.maxItems)
        assertEquals(NotificationConstants.NAME_TAG, getNotificationConfigRequest.sortField)
        assertEquals(SortOrder.fromString(table.sortOrder), getNotificationConfigRequest.sortOrder)
        assertTrue(getNotificationConfigRequest.filterParams.containsKey("name"))
        assertEquals(table.searchString, getNotificationConfigRequest.filterParams["name"])
        assertTrue(getNotificationConfigRequest.filterParams.containsKey("config_type"))
        assertEquals("chime", getNotificationConfigRequest.filterParams["config_type"])
    }

    fun `test convertGetDestinationsRequestToGetNotificationConfigRequest with custom webhook`() {
        val table = Table("asc", "destination.name.keyword", null, 0, 0, null)
        val getDestinationsRequest = GetDestinationsRequest(
            "destinationId",
            0L,
            FetchSourceContext.DO_NOT_FETCH_SOURCE,
            table,
            "custom_webhook"
        )
        val getNotificationConfigRequest = convertGetDestinationsRequestToGetNotificationConfigRequest(getDestinationsRequest)

        assertEquals(1, getNotificationConfigRequest.configIds.size)
        assertEquals(getDestinationsRequest.destinationId, getNotificationConfigRequest.configIds.elementAt(0))
        assertEquals(table.startIndex, getNotificationConfigRequest.fromIndex)
        assertEquals(table.size, getNotificationConfigRequest.maxItems)
        assertEquals(NotificationConstants.NAME_TAG, getNotificationConfigRequest.sortField)
        assertEquals(SortOrder.fromString(table.sortOrder), getNotificationConfigRequest.sortOrder)
        assertFalse(getNotificationConfigRequest.filterParams.containsKey("name"))
        assertTrue(getNotificationConfigRequest.filterParams.containsKey("config_type"))
        assertEquals("webhook", getNotificationConfigRequest.filterParams["config_type"])
    }

    fun `test convertGetDestinationsRequestToGetNotificationConfigRequest with all destination types`() {
        val table = Table("asc", "destination.name.keyword", null, 0, 0, "searchString")
        val getDestinationsRequest = GetDestinationsRequest(
            "destinationId",
            0L,
            FetchSourceContext.DO_NOT_FETCH_SOURCE,
            table,
            "ALL"
        )
        val getNotificationConfigRequest = convertGetDestinationsRequestToGetNotificationConfigRequest(getDestinationsRequest)

        assertEquals(1, getNotificationConfigRequest.configIds.size)
        assertEquals(getDestinationsRequest.destinationId, getNotificationConfigRequest.configIds.elementAt(0))
        assertEquals(table.startIndex, getNotificationConfigRequest.fromIndex)
        assertEquals(table.size, getNotificationConfigRequest.maxItems)
        assertEquals(NotificationConstants.NAME_TAG, getNotificationConfigRequest.sortField)
        assertEquals(SortOrder.fromString(table.sortOrder), getNotificationConfigRequest.sortOrder)
        assertTrue(getNotificationConfigRequest.filterParams.containsKey("name"))
        assertEquals(table.searchString, getNotificationConfigRequest.filterParams["name"])
        assertTrue(getNotificationConfigRequest.filterParams.containsKey("config_type"))
        assertEquals("slack,chime,webhook,email", getNotificationConfigRequest.filterParams["config_type"])
    }

    fun `test convertGetDestinationsRequestToGetNotificationConfigRequest with sort by config type`() {
        val table = Table("asc", "destination.type", null, 0, 0, "searchString")
        val getDestinationsRequest = GetDestinationsRequest(
            "destinationId",
            0L,
            FetchSourceContext.DO_NOT_FETCH_SOURCE,
            table,
            "ALL"
        )
        val getNotificationConfigRequest = convertGetDestinationsRequestToGetNotificationConfigRequest(getDestinationsRequest)

        assertEquals(1, getNotificationConfigRequest.configIds.size)
        assertEquals(getDestinationsRequest.destinationId, getNotificationConfigRequest.configIds.elementAt(0))
        assertEquals(table.startIndex, getNotificationConfigRequest.fromIndex)
        assertEquals(table.size, getNotificationConfigRequest.maxItems)
        assertEquals(NotificationConstants.CONFIG_TYPE_TAG, getNotificationConfigRequest.sortField)
        assertEquals(SortOrder.fromString(table.sortOrder), getNotificationConfigRequest.sortOrder)
        assertTrue(getNotificationConfigRequest.filterParams.containsKey("name"))
        assertEquals(table.searchString, getNotificationConfigRequest.filterParams["name"])
        assertTrue(getNotificationConfigRequest.filterParams.containsKey("config_type"))
        assertEquals("slack,chime,webhook,email", getNotificationConfigRequest.filterParams["config_type"])
    }

    fun `test convertGetDestinationsRequestToGetNotificationConfigRequest with sort by last updated time`() {
        val table = Table("asc", "destination.last_update_time", null, 0, 0, "searchString")
        val getDestinationsRequest = GetDestinationsRequest(
            null,
            0L,
            FetchSourceContext.DO_NOT_FETCH_SOURCE,
            table,
            "ALL"
        )
        val getNotificationConfigRequest = convertGetDestinationsRequestToGetNotificationConfigRequest(getDestinationsRequest)

        assertTrue(getNotificationConfigRequest.configIds.isEmpty())
        assertEquals(table.startIndex, getNotificationConfigRequest.fromIndex)
        assertEquals(table.size, getNotificationConfigRequest.maxItems)
        assertEquals(NotificationConstants.UPDATED_TIME_TAG, getNotificationConfigRequest.sortField)
        assertEquals(SortOrder.fromString(table.sortOrder), getNotificationConfigRequest.sortOrder)
        assertTrue(getNotificationConfigRequest.filterParams.containsKey("name"))
        assertEquals(table.searchString, getNotificationConfigRequest.filterParams["name"])
        assertTrue(getNotificationConfigRequest.filterParams.containsKey("config_type"))
        assertEquals("slack,chime,webhook,email", getNotificationConfigRequest.filterParams["config_type"])
    }

    fun `test convertGetNotificationConfigResponseToGetDestinationsResponse with chime`() {
        val chime = Chime("https://hooks.chime.aws/incomingwebhooks/webhookId")
        val notificationConfig = NotificationConfig(
            "notificationConfig",
            "description",
            ConfigType.CHIME,
            EnumSet.of(Feature.ALERTING),
            chime,
            true
        )
        val notificationConfigInfo = NotificationConfigInfo("configId", Instant.now(), Instant.now(), "tenant", notificationConfig)
        val notificationConfigSearchResult = NotificationConfigSearchResult(notificationConfigInfo)
        val getNotificationConfigResponse = GetNotificationConfigResponse(notificationConfigSearchResult)
        val getDestinationsResponse = convertGetNotificationConfigResponseToGetDestinationsResponse(getNotificationConfigResponse)

        assertEquals(1, getDestinationsResponse.totalDestinations)
        assertEquals(RestStatus.OK, getDestinationsResponse.status)
        val destination = getDestinationsResponse.destinations[0]
        assertEquals(DestinationType.CHIME, destination.type)
        assertEquals(chime.url, destination.chime?.url)
        assertEquals(notificationConfig.name, destination.name)
        assertEquals(notificationConfigInfo.configId, destination.id)
    }

    fun `test convertGetNotificationConfigResponseToGetDestinationsResponse with slack`() {
        val slack = Slack("https://hooks.slack.com/services/slackId")
        val notificationConfig = NotificationConfig(
            "notificationConfig",
            "description",
            ConfigType.SLACK,
            EnumSet.of(Feature.ALERTING),
            slack,
            true
        )
        val notificationConfigInfo = NotificationConfigInfo("configId", Instant.now(), Instant.now(), "tenant", notificationConfig)
        val notificationConfigSearchResult = NotificationConfigSearchResult(notificationConfigInfo)
        val getNotificationConfigResponse = GetNotificationConfigResponse(notificationConfigSearchResult)
        val getDestinationsResponse = convertGetNotificationConfigResponseToGetDestinationsResponse(getNotificationConfigResponse)

        assertEquals(1, getDestinationsResponse.totalDestinations)
        assertEquals(RestStatus.OK, getDestinationsResponse.status)
        val destination = getDestinationsResponse.destinations[0]
        assertEquals(DestinationType.SLACK, destination.type)
        assertEquals(slack.url, destination.slack?.url)
        assertEquals(notificationConfig.name, destination.name)
        assertEquals(notificationConfigInfo.configId, destination.id)
    }

    fun `test convertGetNotificationConfigResponseToGetDestinationsResponse with email`() {
        val email = Email("accountId", listOf("test@email.com"), emptyList())
        val notificationConfig = NotificationConfig(
            "notificationConfig",
            "description",
            ConfigType.EMAIL,
            EnumSet.of(Feature.ALERTING),
            email,
            true
        )
        val notificationConfigInfo = NotificationConfigInfo("configId", Instant.now(), Instant.now(), "tenant", notificationConfig)
        val notificationConfigSearchResult = NotificationConfigSearchResult(notificationConfigInfo)
        val getNotificationConfigResponse = GetNotificationConfigResponse(notificationConfigSearchResult)
        val getDestinationsResponse = convertGetNotificationConfigResponseToGetDestinationsResponse(getNotificationConfigResponse)

        assertEquals(1, getDestinationsResponse.totalDestinations)
        assertEquals(RestStatus.OK, getDestinationsResponse.status)
        val destination = getDestinationsResponse.destinations[0]
        assertEquals(DestinationType.EMAIL, destination.type)
        assertEquals(email.emailAccountID, destination.email?.emailAccountID)
        assertEquals(1, destination.email?.recipients?.size)
        assertEquals("test@email.com", destination.email?.recipients?.get(0)?.email)
        assertEquals(notificationConfig.name, destination.name)
        assertEquals(notificationConfigInfo.configId, destination.id)
    }

    fun `test convertGetNotificationConfigResponseToGetDestinationsResponse with webhook`() {
        val webhook = Webhook("https://hooks.slack.com/services/slackId")
        val notificationConfig = NotificationConfig(
            "notificationConfig",
            "description",
            ConfigType.WEBHOOK,
            EnumSet.of(Feature.ALERTING),
            webhook,
            true
        )
        val notificationConfigInfo = NotificationConfigInfo("configId", Instant.now(), Instant.now(), "tenant", notificationConfig)
        val notificationConfigSearchResult = NotificationConfigSearchResult(notificationConfigInfo)
        val getNotificationConfigResponse = GetNotificationConfigResponse(notificationConfigSearchResult)
        val getDestinationsResponse = convertGetNotificationConfigResponseToGetDestinationsResponse(getNotificationConfigResponse)

        assertEquals(1, getDestinationsResponse.totalDestinations)
        assertEquals(RestStatus.OK, getDestinationsResponse.status)
        val destination = getDestinationsResponse.destinations[0]
        assertEquals(DestinationType.CUSTOM_WEBHOOK, destination.type)
        assertEquals(webhook.url, destination.customWebhook?.url)
        assertEquals(notificationConfig.name, destination.name)
        assertEquals(notificationConfigInfo.configId, destination.id)
    }

    fun `test convertIndexDestinationRequestToCreateNotificationConfigRequest with webhook`() {
        val customWebhook = getCustomWebhookDestination()
        val indexDestinationRequest = IndexDestinationRequest(
            "destinationId",
            0L,
            0L,
            WriteRequest.RefreshPolicy.NONE,
            RestRequest.Method.POST,
            customWebhook
        )

        val createNotificationConfigRequest = convertIndexDestinationRequestToCreateNotificationConfigRequest(indexDestinationRequest)

        assertEquals(indexDestinationRequest.destinationId, createNotificationConfigRequest.configId)
        val notificationConfig = createNotificationConfigRequest.notificationConfig
        assertEquals(customWebhook.name, notificationConfig.name)
        assertEquals(ConfigType.WEBHOOK, notificationConfig.configType)
        assertEquals("Webhook destination created from the Alerting plugin", notificationConfig.description)
        assertEquals(EnumSet.of(Feature.ALERTING), notificationConfig.features)
        val webhook = notificationConfig.configData as Webhook
        assertEquals(customWebhook.customWebhook?.url, webhook.url)
    }

    fun `test convertIndexDestinationRequestToCreateNotificationConfigRequest with slack`() {
        val slack = getSlackDestination()
        val indexDestinationRequest = IndexDestinationRequest(
            "destinationId",
            0L,
            0L,
            WriteRequest.RefreshPolicy.NONE,
            RestRequest.Method.POST,
            slack
        )

        val createNotificationConfigRequest = convertIndexDestinationRequestToCreateNotificationConfigRequest(indexDestinationRequest)

        assertEquals(indexDestinationRequest.destinationId, createNotificationConfigRequest.configId)
        val notificationConfig = createNotificationConfigRequest.notificationConfig
        assertEquals(slack.name, notificationConfig.name)
        assertEquals(ConfigType.SLACK, notificationConfig.configType)
        assertEquals("Slack destination created from the Alerting plugin", notificationConfig.description)
        assertEquals(EnumSet.of(Feature.ALERTING), notificationConfig.features)
        val notifSlack = notificationConfig.configData as Slack
        assertEquals(slack.slack?.url, notifSlack.url)
    }

    fun `test convertIndexDestinationRequestToCreateNotificationConfigRequest with chime`() {
        val chime = getChimeDestination()
        val indexDestinationRequest = IndexDestinationRequest(
            "destinationId",
            0L,
            0L,
            WriteRequest.RefreshPolicy.NONE,
            RestRequest.Method.POST,
            chime
        )

        val createNotificationConfigRequest = convertIndexDestinationRequestToCreateNotificationConfigRequest(indexDestinationRequest)

        assertEquals(indexDestinationRequest.destinationId, createNotificationConfigRequest.configId)
        val notificationConfig = createNotificationConfigRequest.notificationConfig
        assertEquals(chime.name, notificationConfig.name)
        assertEquals(ConfigType.CHIME, notificationConfig.configType)
        assertEquals("Chime destination created from the Alerting plugin", notificationConfig.description)
        assertEquals(EnumSet.of(Feature.ALERTING), notificationConfig.features)
        val notifChime = notificationConfig.configData as Chime
        assertEquals(chime.chime?.url, notifChime.url)
    }

    fun `test convertIndexDestinationRequestToCreateNotificationConfigRequest with email`() {
        val email = getEmailDestination()
        val indexDestinationRequest = IndexDestinationRequest(
            "destinationId",
            0L,
            0L,
            WriteRequest.RefreshPolicy.NONE,
            RestRequest.Method.POST,
            email
        )

        val createNotificationConfigRequest = convertIndexDestinationRequestToCreateNotificationConfigRequest(indexDestinationRequest)

        assertEquals(indexDestinationRequest.destinationId, createNotificationConfigRequest.configId)
        val notificationConfig = createNotificationConfigRequest.notificationConfig
        assertEquals(email.name, notificationConfig.name)
        assertEquals(ConfigType.EMAIL, notificationConfig.configType)
        assertEquals("Email destination created from the Alerting plugin", notificationConfig.description)
        assertEquals(EnumSet.of(Feature.ALERTING), notificationConfig.features)
        val notifEmail = notificationConfig.configData as Email
        assertEquals(email.email?.emailAccountID, notifEmail.emailAccountID)
        assertEquals(email.email?.recipients?.get(0)?.email, notifEmail.recipients[0])
    }

    fun `test convertIndexDestinationRequestToUpdateNotificationConfigRequest with webhook`() {
        val customWebhook = getCustomWebhookDestination()
        val indexDestinationRequest = IndexDestinationRequest(
            "destinationId",
            0L,
            0L,
            WriteRequest.RefreshPolicy.NONE,
            RestRequest.Method.POST,
            customWebhook
        )

        val updateNotificationConfigRequest = convertIndexDestinationRequestToUpdateNotificationConfigRequest(indexDestinationRequest)

        assertEquals(indexDestinationRequest.destinationId, updateNotificationConfigRequest.configId)
        val notificationConfig = updateNotificationConfigRequest.notificationConfig
        assertEquals(customWebhook.name, notificationConfig.name)
        assertEquals(ConfigType.WEBHOOK, notificationConfig.configType)
        assertEquals("Webhook destination created from the Alerting plugin", notificationConfig.description)
        assertEquals(EnumSet.of(Feature.ALERTING), notificationConfig.features)
        val webhook = notificationConfig.configData as Webhook
        assertEquals(customWebhook.customWebhook?.url, webhook.url)
    }

    fun `test convertIndexDestinationRequestToUpdateNotificationConfigRequest with slack`() {
        val slack = getSlackDestination()
        val indexDestinationRequest = IndexDestinationRequest(
            "destinationId",
            0L,
            0L,
            WriteRequest.RefreshPolicy.NONE,
            RestRequest.Method.POST,
            slack
        )

        val updateNotificationConfigRequest = convertIndexDestinationRequestToUpdateNotificationConfigRequest(indexDestinationRequest)

        assertEquals(indexDestinationRequest.destinationId, updateNotificationConfigRequest.configId)
        val notificationConfig = updateNotificationConfigRequest.notificationConfig
        assertEquals(slack.name, notificationConfig.name)
        assertEquals(ConfigType.SLACK, notificationConfig.configType)
        assertEquals("Slack destination created from the Alerting plugin", notificationConfig.description)
        assertEquals(EnumSet.of(Feature.ALERTING), notificationConfig.features)
        val notifSlack = notificationConfig.configData as Slack
        assertEquals(slack.slack?.url, notifSlack.url)
    }

    fun `test convertIndexDestinationRequestToUpdateNotificationConfigRequest with chime`() {
        val chime = getChimeDestination()
        val indexDestinationRequest = IndexDestinationRequest(
            "destinationId",
            0L,
            0L,
            WriteRequest.RefreshPolicy.NONE,
            RestRequest.Method.POST,
            chime
        )

        val updateNotificationConfigRequest = convertIndexDestinationRequestToUpdateNotificationConfigRequest(indexDestinationRequest)

        assertEquals(indexDestinationRequest.destinationId, updateNotificationConfigRequest.configId)
        val notificationConfig = updateNotificationConfigRequest.notificationConfig
        assertEquals(chime.name, notificationConfig.name)
        assertEquals(ConfigType.CHIME, notificationConfig.configType)
        assertEquals("Chime destination created from the Alerting plugin", notificationConfig.description)
        assertEquals(EnumSet.of(Feature.ALERTING), notificationConfig.features)
        val notifChime = notificationConfig.configData as Chime
        assertEquals(chime.chime?.url, notifChime.url)
    }

    fun `test convertIndexDestinationRequestToUpdateNotificationConfigRequest with email`() {
        val email = getEmailDestination()
        val indexDestinationRequest = IndexDestinationRequest(
            "destinationId",
            0L,
            0L,
            WriteRequest.RefreshPolicy.NONE,
            RestRequest.Method.POST,
            email
        )

        val updateNotificationConfigRequest = convertIndexDestinationRequestToUpdateNotificationConfigRequest(indexDestinationRequest)

        assertEquals(indexDestinationRequest.destinationId, updateNotificationConfigRequest.configId)
        val notificationConfig = updateNotificationConfigRequest.notificationConfig
        assertEquals(email.name, notificationConfig.name)
        assertEquals(ConfigType.EMAIL, notificationConfig.configType)
        assertEquals("Email destination created from the Alerting plugin", notificationConfig.description)
        assertEquals(EnumSet.of(Feature.ALERTING), notificationConfig.features)
        val notifEmail = notificationConfig.configData as Email
        assertEquals(email.email?.emailAccountID, notifEmail.emailAccountID)
        assertEquals(email.email?.recipients?.get(0)?.email, notifEmail.recipients[0])
    }

    fun `test convertToIndexDestinationResponse with slack`() {
        val slack = Slack("https://hooks.slack.com/services/slackId")
        val notificationConfig = NotificationConfig(
            "notificationConfig",
            "description",
            ConfigType.SLACK,
            EnumSet.of(Feature.ALERTING),
            slack,
            true
        )
        val notificationConfigInfo = NotificationConfigInfo("configId", Instant.now(), Instant.now(), "tenant", notificationConfig)
        val notificationConfigSearchResult = NotificationConfigSearchResult(notificationConfigInfo)
        val getNotificationConfigResponse = GetNotificationConfigResponse(notificationConfigSearchResult)
        val indexDestinationResponse = convertToIndexDestinationResponse("configId", getNotificationConfigResponse)

        assertEquals("configId", indexDestinationResponse.id)
        assertEquals(RestStatus.OK, indexDestinationResponse.status)
        assertEquals("configId", indexDestinationResponse.destination.id)
        assertEquals(slack.url, indexDestinationResponse.destination.slack?.url)
    }

    fun `test convertToIndexDestinationResponse with chime`() {
        val chime = Chime("https://hooks.slack.com/services/slackId")
        val notificationConfig = NotificationConfig(
            "notificationConfig",
            "description",
            ConfigType.CHIME,
            EnumSet.of(Feature.ALERTING),
            chime,
            true
        )
        val notificationConfigInfo = NotificationConfigInfo("configId", Instant.now(), Instant.now(), "tenant", notificationConfig)
        val notificationConfigSearchResult = NotificationConfigSearchResult(notificationConfigInfo)
        val getNotificationConfigResponse = GetNotificationConfigResponse(notificationConfigSearchResult)
        val indexDestinationResponse = convertToIndexDestinationResponse("configId", getNotificationConfigResponse)

        assertEquals("configId", indexDestinationResponse.id)
        assertEquals(RestStatus.OK, indexDestinationResponse.status)
        assertEquals("configId", indexDestinationResponse.destination.id)
        assertEquals(chime.url, indexDestinationResponse.destination.chime?.url)
    }

    fun `test convertToIndexDestinationResponse with webhook`() {
        val webhook = Webhook("https://hooks.slack.com/services/slackId")
        val notificationConfig = NotificationConfig(
            "notificationConfig",
            "description",
            ConfigType.WEBHOOK,
            EnumSet.of(Feature.ALERTING),
            webhook,
            true
        )
        val notificationConfigInfo = NotificationConfigInfo("configId", Instant.now(), Instant.now(), "tenant", notificationConfig)
        val notificationConfigSearchResult = NotificationConfigSearchResult(notificationConfigInfo)
        val getNotificationConfigResponse = GetNotificationConfigResponse(notificationConfigSearchResult)
        val indexDestinationResponse = convertToIndexDestinationResponse("configId", getNotificationConfigResponse)

        assertEquals("configId", indexDestinationResponse.id)
        assertEquals(RestStatus.OK, indexDestinationResponse.status)
        assertEquals("configId", indexDestinationResponse.destination.id)
        assertEquals(webhook.url, indexDestinationResponse.destination.customWebhook?.url)
        assertEquals(-1, indexDestinationResponse.destination.customWebhook?.port)
    }

    fun `test convertToIndexDestinationResponse with email`() {
        val email = Email("accountId", listOf("test@email.com"), emptyList())
        val notificationConfig = NotificationConfig(
            "notificationConfig",
            "description",
            ConfigType.EMAIL,
            EnumSet.of(Feature.ALERTING),
            email,
            true
        )
        val notificationConfigInfo = NotificationConfigInfo("configId", Instant.now(), Instant.now(), "tenant", notificationConfig)
        val notificationConfigSearchResult = NotificationConfigSearchResult(notificationConfigInfo)
        val getNotificationConfigResponse = GetNotificationConfigResponse(notificationConfigSearchResult)
        val indexDestinationResponse = convertToIndexDestinationResponse("configId", getNotificationConfigResponse)

        assertEquals("configId", indexDestinationResponse.id)
        assertEquals(RestStatus.OK, indexDestinationResponse.status)
        assertEquals("configId", indexDestinationResponse.destination.id)
        assertEquals(email.emailAccountID, indexDestinationResponse.destination.email?.emailAccountID)
        assertEquals(email.recipients[0], indexDestinationResponse.destination.email?.recipients?.get(0)?.email)
    }
}

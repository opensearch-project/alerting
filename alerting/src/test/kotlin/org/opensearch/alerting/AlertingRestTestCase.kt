/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.apache.http.HttpEntity
import org.apache.http.HttpHeaders
import org.apache.http.entity.ContentType
import org.apache.http.entity.ContentType.APPLICATION_JSON
import org.apache.http.entity.StringEntity
import org.apache.http.message.BasicHeader
import org.junit.AfterClass
import org.junit.rules.DisableOnDebug
import org.opensearch.action.search.SearchResponse
import org.opensearch.alerting.AlertingPlugin.Companion.EMAIL_ACCOUNT_BASE_URI
import org.opensearch.alerting.AlertingPlugin.Companion.EMAIL_GROUP_BASE_URI
import org.opensearch.alerting.alerts.AlertIndices
import org.opensearch.alerting.alerts.AlertIndices.Companion.FINDING_HISTORY_WRITE_INDEX
import org.opensearch.alerting.core.settings.ScheduledJobSettings
import org.opensearch.alerting.model.destination.Chime
import org.opensearch.alerting.model.destination.CustomWebhook
import org.opensearch.alerting.model.destination.Destination
import org.opensearch.alerting.model.destination.Slack
import org.opensearch.alerting.model.destination.email.EmailAccount
import org.opensearch.alerting.model.destination.email.EmailGroup
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.settings.DestinationSettings
import org.opensearch.alerting.util.DestinationType
import org.opensearch.client.Request
import org.opensearch.client.Response
import org.opensearch.client.RestClient
import org.opensearch.client.WarningFailureException
import org.opensearch.common.UUIDs
import org.opensearch.common.io.PathUtils
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentFactory.jsonBuilder
import org.opensearch.common.xcontent.XContentType
import org.opensearch.common.xcontent.json.JsonXContent.jsonXContent
import org.opensearch.commons.alerting.action.GetFindingsResponse
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.commons.alerting.model.BucketLevelTrigger
import org.opensearch.commons.alerting.model.ChainedAlertTrigger
import org.opensearch.commons.alerting.model.DocLevelMonitorInput
import org.opensearch.commons.alerting.model.DocLevelQuery
import org.opensearch.commons.alerting.model.DocumentLevelTrigger
import org.opensearch.commons.alerting.model.Finding
import org.opensearch.commons.alerting.model.FindingWithDocs
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.QueryLevelTrigger
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.alerting.model.SearchInput
import org.opensearch.commons.alerting.model.Workflow
import org.opensearch.commons.alerting.util.string
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParserUtils
import org.opensearch.search.SearchModule
import java.net.URLEncoder
import java.nio.file.Files
import java.time.Instant
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.Locale
import java.util.UUID
import java.util.stream.Collectors
import javax.management.MBeanServerInvocationHandler
import javax.management.ObjectName
import javax.management.remote.JMXConnectorFactory
import javax.management.remote.JMXServiceURL

/**
 * Superclass for tests that interact with an external test cluster using OpenSearch's RestClient
 */
abstract class AlertingRestTestCase : ODFERestTestCase() {

    protected val password = "D%LMX3bo#@U3XqVQ"

    protected val isDebuggingTest = DisableOnDebug(null).isDebugging
    protected val isDebuggingRemoteCluster = System.getProperty("cluster.debug", "false")!!.toBoolean()
    protected val numberOfNodes = System.getProperty("cluster.number_of_nodes", "1")!!.toInt()
    protected val isMultiNode = numberOfNodes > 1

    protected val statsResponseOpendistroSweeperEnabledField = "opendistro.scheduled_jobs.enabled"
    protected val statsResponseOpenSearchSweeperEnabledField = "plugins.scheduled_jobs.enabled"

    override fun xContentRegistry(): NamedXContentRegistry {
        return NamedXContentRegistry(
            mutableListOf(
                Monitor.XCONTENT_REGISTRY,
                SearchInput.XCONTENT_REGISTRY,
                DocLevelMonitorInput.XCONTENT_REGISTRY,
                QueryLevelTrigger.XCONTENT_REGISTRY,
                BucketLevelTrigger.XCONTENT_REGISTRY,
                DocumentLevelTrigger.XCONTENT_REGISTRY,
                Workflow.XCONTENT_REGISTRY,
                ChainedAlertTrigger.XCONTENT_REGISTRY
            ) + SearchModule(Settings.EMPTY, emptyList()).namedXContents
        )
    }

    fun Response.asMap(): Map<String, Any> {
        return entityAsMap(this)
    }

    private fun createMonitorEntityWithBackendRoles(monitor: Monitor, rbacRoles: List<String>?): HttpEntity {
        if (rbacRoles == null) {
            return monitor.toHttpEntity()
        }
        val temp = monitor.toJsonString()
        val toReplace = temp.lastIndexOf("}")
        val rbacString = rbacRoles.joinToString { "\"$it\"" }
        val jsonString = temp.substring(0, toReplace) + ", \"rbac_roles\": [$rbacString] }"
        return StringEntity(jsonString, APPLICATION_JSON)
    }

    protected fun createMonitorWithClient(
        client: RestClient,
        monitor: Monitor,
        rbacRoles: List<String>? = null,
        refresh: Boolean = true,
    ): Monitor {
        val response = client.makeRequest(
            "POST",
            "$ALERTING_BASE_URI?refresh=$refresh",
            emptyMap(),
            createMonitorEntityWithBackendRoles(monitor, rbacRoles)
        )
        assertEquals("Unable to create a new monitor", RestStatus.CREATED, response.restStatus())

        val monitorJson = jsonXContent.createParser(
            NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE,
            response.entity.content
        ).map()
        assertUserNull(monitorJson as HashMap<String, Any>)

        return getMonitor(monitorId = monitorJson["_id"] as String)
    }

    protected fun createMonitor(monitor: Monitor, refresh: Boolean = true): Monitor {
        return createMonitorWithClient(client(), monitor, emptyList(), refresh)
    }

    protected fun deleteMonitor(monitor: Monitor, refresh: Boolean = true): Response {
        val response = client().makeRequest(
            "DELETE",
            "$ALERTING_BASE_URI/${monitor.id}?refresh=$refresh",
            emptyMap(),
            monitor.toHttpEntity()
        )
        assertEquals("Unable to delete a monitor", RestStatus.OK, response.restStatus())

        return response
    }

    protected fun deleteWorkflow(workflow: Workflow, deleteDelegates: Boolean = false, refresh: Boolean = true): Response {
        val response = client().makeRequest(
            "DELETE",
            "$WORKFLOW_ALERTING_BASE_URI/${workflow.id}?refresh=$refresh&deleteDelegateMonitors=$deleteDelegates",
            emptyMap(),
            workflow.toHttpEntity()
        )
        assertEquals("Unable to delete a workflow", RestStatus.OK, response.restStatus())
        return response
    }

    protected fun deleteWorkflowWithClient(
        client: RestClient,
        workflow: Workflow,
        deleteDelegates: Boolean = false,
        refresh: Boolean = true,
    ): Response {
        val response = client.makeRequest(
            "DELETE",
            "$WORKFLOW_ALERTING_BASE_URI/${workflow.id}?refresh=$refresh&deleteDelegateMonitors=$deleteDelegates",
            emptyMap(),
            workflow.toHttpEntity()
        )
        assertEquals("Unable to delete a workflow", RestStatus.OK, response.restStatus())

        return response
    }

    /**
     * Destinations are now deprecated in favor of the Notification plugin's configs.
     * This method should only be used for checking legacy behavior/Notification migration scenarios.
     */
    protected fun createDestination(destination: Destination = getTestDestination(), refresh: Boolean = true): Destination {
        // Create Alerting config index if it doesn't exist to avoid mapping issues with legacy destination indexing
        createAlertingConfigIndex()

        val response = indexDocWithAdminClient(
            ScheduledJob.SCHEDULED_JOBS_INDEX,
            UUIDs.base64UUID(),
            destination.toJsonStringWithType(),
            refresh
        )
        val destinationJson = jsonXContent.createParser(
            NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE,
            response.entity.content
        ).map()

        return destination.copy(
            id = destinationJson["_id"] as String,
            version = (destinationJson["_version"] as Int).toLong(),
            primaryTerm = destinationJson["_primary_term"] as Int
        )
    }

    protected fun deleteDestination(destination: Destination = getTestDestination(), refresh: Boolean = true): Response {
        val response = client().makeRequest(
            "DELETE",
            "$DESTINATION_BASE_URI/${destination.id}?refresh=$refresh",
            emptyMap(),
            destination.toHttpEntity()
        )
        assertEquals("Unable to delete destination", RestStatus.OK, response.restStatus())

        return response
    }

    protected fun updateDestination(destination: Destination, refresh: Boolean = true): Destination {
        val response = client().makeRequest(
            "PUT",
            "$DESTINATION_BASE_URI/${destination.id}?refresh=$refresh",
            emptyMap(),
            destination.toHttpEntity()
        )
        assertEquals("Unable to update a destination", RestStatus.OK, response.restStatus())
        val destinationJson = jsonXContent.createParser(
            NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE,
            response.entity.content
        ).map()
        assertUserNull(destinationJson as HashMap<String, Any>)

        return destination.copy(id = destinationJson["_id"] as String, version = (destinationJson["_version"] as Int).toLong())
    }

    protected fun getEmailAccount(
        emailAccountID: String,
        header: BasicHeader = BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json"),
    ): EmailAccount {
        val response = client().makeRequest("GET", "$EMAIL_ACCOUNT_BASE_URI/$emailAccountID", null, header)
        assertEquals("Unable to get email account $emailAccountID", RestStatus.OK, response.restStatus())

        val parser = createParser(XContentType.JSON.xContent(), response.entity.content)
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser)

        lateinit var id: String
        var version: Long = 0
        lateinit var emailAccount: EmailAccount

        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            parser.nextToken()

            when (parser.currentName()) {
                "_id" -> id = parser.text()
                "_version" -> version = parser.longValue()
                "email_account" -> emailAccount = EmailAccount.parse(parser)
            }
        }

        return emailAccount.copy(id = id, version = version)
    }

    /**
     * Email Accounts are now deprecated in favor of the Notification plugin's configs.
     * This method should only be used for checking legacy behavior/Notification migration scenarios.
     */
    protected fun createEmailAccount(emailAccount: EmailAccount = getTestEmailAccount(), refresh: Boolean = true): EmailAccount {
        // Create Alerting config index if it doesn't exist to avoid mapping issues with legacy destination indexing
        createAlertingConfigIndex()

        val response = indexDocWithAdminClient(
            ScheduledJob.SCHEDULED_JOBS_INDEX,
            UUIDs.base64UUID(),
            emailAccount.toJsonStringWithType(),
            refresh
        )
        val emailAccountJson = jsonXContent.createParser(
            NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE,
            response.entity.content
        ).map()
        return emailAccount.copy(id = emailAccountJson["_id"] as String)
    }

    protected fun createRandomEmailAccount(refresh: Boolean = true): EmailAccount {
        val emailAccount = randomEmailAccount()
        val emailAccountID = createEmailAccount(emailAccount, refresh).id
        return getEmailAccount(emailAccountID = emailAccountID)
    }

    protected fun createRandomEmailAccountWithGivenName(refresh: Boolean = true, randomName: String): EmailAccount {
        val emailAccount = randomEmailAccount(salt = randomName)
        val emailAccountID = createEmailAccount(emailAccount, refresh).id
        return getEmailAccount(emailAccountID = emailAccountID)
    }

    protected fun getEmailGroup(
        emailGroupID: String,
        header: BasicHeader = BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json"),
    ): EmailGroup {
        val response = client().makeRequest("GET", "$EMAIL_GROUP_BASE_URI/$emailGroupID", null, header)
        assertEquals("Unable to get email group $emailGroupID", RestStatus.OK, response.restStatus())

        val parser = createParser(XContentType.JSON.xContent(), response.entity.content)
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser)

        lateinit var id: String
        var version: Long = 0
        lateinit var emailGroup: EmailGroup

        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            parser.nextToken()

            when (parser.currentName()) {
                "_id" -> id = parser.text()
                "_version" -> version = parser.longValue()
                "email_group" -> emailGroup = EmailGroup.parse(parser)
            }
        }

        return emailGroup.copy(id = id, version = version)
    }

    /**
     * Email Groups are now deprecated in favor of the Notification plugin's configs.
     * This method should only be used for checking legacy behavior/Notification migration scenarios.
     */
    protected fun createEmailGroup(emailGroup: EmailGroup = getTestEmailGroup(), refresh: Boolean = true): EmailGroup {
        // Create Alerting config index if it doesn't exist to avoid mapping issues with legacy destination indexing
        createAlertingConfigIndex()

        val response = indexDocWithAdminClient(
            ScheduledJob.SCHEDULED_JOBS_INDEX,
            UUIDs.base64UUID(),
            emailGroup.toJsonStringWithType(),
            refresh
        )
        val emailGroupJson = jsonXContent.createParser(
            NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE,
            response.entity.content
        ).map()
        return emailGroup.copy(id = emailGroupJson["_id"] as String)
    }

    protected fun createRandomEmailGroup(refresh: Boolean = true): EmailGroup {
        val emailGroup = randomEmailGroup()
        val emailGroupID = createEmailGroup(emailGroup, refresh).id
        return getEmailGroup(emailGroupID = emailGroupID)
    }

    protected fun createRandomEmailGroupWithGivenName(refresh: Boolean = true, randomName: String): EmailGroup {
        val emailGroup = randomEmailGroup(salt = randomName)
        val emailGroupID = createEmailGroup(emailGroup, refresh).id
        return getEmailGroup(emailGroupID = emailGroupID)
    }

    @Suppress("UNCHECKED_CAST")
    protected fun getDestination(destination: Destination): Map<String, Any> {
        val response = client().makeRequest(
            "GET",
            "$DESTINATION_BASE_URI/${destination.id}"
        )
        assertEquals("Unable to update a destination", RestStatus.OK, response.restStatus())
        val destinationJson = jsonXContent.createParser(
            NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE,
            response.entity.content
        ).map()
        assertUserNull(destinationJson as HashMap<String, Any>)
        return (destinationJson["destinations"] as List<Any?>)[0] as Map<String, Any>
    }

    @Suppress("UNCHECKED_CAST")
    protected fun getDestinations(dataMap: Map<String, Any> = emptyMap()): List<Map<String, Any>> {
        return getDestinations(client(), dataMap)
    }

    @Suppress("UNCHECKED_CAST")
    protected fun getDestinations(
        client: RestClient,
        dataMap: Map<String, Any> = emptyMap(),
        header: BasicHeader = BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json"),
    ): List<Map<String, Any>> {
        var baseEndpoint = "$DESTINATION_BASE_URI?"
        for (entry in dataMap.entries) {
            baseEndpoint += "${entry.key}=${entry.value}&"
        }

        val response = client.makeRequest(
            "GET",
            baseEndpoint,
            null,
            header
        )
        assertEquals("Unable to update a destination", RestStatus.OK, response.restStatus())
        val destinationJson = jsonXContent.createParser(
            NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE,
            response.entity.content
        ).map()
        return destinationJson["destinations"] as List<Map<String, Any>>
    }

    protected fun getTestDestination(): Destination {
        return Destination(
            type = DestinationType.TEST_ACTION,
            name = "test",
            user = randomUser(),
            lastUpdateTime = Instant.now(),
            chime = null,
            slack = null,
            customWebhook = null,
            email = null
        )
    }

    fun getSlackDestination(): Destination {
        val slack = Slack("https://hooks.slack.com/services/slackId")
        return Destination(
            type = DestinationType.SLACK,
            name = "test",
            user = randomUser(),
            lastUpdateTime = Instant.now(),
            chime = null,
            slack = slack,
            customWebhook = null,
            email = null
        )
    }

    fun getChimeDestination(): Destination {
        val chime = Chime("https://hooks.chime.aws/incomingwebhooks/chimeId")
        return Destination(
            type = DestinationType.CHIME,
            name = "test",
            user = randomUser(),
            lastUpdateTime = Instant.now(),
            chime = chime,
            slack = null,
            customWebhook = null,
            email = null
        )
    }

    fun getCustomWebhookDestination(): Destination {
        val customWebhook = CustomWebhook(
            "https://hooks.slack.com/services/customWebhookId",
            null,
            null,
            80,
            null,
            null,
            emptyMap(),
            emptyMap(),
            null,
            null
        )
        return Destination(
            type = DestinationType.CUSTOM_WEBHOOK,
            name = "test",
            user = randomUser(),
            lastUpdateTime = Instant.now(),
            chime = null,
            slack = null,
            customWebhook = customWebhook,
            email = null
        )
    }

    private fun getTestEmailAccount(): EmailAccount {
        return EmailAccount(
            name = "test",
            email = "test@email.com",
            host = "smtp.com",
            port = 25,
            method = EmailAccount.MethodType.NONE,
            username = null,
            password = null
        )
    }

    private fun getTestEmailGroup(): EmailGroup {
        return EmailGroup(
            name = "test",
            emails = listOf()
        )
    }

    protected fun verifyIndexSchemaVersion(index: String, expectedVersion: Int) {
        val indexMapping = client().getIndexMapping(index)
        val indexName = indexMapping.keys.toList()[0]
        val mappings = indexMapping.stringMap(indexName)?.stringMap("mappings")
        var version = 0
        if (mappings!!.containsKey("_meta")) {
            val meta = mappings.stringMap("_meta")
            if (meta!!.containsKey("schema_version")) version = meta.get("schema_version") as Int
        }
        assertEquals(expectedVersion, version)
    }

    protected fun createAlert(alert: Alert): Alert {
        val response = adminClient().makeRequest(
            "POST",
            "/${AlertIndices.ALERT_INDEX}/_doc?refresh=true&routing=${alert.monitorId}",
            emptyMap(),
            alert.toHttpEntityWithUser()
        )
        assertEquals("Unable to create a new alert", RestStatus.CREATED, response.restStatus())

        val alertJson = jsonXContent.createParser(
            NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE,
            response.entity.content
        ).map()

        assertNull(alertJson["monitor_user"])
        return alert.copy(id = alertJson["_id"] as String, version = (alertJson["_version"] as Int).toLong())
    }

    protected fun createRandomMonitor(refresh: Boolean = false, withMetadata: Boolean = false): Monitor {
        val monitor = randomQueryLevelMonitor(withMetadata = withMetadata)
        val monitorId = createMonitor(monitor, refresh).id
        if (withMetadata) {
            return getMonitor(monitorId = monitorId, header = BasicHeader(HttpHeaders.USER_AGENT, "OpenSearch-Dashboards"))
        }
        return getMonitor(monitorId = monitorId)
    }

    protected fun createRandomDocumentMonitor(refresh: Boolean = false, withMetadata: Boolean = false): Monitor {
        val monitor = randomDocumentLevelMonitor(withMetadata = withMetadata)
        val monitorId = createMonitor(monitor, refresh).id
        if (withMetadata) {
            return getMonitor(monitorId = monitorId, header = BasicHeader(HttpHeaders.USER_AGENT, "OpenSearch-Dashboards"))
        }
        return getMonitor(monitorId = monitorId)
    }

    @Suppress("UNCHECKED_CAST")
    protected fun updateMonitor(monitor: Monitor, refresh: Boolean = false): Monitor {
        val response = client().makeRequest(
            "PUT",
            "${monitor.relativeUrl()}?refresh=$refresh",
            emptyMap(),
            monitor.toHttpEntity()
        )
        assertEquals("Unable to update a monitor", RestStatus.OK, response.restStatus())
        assertUserNull(response.asMap()["monitor"] as Map<String, Any>)
        return getMonitor(monitorId = monitor.id)
    }

    @Suppress("UNCHECKED_CAST")
    protected fun updateWorkflow(workflow: Workflow, refresh: Boolean = false): Workflow {
        val response = client().makeRequest(
            "PUT",
            "${workflow.relativeUrl()}?refresh=$refresh",
            emptyMap(),
            workflow.toHttpEntity()
        )
        assertEquals("Unable to update a workflow", RestStatus.OK, response.restStatus())
        assertUserNull(response.asMap()["workflow"] as Map<String, Any>)
        return getWorkflow(workflowId = workflow.id)
    }

    protected fun updateMonitorWithClient(
        client: RestClient,
        monitor: Monitor,
        rbacRoles: List<String> = emptyList(),
        refresh: Boolean = true,
    ): Monitor {
        val response = client.makeRequest(
            "PUT",
            "${monitor.relativeUrl()}?refresh=$refresh",
            emptyMap(),
            createMonitorEntityWithBackendRoles(monitor, rbacRoles)
        )
        assertEquals("Unable to update a monitor", RestStatus.OK, response.restStatus())
        assertUserNull(response.asMap()["monitor"] as Map<String, Any>)
        return getMonitor(monitorId = monitor.id)
    }

    protected fun updateWorkflowWithClient(
        client: RestClient,
        workflow: Workflow,
        rbacRoles: List<String> = emptyList(),
        refresh: Boolean = true,
    ): Workflow {
        val response = client.makeRequest(
            "PUT",
            "${workflow.relativeUrl()}?refresh=$refresh",
            emptyMap(),
            createWorkflowEntityWithBackendRoles(workflow, rbacRoles)
        )
        assertEquals("Unable to update a workflow", RestStatus.OK, response.restStatus())
        assertUserNull(response.asMap()["workflow"] as Map<String, Any>)
        return getWorkflow(workflowId = workflow.id)
    }

    protected fun getMonitor(monitorId: String, header: BasicHeader = BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")): Monitor {
        val response = client().makeRequest("GET", "$ALERTING_BASE_URI/$monitorId", null, header)
        assertEquals("Unable to get monitor $monitorId", RestStatus.OK, response.restStatus())

        val parser = createParser(XContentType.JSON.xContent(), response.entity.content)
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser)

        lateinit var id: String
        var version: Long = 0
        lateinit var monitor: Monitor

        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            parser.nextToken()

            when (parser.currentName()) {
                "_id" -> id = parser.text()
                "_version" -> version = parser.longValue()
                "monitor" -> monitor = Monitor.parse(parser)
                "associated_workflows" -> {
                    XContentParserUtils.ensureExpectedToken(
                        XContentParser.Token.START_ARRAY,
                        parser.currentToken(),
                        parser
                    )
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        // do nothing
                    }
                }
            }
        }

        assertUserNull(monitor)
        return monitor.copy(id = id, version = version)
    }

    // TODO: understand why doc alerts wont work with the normal search Alerts function
    protected fun searchAlertsWithFilter(
        monitor: Monitor,
        indices: String = AlertIndices.ALERT_INDEX,
        refresh: Boolean = true,
    ): List<Alert> {
        if (refresh) refreshIndex(indices)

        val request = """
                { "version" : true,
                  "query": { "match_all": {} }
                }
        """.trimIndent()
        val httpResponse = adminClient().makeRequest("GET", "/$indices/_search", StringEntity(request, APPLICATION_JSON))
        assertEquals("Search failed", RestStatus.OK, httpResponse.restStatus())

        val searchResponse = SearchResponse.fromXContent(createParser(jsonXContent, httpResponse.entity.content))
        return searchResponse.hits.hits.map {
            val xcp = createParser(jsonXContent, it.sourceRef).also { it.nextToken() }
            Alert.parse(xcp, it.id, it.version)
        }.filter { alert -> alert.monitorId == monitor.id }
    }

    protected fun createFinding(
        monitorId: String = "NO_ID",
        monitorName: String = "NO_NAME",
        index: String = "testIndex",
        docLevelQueries: List<DocLevelQuery> = listOf(
            DocLevelQuery(query = "test_field:\"us-west-2\"", name = "testQuery", fields = listOf())
        ),
        matchingDocIds: List<String>,
    ): String {
        val finding = Finding(
            id = UUID.randomUUID().toString(),
            relatedDocIds = matchingDocIds,
            monitorId = monitorId,
            monitorName = monitorName,
            index = index,
            docLevelQueries = docLevelQueries,
            timestamp = Instant.now()
        )

        val findingStr = finding.toXContent(XContentBuilder.builder(XContentType.JSON.xContent()), ToXContent.EMPTY_PARAMS).string()

        indexDoc(FINDING_HISTORY_WRITE_INDEX, finding.id, findingStr)
        return finding.id
    }

    protected fun searchFindings(
        monitor: Monitor,
        indices: String = AlertIndices.ALL_FINDING_INDEX_PATTERN,
        refresh: Boolean = true,
    ): List<Finding> {
        if (refresh) refreshIndex(indices)

        val request = """
                { "version" : true,
                  "query": { "match_all": {} }
                }
        """.trimIndent()
        val httpResponse = adminClient().makeRequest("GET", "/$indices/_search", StringEntity(request, APPLICATION_JSON))
        assertEquals("Search failed", RestStatus.OK, httpResponse.restStatus())

        val searchResponse = SearchResponse.fromXContent(createParser(jsonXContent, httpResponse.entity.content))
        return searchResponse.hits.hits.map {
            val xcp = createParser(jsonXContent, it.sourceRef).also { it.nextToken() }
            Finding.parse(xcp)
        }.filter { finding -> finding.monitorId == monitor.id }
    }

    protected fun searchAlerts(monitor: Monitor, indices: String = AlertIndices.ALERT_INDEX, refresh: Boolean = true): List<Alert> {
        try {
            if (refresh) refreshIndex(indices)
        } catch (e: Exception) {
            logger.warn("Could not refresh index $indices because: ${e.message}")
            return emptyList()
        }

        // If this is a test monitor (it doesn't have an ID) and no alerts will be saved for it.
        val searchParams = if (monitor.id != Monitor.NO_ID) mapOf("routing" to monitor.id) else mapOf()
        val request = """
                { "version" : true,
                  "query" : { "term" : { "${Alert.MONITOR_ID_FIELD}" : "${monitor.id}" } }
                }
        """.trimIndent()
        val httpResponse = adminClient().makeRequest("GET", "/$indices/_search", searchParams, StringEntity(request, APPLICATION_JSON))
        assertEquals("Search failed", RestStatus.OK, httpResponse.restStatus())

        val searchResponse = SearchResponse.fromXContent(createParser(jsonXContent, httpResponse.entity.content))
        return searchResponse.hits.hits.map {
            val xcp = createParser(jsonXContent, it.sourceRef).also { it.nextToken() }
            Alert.parse(xcp, it.id, it.version)
        }
    }

    protected fun acknowledgeAlerts(monitor: Monitor, vararg alerts: Alert): Response {
        val request = XContentFactory.jsonBuilder().startObject()
            .array("alerts", *alerts.map { it.id }.toTypedArray())
            .endObject()
            .string()
            .let { StringEntity(it, APPLICATION_JSON) }

        val response = client().makeRequest(
            "POST",
            "${monitor.relativeUrl()}/_acknowledge/alerts?refresh=true",
            emptyMap(),
            request
        )
        assertEquals("Acknowledge call failed.", RestStatus.OK, response.restStatus())
        return response
    }

    protected fun acknowledgeChainedAlerts(workflowId: String, vararg alertId: String): Response {
        val request = jsonBuilder().startObject()
            .array("alerts", *alertId.map { it }.toTypedArray())
            .endObject()
            .string()
            .let { StringEntity(it, APPLICATION_JSON) }

        val response = client().makeRequest(
            "POST", "${AlertingPlugin.WORKFLOW_BASE_URI}/$workflowId/_acknowledge/alerts",
            emptyMap(), request
        )
        assertEquals("Acknowledge call failed.", RestStatus.OK, response.restStatus())
        return response
    }

    protected fun getAlerts(
        client: RestClient,
        dataMap: Map<String, Any> = emptyMap(),
        header: BasicHeader = BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json"),
    ): Response {
        var baseEndpoint = "$ALERTING_BASE_URI/alerts?"
        for (entry in dataMap.entries) {
            baseEndpoint += "${entry.key}=${entry.value}&"
        }

        val response = client.makeRequest("GET", baseEndpoint, null, header)
        assertEquals("Get call failed.", RestStatus.OK, response.restStatus())
        return response
    }

    protected fun getAlerts(
        dataMap: Map<String, Any> = emptyMap(),
        header: BasicHeader = BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json"),
    ): Response {
        return getAlerts(client(), dataMap, header)
    }

    protected fun refreshIndex(index: String): Response {
        val response = client().makeRequest("POST", "/$index/_refresh?expand_wildcards=all")
        assertEquals("Unable to refresh index", RestStatus.OK, response.restStatus())
        return response
    }

    protected fun deleteIndex(index: String): Response {
        val response = adminClient().makeRequest("DELETE", "/$index")
        assertEquals("Unable to delete index", RestStatus.OK, response.restStatus())
        return response
    }

    protected fun executeMonitor(monitorId: String, params: Map<String, String> = mutableMapOf()): Response {
        return executeMonitor(client(), monitorId, params)
    }

    protected fun executeWorkflow(workflowId: String, params: Map<String, String> = mutableMapOf()): Response {
        return executeWorkflow(client(), workflowId, params)
    }

    protected fun getWorkflowAlerts(
        workflowId: String,
        alertId: String? = "",
        getAssociatedAlerts: Boolean = true,
    ): Response {
        return getWorkflowAlerts(
            client(),
            mutableMapOf(Pair("workflowIds", workflowId), Pair("getAssociatedAlerts", getAssociatedAlerts), Pair("alertIds", alertId!!))
        )
    }

    protected fun getWorkflowAlerts(
        client: RestClient,
        dataMap: Map<String, Any> = emptyMap(),
        header: BasicHeader = BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json"),
    ): Response {
        var baseEndpoint = "$WORKFLOW_ALERTING_BASE_URI/alerts?"
        for (entry in dataMap.entries) {
            baseEndpoint += "${entry.key}=${entry.value}&"
        }

        val response = client.makeRequest("GET", baseEndpoint, null, header)
        assertEquals("Get call failed.", RestStatus.OK, response.restStatus())
        return response
    }

    protected fun executeMonitor(client: RestClient, monitorId: String, params: Map<String, String> = mutableMapOf()): Response {
        return client.makeRequest("POST", "$ALERTING_BASE_URI/$monitorId/_execute", params)
    }

    protected fun executeWorkflow(client: RestClient, workflowId: String, params: Map<String, String> = mutableMapOf()): Response {
        return client.makeRequest("POST", "$WORKFLOW_ALERTING_BASE_URI/$workflowId/_execute", params)
    }

    protected fun executeMonitor(monitor: Monitor, params: Map<String, String> = mapOf()): Response {
        return executeMonitor(client(), monitor, params)
    }

    protected fun executeMonitor(client: RestClient, monitor: Monitor, params: Map<String, String> = mapOf()): Response =
        client.makeRequest("POST", "$ALERTING_BASE_URI/_execute", params, monitor.toHttpEntityWithUser())

    protected fun searchFindings(params: Map<String, String> = mutableMapOf()): GetFindingsResponse {
        var baseEndpoint = "${AlertingPlugin.FINDING_BASE_URI}/_search?"
        for (entry in params.entries) {
            baseEndpoint += "${entry.key}=${entry.value}&"
        }

        val response = client().makeRequest("GET", baseEndpoint)

        assertEquals("Unable to retrieve findings", RestStatus.OK, response.restStatus())

        val parser = createParser(XContentType.JSON.xContent(), response.entity.content)
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser)

        var totalFindings = 0
        val findings = mutableListOf<FindingWithDocs>()

        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            parser.nextToken()

            when (parser.currentName()) {
                "total_findings" -> totalFindings = parser.intValue()
                "findings" -> {
                    XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser)
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        findings.add(FindingWithDocs.parse(parser))
                    }
                }
            }
        }

        return GetFindingsResponse(response.restStatus(), totalFindings, findings)
    }

    protected fun searchMonitors(): SearchResponse {
        var baseEndpoint = "${AlertingPlugin.MONITOR_BASE_URI}/_search?"
        val request = """
                { "version" : true,
                  "query": { "match_all": {} }
                }
        """.trimIndent()
        val httpResponse = adminClient().makeRequest("POST", baseEndpoint, StringEntity(request, APPLICATION_JSON))
        assertEquals("Search failed", RestStatus.OK, httpResponse.restStatus())
        return SearchResponse.fromXContent(createParser(jsonXContent, httpResponse.entity.content))
    }

    protected fun indexDoc(index: String, id: String, doc: String, refresh: Boolean = true): Response {
        return indexDoc(client(), index, id, doc, refresh)
    }

    protected fun indexDocWithAdminClient(index: String, id: String, doc: String, refresh: Boolean = true): Response {
        return indexDoc(adminClient(), index, id, doc, refresh)
    }

    private fun indexDoc(client: RestClient, index: String, id: String, doc: String, refresh: Boolean = true): Response {
        val requestBody = StringEntity(doc, APPLICATION_JSON)
        val params = if (refresh) mapOf("refresh" to "true") else mapOf()
        val response = client.makeRequest("PUT", "$index/_doc/$id", params, requestBody)
        assertTrue(
            "Unable to index doc: '${doc.take(15)}...' to index: '$index'",
            listOf(RestStatus.OK, RestStatus.CREATED).contains(response.restStatus())
        )
        return response
    }

    protected fun deleteDoc(index: String, id: String, refresh: Boolean = true): Response {
        val params = if (refresh) mapOf("refresh" to "true") else mapOf()
        val response = client().makeRequest("DELETE", "$index/_doc/$id", params)
        assertTrue("Unable to delete doc with ID $id in index: '$index'", listOf(RestStatus.OK).contains(response.restStatus()))
        return response
    }

    /** A test index that can be used across tests. Feel free to add new fields but don't remove any. */
    protected fun createTestIndex(index: String = randomAlphaOfLength(10).lowercase(Locale.ROOT)): String {
        createIndex(
            index,
            Settings.EMPTY,
            """
                "properties" : {
                  "test_strict_date_time" : { "type" : "date", "format" : "strict_date_time" },
                  "test_field" : { "type" : "keyword" },
                  "number" : { "type" : "keyword" }
                }
            """.trimIndent()
        )
        return index
    }

    protected fun createTestIndex(index: String, mapping: String): String {
        createIndex(index, Settings.EMPTY, mapping.trimIndent())
        return index
    }

    protected fun createTestConfigIndex(index: String = "." + randomAlphaOfLength(10).lowercase(Locale.ROOT)): String {
        try {
            createIndex(
                index,
                Settings.builder().build(),
                """
                    "properties" : {
                      "test_strict_date_time" : { "type" : "date", "format" : "strict_date_time" }
                    }
                """.trimIndent()
            )
        } catch (ex: WarningFailureException) {
            // ignore
        }
        return index
    }

    protected fun createTestAlias(
        alias: String = randomAlphaOfLength(10).lowercase(Locale.ROOT),
        numOfAliasIndices: Int = randomIntBetween(1, 10),
        includeWriteIndex: Boolean = true,
    ): MutableMap<String, MutableMap<String, Boolean>> {
        return createTestAlias(alias = alias, indices = randomAliasIndices(alias, numOfAliasIndices, includeWriteIndex))
    }

    protected fun createTestAlias(
        alias: String = randomAlphaOfLength(10).lowercase(Locale.ROOT),
        indices: Map<String, Boolean> = randomAliasIndices(
            alias = alias,
            num = randomIntBetween(1, 10),
            includeWriteIndex = true
        ),
    ): MutableMap<String, MutableMap<String, Boolean>> {
        val indicesMap = mutableMapOf<String, Boolean>()
        val indicesJson = jsonBuilder().startObject().startArray("actions")
        indices.keys.map {
            val indexName = createTestIndex(index = it.lowercase(Locale.ROOT), mapping = "")
            val isWriteIndex = indices.getOrDefault(indexName, false)
            indicesMap[indexName] = isWriteIndex
            val indexMap = mapOf(
                "add" to mapOf(
                    "index" to indexName,
                    "alias" to alias,
                    "is_write_index" to isWriteIndex
                )
            )
            indicesJson.value(indexMap)
        }
        val requestBody = indicesJson.endArray().endObject().string()
        client().makeRequest("POST", "/_aliases", emptyMap(), StringEntity(requestBody, APPLICATION_JSON))
        return mutableMapOf(alias to indicesMap)
    }

    protected fun randomAliasIndices(
        alias: String,
        num: Int = randomIntBetween(1, 10),
        includeWriteIndex: Boolean = true,
    ): Map<String, Boolean> {
        val indices = mutableMapOf<String, Boolean>()
        val writeIndex = randomIntBetween(0, num)
        for (i: Int in 0 until num) {
            var indexName = randomAlphaOfLength(10)
            while (indexName.equals(alias) || indices.containsKey(indexName))
                indexName = randomAlphaOfLength(10)
            indices[indexName] = includeWriteIndex && i == writeIndex
        }
        return indices
    }

    protected fun insertSampleTimeSerializedData(index: String, data: List<String>) {
        data.forEachIndexed { i, value ->
            val twoMinsAgo = ZonedDateTime.now().minus(2, ChronoUnit.MINUTES).truncatedTo(ChronoUnit.MILLIS)
            val testTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(twoMinsAgo)
            val testDoc = """
                {
                  "test_strict_date_time": "$testTime",
                  "test_field": "$value",
                   "number": "$i"
                }
            """.trimIndent()
            // Indexing documents with deterministic doc id to allow for easy selected deletion during testing
            indexDoc(index, (i + 1).toString(), testDoc)
        }
    }

    protected fun deleteDataWithDocIds(index: String, docIds: List<String>) {
        docIds.forEach {
            deleteDoc(index, it)
        }
    }

    fun putAlertMappings(mapping: String? = null) {
        val mappingHack = if (mapping != null) mapping else AlertIndices.alertMapping().trimStart('{').trimEnd('}')
        val encodedHistoryIndex = URLEncoder.encode(AlertIndices.ALERT_HISTORY_INDEX_PATTERN, Charsets.UTF_8.toString())
        val settings = Settings.builder().put("index.hidden", true).build()
        createIndex(AlertIndices.ALERT_INDEX, settings, mappingHack)
        createIndex(encodedHistoryIndex, settings, mappingHack, "\"${AlertIndices.ALERT_HISTORY_WRITE_INDEX}\" : {}")
    }

    fun putFindingMappings(mapping: String? = null) {
        val mappingHack = if (mapping != null) mapping else AlertIndices.findingMapping().trimStart('{').trimEnd('}')
        val encodedHistoryIndex = URLEncoder.encode(AlertIndices.FINDING_HISTORY_INDEX_PATTERN, Charsets.UTF_8.toString())
        val settings = Settings.builder().put("index.hidden", true).build()
//        createIndex(AlertIndices.FINDING_HISTORY_WRITE_INDEX, settings, mappingHack)
        createIndex(encodedHistoryIndex, settings, mappingHack, "\"${AlertIndices.FINDING_HISTORY_WRITE_INDEX}\" : {}")
    }

    fun scheduledJobMappings(): String {
        return javaClass.classLoader.getResource("mappings/scheduled-jobs.json").readText()
    }

    /** Creates the Alerting config index if it does not exist */
    fun createAlertingConfigIndex(mapping: String? = null) {
        val indexExistsResponse = client().makeRequest("HEAD", ScheduledJob.SCHEDULED_JOBS_INDEX)
        if (indexExistsResponse.restStatus() == RestStatus.NOT_FOUND) {
            val mappingHack = mapping ?: scheduledJobMappings().trimStart('{').trimEnd('}')
            val settings = Settings.builder().put("index.hidden", true).build()
            createIndex(ScheduledJob.SCHEDULED_JOBS_INDEX, settings, mappingHack)
        }
    }

    protected fun Response.restStatus(): RestStatus {
        return RestStatus.fromCode(this.statusLine.statusCode)
    }

    protected fun Monitor.toHttpEntity(): HttpEntity {
        return StringEntity(toJsonString(), APPLICATION_JSON)
    }

    private fun Monitor.toJsonString(): String {
        val builder = XContentFactory.jsonBuilder()
        return shuffleXContent(toXContent(builder, ToXContent.EMPTY_PARAMS)).string()
    }

    protected fun Monitor.toHttpEntityWithUser(): HttpEntity {
        return StringEntity(toJsonStringWithUser(), APPLICATION_JSON)
    }

    private fun Monitor.toJsonStringWithUser(): String {
        val builder = jsonBuilder()
        return shuffleXContent(toXContentWithUser(builder, ToXContent.EMPTY_PARAMS)).string()
    }

    protected fun Destination.toHttpEntity(): HttpEntity {
        return StringEntity(toJsonString(), APPLICATION_JSON)
    }

    protected fun Destination.toJsonString(): String {
        val builder = jsonBuilder()
        return shuffleXContent(toXContent(builder)).string()
    }

    protected fun Destination.toJsonStringWithType(): String {
        val builder = jsonBuilder()
        return shuffleXContent(
            toXContent(builder, ToXContent.MapParams(mapOf("with_type" to "true")))
        ).string()
    }

    protected fun EmailAccount.toHttpEntity(): HttpEntity {
        return StringEntity(toJsonString(), APPLICATION_JSON)
    }

    protected fun EmailAccount.toJsonString(): String {
        val builder = jsonBuilder()
        return shuffleXContent(toXContent(builder)).string()
    }

    protected fun EmailAccount.toJsonStringWithType(): String {
        val builder = jsonBuilder()
        return shuffleXContent(
            toXContent(builder, ToXContent.MapParams(mapOf("with_type" to "true")))
        ).string()
    }

    protected fun EmailGroup.toHttpEntity(): HttpEntity {
        return StringEntity(toJsonString(), APPLICATION_JSON)
    }

    protected fun EmailGroup.toJsonString(): String {
        val builder = jsonBuilder()
        return shuffleXContent(toXContent(builder)).string()
    }

    protected fun EmailGroup.toJsonStringWithType(): String {
        val builder = jsonBuilder()
        return shuffleXContent(
            toXContent(builder, ToXContent.MapParams(mapOf("with_type" to "true")))
        ).string()
    }

    protected fun Alert.toHttpEntityWithUser(): HttpEntity {
        return StringEntity(toJsonStringWithUser(), APPLICATION_JSON)
    }

    private fun Alert.toJsonStringWithUser(): String {
        val builder = jsonBuilder()
        return shuffleXContent(toXContentWithUser(builder)).string()
    }

    protected fun Monitor.relativeUrl() = "$ALERTING_BASE_URI/$id"

    // Useful settings when debugging to prevent timeouts
    override fun restClientSettings(): Settings {
        return if (isDebuggingTest || isDebuggingRemoteCluster) {
            Settings.builder()
                .put(CLIENT_SOCKET_TIMEOUT, TimeValue.timeValueMinutes(10))
                .build()
        } else {
            super.restClientSettings()
        }
    }

    fun RestClient.getClusterSettings(settings: Map<String, String>): Map<String, Any> {
        val response = this.makeRequest("GET", "_cluster/settings", settings)
        assertEquals(RestStatus.OK, response.restStatus())
        return response.asMap()
    }

    fun RestClient.getIndexMapping(index: String): Map<String, Any> {
        val response = this.makeRequest("GET", "$index/_mapping")
        assertEquals(RestStatus.OK, response.restStatus())
        return response.asMap()
    }

    fun RestClient.updateSettings(setting: String, value: Any): Map<String, Any> {
        val settings = jsonBuilder()
            .startObject()
            .startObject("persistent")
            .field(setting, value)
            .endObject()
            .endObject()
            .string()
        val response = this.makeRequest("PUT", "_cluster/settings", StringEntity(settings, APPLICATION_JSON))
        assertEquals(RestStatus.OK, response.restStatus())
        return response.asMap()
    }

    @Suppress("UNCHECKED_CAST")
    fun Map<String, Any>.opendistroSettings(): Map<String, Any>? {
        val map = this as Map<String, Map<String, Map<String, Map<String, Any>>>>
        return map["defaults"]?.get("opendistro")?.get("alerting")
    }

    @Suppress("UNCHECKED_CAST")
    fun Map<String, Any>.stringMap(key: String): Map<String, Any>? {
        val map = this as Map<String, Map<String, Any>>
        return map[key]
    }

    fun getAlertingStats(metrics: String = ""): Map<String, Any> {
        val monitorStatsResponse = client().makeRequest("GET", "/_plugins/_alerting/stats$metrics")
        val responseMap = createParser(XContentType.JSON.xContent(), monitorStatsResponse.entity.content).map()
        return responseMap
    }

    fun enableScheduledJob(): Response {
        val updateResponse = client().makeRequest(
            "PUT",
            "_cluster/settings",
            emptyMap(),
            StringEntity(
                XContentFactory.jsonBuilder().startObject().field("persistent")
                    .startObject().field(ScheduledJobSettings.SWEEPER_ENABLED.key, true).endObject()
                    .endObject().string(),
                ContentType.APPLICATION_JSON
            )
        )
        return updateResponse
    }

    fun disableScheduledJob(): Response {
        val updateResponse = client().makeRequest(
            "PUT",
            "_cluster/settings",
            emptyMap(),
            StringEntity(
                XContentFactory.jsonBuilder().startObject().field("persistent")
                    .startObject().field(ScheduledJobSettings.SWEEPER_ENABLED.key, false).endObject()
                    .endObject().string(),
                ContentType.APPLICATION_JSON
            )
        )
        return updateResponse
    }

    fun enableFilterBy() {
        val updateResponse = client().makeRequest(
            "PUT",
            "_cluster/settings",
            emptyMap(),
            StringEntity(
                XContentFactory.jsonBuilder().startObject().field("persistent")
                    .startObject().field(AlertingSettings.FILTER_BY_BACKEND_ROLES.key, true).endObject()
                    .endObject().string(),
                ContentType.APPLICATION_JSON
            )
        )
        assertEquals(updateResponse.statusLine.toString(), 200, updateResponse.statusLine.statusCode)
    }

    fun disableFilterBy() {
        val updateResponse = client().makeRequest(
            "PUT",
            "_cluster/settings",
            emptyMap(),
            StringEntity(
                XContentFactory.jsonBuilder().startObject().field("persistent")
                    .startObject().field(AlertingSettings.FILTER_BY_BACKEND_ROLES.key, false).endObject()
                    .endObject().string(),
                ContentType.APPLICATION_JSON
            )
        )
        assertEquals(updateResponse.statusLine.toString(), 200, updateResponse.statusLine.statusCode)
    }

    fun removeEmailFromAllowList() {
        val allowedDestinations = DestinationType.values().toList()
            .filter { destinationType -> destinationType != DestinationType.EMAIL }
            .joinToString(prefix = "[", postfix = "]") { string -> "\"$string\"" }
        client().updateSettings(DestinationSettings.ALLOW_LIST.key, allowedDestinations)
    }

    fun createUser(name: String, backendRoles: Array<String>) {
        val request = Request("PUT", "/_plugins/_security/api/internalusers/$name")
        val broles = backendRoles.joinToString { it -> "\"$it\"" }
        var entity = " {\n" +
            "\"password\": \"$password\",\n" +
            "\"backend_roles\": [$broles],\n" +
            "\"attributes\": {\n" +
            "}} "
        request.setJsonEntity(entity)
        client().performRequest(request)
    }

    fun patchUserBackendRoles(name: String, backendRoles: Array<String>) {
        val request = Request("PATCH", "/_plugins/_security/api/internalusers/$name")
        val broles = backendRoles.joinToString { "\"$it\"" }
        var entity = " [{\n" +
            "\"op\": \"replace\",\n" +
            "\"path\": \"/backend_roles\",\n" +
            "\"value\": [$broles]\n" +
            "}]"
        request.setJsonEntity(entity)
        client().performRequest(request)
    }

    fun createIndexRole(name: String, index: String) {
        val request = Request("PUT", "/_plugins/_security/api/roles/$name")
        var entity = "{\n" +
            "\"cluster_permissions\": [\n" +
            "],\n" +
            "\"index_permissions\": [\n" +
            "{\n" +
            "\"index_patterns\": [\n" +
            "\"$index\"\n" +
            "],\n" +
            "\"dls\": \"\",\n" +
            "\"fls\": [],\n" +
            "\"masked_fields\": [],\n" +
            "\"allowed_actions\": [\n" +
            "\"crud\"\n" +
            "]\n" +
            "}\n" +
            "],\n" +
            "\"tenant_permissions\": []\n" +
            "}"
        request.setJsonEntity(entity)
        client().performRequest(request)
    }

    fun createCustomIndexRole(name: String, index: String, clusterPermissions: String?) {
        val request = Request("PUT", "/_plugins/_security/api/roles/$name")
        var entity = "{\n" +
            "\"cluster_permissions\": [\n" +
            "\"$clusterPermissions\"\n" +
            "],\n" +
            "\"index_permissions\": [\n" +
            "{\n" +
            "\"index_patterns\": [\n" +
            "\"$index\"\n" +
            "],\n" +
            "\"dls\": \"\",\n" +
            "\"fls\": [],\n" +
            "\"masked_fields\": [],\n" +
            "\"allowed_actions\": [\n" +
            "\"crud\"\n" +
            "]\n" +
            "}\n" +
            "],\n" +
            "\"tenant_permissions\": []\n" +
            "}"
        request.setJsonEntity(entity)
        client().performRequest(request)
    }

    private fun createCustomIndexRole(name: String, index: String, clusterPermissions: List<String?>) {
        val request = Request("PUT", "/_plugins/_security/api/roles/$name")

        val clusterPermissionsStr =
            clusterPermissions.stream().map { p: String? -> "\"" + p + "\"" }.collect(
                Collectors.joining(",")
            )

        var entity = "{\n" +
            "\"cluster_permissions\": [\n" +
            "$clusterPermissionsStr\n" +
            "],\n" +
            "\"index_permissions\": [\n" +
            "{\n" +
            "\"index_patterns\": [\n" +
            "\"$index\"\n" +
            "],\n" +
            "\"dls\": \"\",\n" +
            "\"fls\": [],\n" +
            "\"masked_fields\": [],\n" +
            "\"allowed_actions\": [\n" +
            "\"crud\"\n" +
            "]\n" +
            "}\n" +
            "],\n" +
            "\"tenant_permissions\": []\n" +
            "}"
        request.setJsonEntity(entity)
        client().performRequest(request)
    }

    fun createIndexRoleWithDocLevelSecurity(name: String, index: String, dlsQuery: String, clusterPermissions: String? = "") {
        val request = Request("PUT", "/_plugins/_security/api/roles/$name")
        var entity = "{\n" +
            "\"cluster_permissions\": [\n" +
            "\"$clusterPermissions\"\n" +
            "],\n" +
            "\"index_permissions\": [\n" +
            "{\n" +
            "\"index_patterns\": [\n" +
            "\"$index\"\n" +
            "],\n" +
            "\"dls\": \"$dlsQuery\",\n" +
            "\"fls\": [],\n" +
            "\"masked_fields\": [],\n" +
            "\"allowed_actions\": [\n" +
            "\"crud\"\n" +
            "]\n" +
            "}\n" +
            "],\n" +
            "\"tenant_permissions\": []\n" +
            "}"
        request.setJsonEntity(entity)
        client().performRequest(request)
    }

    fun createIndexRoleWithDocLevelSecurity(name: String, index: String, dlsQuery: String, clusterPermissions: List<String>) {
        val clusterPermissionsStr =
            clusterPermissions.stream().map { p: String -> "\"" + getClusterPermissionsFromCustomRole(p) + "\"" }.collect(
                Collectors.joining(",")
            )

        val request = Request("PUT", "/_plugins/_security/api/roles/$name")
        var entity = "{\n" +
            "\"cluster_permissions\": [\n" +
            "$clusterPermissionsStr\n" +
            "],\n" +
            "\"index_permissions\": [\n" +
            "{\n" +
            "\"index_patterns\": [\n" +
            "\"$index\"\n" +
            "],\n" +
            "\"dls\": \"$dlsQuery\",\n" +
            "\"fls\": [],\n" +
            "\"masked_fields\": [],\n" +
            "\"allowed_actions\": [\n" +
            "\"crud\"\n" +
            "]\n" +
            "}\n" +
            "],\n" +
            "\"tenant_permissions\": []\n" +
            "}"
        request.setJsonEntity(entity)
        client().performRequest(request)
    }

    fun createUserRolesMapping(role: String, users: Array<String>) {
        val request = Request("PUT", "/_plugins/_security/api/rolesmapping/$role")
        val usersStr = users.joinToString { it -> "\"$it\"" }
        var entity = "{                                  \n" +
            "  \"backend_roles\" : [  ],\n" +
            "  \"hosts\" : [  ],\n" +
            "  \"users\" : [$usersStr]\n" +
            "}"
        request.setJsonEntity(entity)
        client().performRequest(request)
    }

    fun updateRoleMapping(role: String, users: List<String>, addUser: Boolean) {
        val request = Request("PATCH", "/_plugins/_security/api/rolesmapping/$role")
        val usersStr = users.joinToString { it -> "\"$it\"" }

        val op = if (addUser) "add" else "remove"

        val entity = "[{\n" +
            "  \"op\" : \"$op\",\n" +
            "  \"path\" : \"/users\",\n" +
            "  \"value\" : [$usersStr]\n" +
            "}]"

        request.setJsonEntity(entity)
        client().performRequest(request)
    }

    fun deleteUser(name: String) {
        client().makeRequest("DELETE", "/_plugins/_security/api/internalusers/$name")
    }

    fun deleteRole(name: String) {
        client().makeRequest("DELETE", "/_plugins/_security/api/roles/$name")
    }

    fun deleteRoleMapping(name: String) {
        client().makeRequest("DELETE", "/_plugins/_security/api/rolesmapping/$name")
    }

    fun deleteRoleAndRoleMapping(role: String) {
        deleteRoleMapping(role)
        deleteRole(role)
    }

    fun createUserWithTestData(user: String, index: String, role: String, backendRole: String) {
        createUser(user, arrayOf(backendRole))
        createTestIndex(index)
        createIndexRole(role, index)
        createUserRolesMapping(role, arrayOf(user))
    }

    fun createUserWithTestDataAndCustomRole(
        user: String,
        index: String,
        role: String,
        backendRoles: List<String>,
        clusterPermissions: String?,
    ) {
        createUser(user, backendRoles.toTypedArray())
        createTestIndex(index)
        createCustomIndexRole(role, index, clusterPermissions)
        createUserRolesMapping(role, arrayOf(user))
    }

    fun createUserWithTestDataAndCustomRole(
        user: String,
        index: String,
        role: String,
        backendRoles: List<String>,
        clusterPermissions: List<String?>,
    ) {
        createUser(user, backendRoles.toTypedArray())
        createTestIndex(index)
        createCustomIndexRole(role, index, clusterPermissions)
        createUserRolesMapping(role, arrayOf(user))
    }

    fun createUserWithRoles(
        user: String,
        roles: List<String>,
        backendRoles: List<String>,
        isExistingRole: Boolean,
    ) {
        createUser(user, backendRoles.toTypedArray())
        for (role in roles) {
            if (isExistingRole) {
                updateRoleMapping(role, listOf(user), true)
            } else {
                createUserRolesMapping(role, arrayOf(user))
            }
        }
    }

    fun createUserWithDocLevelSecurityTestData(
        user: String,
        index: String,
        role: String,
        backendRole: String,
        dlsQuery: String,
    ) {
        createUser(user, arrayOf(backendRole))
        createTestIndex(index)
        createIndexRoleWithDocLevelSecurity(role, index, dlsQuery)
        createUserRolesMapping(role, arrayOf(user))
    }

    fun createUserWithDocLevelSecurityTestDataAndCustomRole(
        user: String,
        index: String,
        role: String,
        backendRole: String,
        dlsQuery: String,
        clusterPermissions: String?,
    ) {
        createUser(user, arrayOf(backendRole))
        createTestIndex(index)
        createIndexRoleWithDocLevelSecurity(role, index, dlsQuery)
        createCustomIndexRole(role, index, clusterPermissions)
        createUserRolesMapping(role, arrayOf(user))
    }

    fun getClusterPermissionsFromCustomRole(clusterPermissions: String): String? {
        return ROLE_TO_PERMISSION_MAPPING.get(clusterPermissions)
    }

    companion object {
        internal interface IProxy {
            val version: String?
            var sessionId: String?

            fun getExecutionData(reset: Boolean): ByteArray?
            fun dump(reset: Boolean)
            fun reset()
        }

        /*
        * We need to be able to dump the jacoco coverage before the cluster shuts down.
        * The new internal testing framework removed some gradle tasks we were listening to,
        * to choose a good time to do it. This will dump the executionData to file after each test.
        * TODO: This is also currently just overwriting integTest.exec with the updated execData without
        *   resetting after writing each time. This can be improved to either write an exec file per test
        *   or by letting jacoco append to the file.
        * */
        @JvmStatic
        @AfterClass
        fun dumpCoverage() {
            // jacoco.dir set in opensearchplugin-coverage.gradle, if it doesn't exist we don't
            // want to collect coverage, so we can return early
            val jacocoBuildPath = System.getProperty("jacoco.dir") ?: return
            val serverUrl = "service:jmx:rmi:///jndi/rmi://127.0.0.1:7777/jmxrmi"
            JMXConnectorFactory.connect(JMXServiceURL(serverUrl)).use { connector ->
                val proxy = MBeanServerInvocationHandler.newProxyInstance(
                    connector.mBeanServerConnection,
                    ObjectName("org.jacoco:type=Runtime"),
                    IProxy::class.java,
                    false
                )
                proxy.getExecutionData(false)?.let {
                    val path = PathUtils.get("$jacocoBuildPath/integTest.exec")
                    Files.write(path, it)
                }
            }
        }
    }

    protected fun createRandomWorkflow(monitorIds: List<String>, refresh: Boolean = false): Workflow {
        val workflow = randomWorkflow(monitorIds = monitorIds)
        return createWorkflow(workflow, refresh)
    }

    private fun createWorkflowEntityWithBackendRoles(workflow: Workflow, rbacRoles: List<String>?): HttpEntity {
        if (rbacRoles == null) {
            return workflow.toHttpEntity()
        }
        val temp = workflow.toJsonString()
        val toReplace = temp.lastIndexOf("}")
        val rbacString = rbacRoles.joinToString { "\"$it\"" }
        val jsonString = temp.substring(0, toReplace) + ", \"rbac_roles\": [$rbacString] }"
        return StringEntity(jsonString, ContentType.APPLICATION_JSON)
    }

    protected fun createWorkflowWithClient(
        client: RestClient,
        workflow: Workflow,
        rbacRoles: List<String>? = null,
        refresh: Boolean = true,
    ): Workflow {
        val response = client.makeRequest(
            "POST", "$WORKFLOW_ALERTING_BASE_URI?refresh=$refresh", emptyMap(),
            createWorkflowEntityWithBackendRoles(workflow, rbacRoles)
        )
        assertEquals("Unable to create a new monitor", RestStatus.CREATED, response.restStatus())

        val workflowJson = jsonXContent.createParser(
            NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE,
            response.entity.content
        ).map()
        assertUserNull(workflowJson as HashMap<String, Any>)
        return workflow.copy(id = workflowJson["_id"] as String)
    }

    protected fun createWorkflow(workflow: Workflow, refresh: Boolean = true): Workflow {
        return createWorkflowWithClient(client(), workflow, emptyList(), refresh)
    }

    protected fun Workflow.toHttpEntity(): HttpEntity {
        return StringEntity(toJsonString(), APPLICATION_JSON)
    }

    private fun Workflow.toJsonString(): String {
        val builder = XContentFactory.jsonBuilder()
        return shuffleXContent(toXContent(builder, ToXContent.EMPTY_PARAMS)).string()
    }

    protected fun getWorkflow(
        workflowId: String,
        header: BasicHeader = BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json"),
    ): Workflow {
        val response = client().makeRequest("GET", "$WORKFLOW_ALERTING_BASE_URI/$workflowId", null, header)
        assertEquals("Unable to get workflow $workflowId", RestStatus.OK, response.restStatus())

        val parser = createParser(XContentType.JSON.xContent(), response.entity.content)
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser)

        lateinit var id: String
        var version: Long = 0
        lateinit var workflow: Workflow

        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            parser.nextToken()

            when (parser.currentName()) {
                "_id" -> id = parser.text()
                "_version" -> version = parser.longValue()
                "workflow" -> workflow = Workflow.parse(parser)
            }
        }

        assertUserNull(workflow)
        return workflow.copy(id = id, version = version)
    }

    protected fun Workflow.relativeUrl() = "$WORKFLOW_ALERTING_BASE_URI/$id"
}

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

package org.opensearch.alerting.util

import org.apache.logging.log4j.LogManager
import org.opensearch.action.ActionListener
import org.opensearch.action.bulk.BulkRequest
import org.opensearch.action.bulk.BulkResponse
import org.opensearch.action.delete.DeleteRequest
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.WriteRequest
import org.opensearch.alerting.actionconverter.DestinationActionsConverter.Companion.convertDestinationToNotificationConfig
import org.opensearch.alerting.actionconverter.EmailAccountActionsConverter.Companion.convertEmailAccountToNotificationConfig
import org.opensearch.alerting.actionconverter.EmailGroupActionsConverter.Companion.convertEmailGroupToNotificationConfig
import org.opensearch.alerting.core.model.ScheduledJob
import org.opensearch.alerting.model.destination.Destination
import org.opensearch.alerting.model.destination.email.EmailAccount
import org.opensearch.alerting.model.destination.email.EmailGroup
import org.opensearch.client.node.NodeClient
import org.opensearch.common.Strings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParserUtils
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.ConfigConstants
import org.opensearch.commons.notifications.action.CreateNotificationConfigRequest
import org.opensearch.commons.notifications.model.NotificationConfig
import org.opensearch.commons.notifications.model.NotificationConfigInfo
import org.opensearch.index.query.QueryBuilders
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.search.fetch.subphase.FetchSourceContext
import java.time.Instant

class MigrationUtilService {

    companion object {

        private val logger = LogManager.getLogger(MigrationUtilService::class)

        @Volatile
        private var runningLock = false // in case 2 moveMetadata() process running

        // used in migration coordinator to cancel scheduled process
        @Volatile
        var finishFlag = false
            internal set

        fun migrateDestinations(client: NodeClient) {
            if (runningLock) {
                logger.info("There is a move metadata process running...")
                return
            } else if (finishFlag) {
                logger.info("Move metadata has finished.")
                return
            }
            try {
                runningLock = true

                val destinationsToMigrate = retrieveDestinationsToMigrate(client)
                logger.info("Need to migrate ${destinationsToMigrate.size} destinations")
                if (destinationsToMigrate.isEmpty()) {
                    finishFlag = true
                    runningLock = false
                    return
                }
                val migratedDestinations = createNotificationChannelIfNotExists(client, destinationsToMigrate)
                logger.info("Migrated ${migratedDestinations.size} destinations")
                val failedDeletedDestinations = deleteOldDestinations(client, migratedDestinations)
                logger.info("Failed to delete ${failedDeletedDestinations.size} destinations from migration process cleanup")
            } finally {
                runningLock = false
            }
        }

        private fun deleteOldDestinations(client: NodeClient, destinationIds: List<String>): List<String> {
            val bulkDeleteRequest = BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            destinationIds.forEach {
                val deleteRequest = DeleteRequest(ScheduledJob.SCHEDULED_JOBS_INDEX, it)
                bulkDeleteRequest.add(deleteRequest)
            }
            val failedToDeleteDestinations = mutableListOf<String>()
            var finishedExecution = false
            client.bulk(
                bulkDeleteRequest,
                object : ActionListener<BulkResponse> {
                    override fun onResponse(response: BulkResponse) {
                        failedToDeleteDestinations.addAll(response.items.filter { it.isFailed }.map { it.id })
                        finishedExecution = true
                    }

                    override fun onFailure(t: Exception) {
                        failedToDeleteDestinations.addAll(destinationIds)
                        finishedExecution = true
                        logger.error("Failed to delete destinations", t)
                    }
                }
            )
            while (!finishedExecution) {
                Thread.sleep(100)
            }
            return failedToDeleteDestinations
        }


        private fun createNotificationChannelIfNotExists(
            client: NodeClient,
            notificationConfigInfoList: List<Pair<NotificationConfigInfo, String>>
        ): List<String> {
            val migratedNotificationConfigs = mutableListOf<String>()
            notificationConfigInfoList.forEach {
                val notificationConfigInfo = it.first
                val userStr = it.second
                val createNotificationConfigRequest = CreateNotificationConfigRequest(notificationConfigInfo.notificationConfig, notificationConfigInfo.configId)
                try {
                    // TODO: recreate user object to pass along the same permissions. Make sure this works when user based security is removed
                    client.threadPool().threadContext.stashContext().use {
                        if (userStr.isNotBlank()) {
                            client.threadPool().threadContext.putTransient(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT, userStr)
                        }
                        val createResponse = NotificationAPIUtils.createNotificationConfig(client, createNotificationConfigRequest)
                        migratedNotificationConfigs.add(createResponse.configId)
                        logger.debug(("Migrated destination: ${createResponse.configId}"))
                    }
                } catch (e: Exception) {
                    if (e.message?.contains("version conflict, document already exists") == true) {
                        migratedNotificationConfigs.add(notificationConfigInfo.configId)
                    } else {
                        logger.warn(
                            "Failed to migrate over Destination ${notificationConfigInfo.configId} because failed to " +
                                "create channel in Notification plugin.",
                            e
                        )
                    }
                }
            }
            return migratedNotificationConfigs
        }

        private fun retrieveDestinationsToMigrate(client: NodeClient): List<Pair<NotificationConfigInfo, String>> {
            var start = 0
            val size = 100
            val notificationConfigInfoList = mutableListOf<Pair<NotificationConfigInfo, String>>()
            var hasMoreResults = true

            while (hasMoreResults) {
                val searchSourceBuilder = SearchSourceBuilder()
                    .size(size)
                    .from(start)
                    .fetchSource(FetchSourceContext(true, Strings.EMPTY_ARRAY, Strings.EMPTY_ARRAY))
                    .seqNoAndPrimaryTerm(true)
                    .version(true)
                val queryBuilder = QueryBuilders.boolQuery()
                    .should(QueryBuilders.existsQuery("email_account"))
                    .should(QueryBuilders.existsQuery("email_group"))
                    .should(QueryBuilders.existsQuery("destination"))
                searchSourceBuilder.query(queryBuilder)
                logger.info("Query running is: ${client.prepareSearch().setQuery(queryBuilder)}")

                val searchRequest = SearchRequest()
                    .source(searchSourceBuilder)
                    .indices(ScheduledJob.SCHEDULED_JOBS_INDEX)
                var finishedExecution = false
                client.search(
                    searchRequest,
                    object : ActionListener<SearchResponse> {
                        override fun onResponse(response: SearchResponse) {
                            if (response.hits.hits.isEmpty()) {
                                hasMoreResults = false
                            }
                            for (hit in response.hits) {
                                logger.info("Migration, found results: ${hit.sourceAsString}")
                                val xcp = XContentFactory.xContent(XContentType.JSON)
                                    .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, hit.sourceAsString)
                                var notificationConfig: NotificationConfig?
                                var userStr = ""
                                if (hit.sourceAsString.contains("\"email_group\"")) {
                                    val emailGroup = EmailGroup.parseWithType(xcp, hit.id, hit.version)
                                    notificationConfig = convertEmailGroupToNotificationConfig(emailGroup)
                                } else if (hit.sourceAsString.contains("\"email_account\"")) {
                                    val emailAccount = EmailAccount.parseWithType(xcp, hit.id, hit.version)
                                    notificationConfig = convertEmailAccountToNotificationConfig(emailAccount)
                                } else {
                                    XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
                                    XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, xcp.nextToken(), xcp)
                                    XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
                                    val destination = Destination.parse(
                                        xcp,
                                        hit.id,
                                        hit.version,
                                        hit.seqNo.toInt(),
                                        hit.primaryTerm.toInt()
                                    )
                                    userStr = destination.user.toString()
                                    notificationConfig = convertDestinationToNotificationConfig(destination)
                                }

                                if (notificationConfig != null)
                                    notificationConfigInfoList.add(
                                        Pair(
                                            NotificationConfigInfo(
                                                hit.id,
                                                Instant.now(),
                                                Instant.now(),
                                                notificationConfig
                                            ),
                                            userStr
                                        )
                                    )
                            }
                            finishedExecution = true
                        }

                        override fun onFailure(t: Exception) {
                            hasMoreResults = false
                            finishedExecution = true
                        }
                    }
                )
                while (!finishedExecution) {
                    Thread.sleep(100)
                }
                start += size
            }
            return notificationConfigInfoList
        }
    }
}

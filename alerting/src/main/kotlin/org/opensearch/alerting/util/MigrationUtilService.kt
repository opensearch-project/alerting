package org.opensearch.alerting.util

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import org.apache.logging.log4j.LogManager
import org.opensearch.action.ActionListener
import org.opensearch.action.bulk.BackoffPolicy
import org.opensearch.action.bulk.BulkRequest
import org.opensearch.action.bulk.BulkResponse
import org.opensearch.action.delete.DeleteRequest
import org.opensearch.action.delete.DeleteResponse
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.WriteRequest
import org.opensearch.alerting.actionconverter.DestinationActionsConverter.Companion.convertDestinationToNotificationConfig
import org.opensearch.alerting.core.model.ScheduledJob
import org.opensearch.alerting.model.destination.Destination
import org.opensearch.client.node.NodeClient
import org.opensearch.cluster.ClusterChangedEvent
import org.opensearch.cluster.ClusterStateListener
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.Strings
import org.opensearch.common.component.LifecycleListener
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParserUtils
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.notifications.action.CreateNotificationConfigRequest
import org.opensearch.commons.notifications.action.GetNotificationConfigRequest
import org.opensearch.index.query.QueryBuilders
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.search.fetch.subphase.FetchSourceContext
import java.util.stream.Collectors
import kotlin.coroutines.CoroutineContext
import kotlin.math.log

//make sure to do check during and after CDI/DDI since old node can index new destination
//TODO: migrate email account and email groups as well!
class MigrationUtilService(
) {

    companion object {

        private val logger = LogManager.getLogger(javaClass)

//    private lateinit var runnerSupervisor: Job
//    override val coroutineContext: CoroutineContext
//        get() = Dispatchers.Default + runnerSupervisor

        @Volatile
        private var runningLock = false // in case 2 moveMetadata() process running

        // used in coordinator sweep to cancel scheduled process
        @Volatile
        final var finishFlag = false
            private set

        fun reenableMigrationService() {
            finishFlag = false
        }

        private var destinations = mutableListOf<Destination>()

        //    @Suppress("MagicNumber")
        @Volatile
        private var retryPolicy =
            BackoffPolicy.constantBackoff(TimeValue.timeValueMillis(50), 3)

//    override fun doStart() {
//        runnerSupervisor = SupervisorJob()
//    }
//
//    override fun doStop() {
//        runnerSupervisor.cancel()
//    }

        @Suppress("ReturnCount", "LongMethod", "ComplexMethod")
        suspend fun migrateDestinations(client: NodeClient) {
            if (runningLock) {
                logger.info("There is a move metadata process running...")
                return
            } else if (finishFlag) {
                logger.info("Move metadata has finished.")
                return
            }
            try {
                runningLock = true

                val destinationsToMigrate = retrieveDestinations(client)
                logger.info("Need to migration ${destinationsToMigrate.size} destinations")
                if (destinationsToMigrate.isEmpty()) {
                    finishFlag = true
                    runningLock = false
                    return
                }
                val migratedDestinations = createNotificationChannelIfNotExists(client, destinationsToMigrate)
                logger.info("Migrated ${migratedDestinations.size} destinations")
                val failedDeletedDestinations = deleteOldDestinations(client, migratedDestinations)
                logger.info("Failed to delete ${failedDeletedDestinations.size} destinations")


            } finally {
                runningLock = false
            }
        }

        private suspend fun deleteOldDestinations(client: NodeClient, destinationIds: List<String>): List<String> {
            val bulkDeleteRequest = BulkRequest()
            destinationIds.forEach {
                val deleteRequest = DeleteRequest(ScheduledJob.SCHEDULED_JOBS_INDEX, it)
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                bulkDeleteRequest.add(deleteRequest)
            }
            val failedToDeleteDestinations = mutableListOf<String>()
            client.bulk(
                bulkDeleteRequest,
                object : ActionListener<BulkResponse> {
                    override fun onResponse(response: BulkResponse) {
                        failedToDeleteDestinations.addAll(response.items.filter { it.isFailed }.map { it.id })
                    }

                    override fun onFailure(t: Exception) {
                        failedToDeleteDestinations.addAll(destinationIds)
                    }
                }
            )
            return failedToDeleteDestinations
        }

        private suspend fun createNotificationChannelIfNotExists(client: NodeClient, destinations: List<Destination>): List<String> {
            val configIds: Set<String> = destinations.stream().map { it.id }.collect(Collectors.toSet())
            //TODO: scroll through all the data, so multiple calls are needed
            val getNotificationConfigRequest = GetNotificationConfigRequest(configIds)
            val getNotificationConfigResponse = NotificationAPIUtils.getNotificationConfig(client, getNotificationConfigRequest)
            val alreadyCreatedDestinations: List<String> = getNotificationConfigResponse?.searchResult?.objectList?.stream()?.map { it.configId }?.collect(Collectors.toList()) ?: emptyList()
            val nonExistentDestinations: List<Destination> = destinations.stream().filter { !alreadyCreatedDestinations.contains(it.id) }.collect(Collectors.toList())

            val migratedDestinations = mutableListOf<String>()
            //TODO: add hardening to retry
            nonExistentDestinations.forEach {
                val notificationConfig = convertDestinationToNotificationConfig(it)
                if (notificationConfig != null) {
                    val createNotificationConfigRequest = CreateNotificationConfigRequest(notificationConfig, it.id)
                    try {
                        val createResponse = NotificationAPIUtils.createNotificationConfig(client, createNotificationConfigRequest)
                        migratedDestinations.plus(createResponse.configId)
                    } catch (e: Exception) {
                        logger.warn("Failed to migrate over Destination ${it.id} because failed to create channel in Notification plugin.", e)
                    }
                }
            }
            migratedDestinations.addAll(alreadyCreatedDestinations)
            return migratedDestinations

        }

        private suspend fun retrieveDestinations(client: NodeClient): List<Destination> {
            var start = 0
            val size = 100
            var destinations = mutableListOf<Destination>()
            var hasMoreResults = true

            while (hasMoreResults) {
                val searchSourceBuilder = SearchSourceBuilder()
                    .size(size)
                    .from(start)
                    .fetchSource(FetchSourceContext(true, Strings.EMPTY_ARRAY, Strings.EMPTY_ARRAY))
                    .seqNoAndPrimaryTerm(true)
                    .version(true)
                val queryBuilder = QueryBuilders.boolQuery()
                    .must(QueryBuilders.existsQuery("destination"))
                searchSourceBuilder.query(queryBuilder)

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
                            logger.info("Getting destinations for migration and found ${response.hits.totalHits?.value?.toInt()}")
                            for (hit in response.hits) {
                                val id = hit.id
                                val version = hit.version
                                val seqNo = hit.seqNo.toInt()
                                val primaryTerm = hit.primaryTerm.toInt()
                                val xcp = XContentFactory.xContent(XContentType.JSON)
                                    .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, hit.sourceAsString)
                                XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
                                XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, xcp.nextToken(), xcp)
                                XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
                                destinations.add(Destination.parse(xcp, id, version, seqNo, primaryTerm))
                            }
                            logger.info("Getting destinations for migration and got ${destinations.size}")
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
            logger.info("Returning these many destinations: ${destinations.size} with start being at: ")
            return destinations
        }
    }

}

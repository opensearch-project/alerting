/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.ActionRequest
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.index.IndexResponse
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.alerts.AlertIndices
import org.opensearch.alerting.notes.NotesIndices
import org.opensearch.alerting.notes.NotesIndices.Companion.NOTES_HISTORY_WRITE_INDEX
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.settings.AlertingSettings.Companion.INDEX_TIMEOUT
import org.opensearch.alerting.util.AlertingException
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.action.IndexNoteRequest
import org.opensearch.commons.alerting.action.IndexNoteResponse
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.commons.alerting.model.Note
import org.opensearch.commons.authuser.User
import org.opensearch.commons.utils.recreateObject
import org.opensearch.core.action.ActionListener
import org.opensearch.core.common.io.stream.NamedWriteableRegistry
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParserUtils
import org.opensearch.index.query.QueryBuilders
import org.opensearch.rest.RestRequest
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import java.time.Instant

private val log = LogManager.getLogger(TransportIndexMonitorAction::class.java)
private val scope: CoroutineScope = CoroutineScope(Dispatchers.IO)

class TransportIndexNoteAction
@Inject
constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val notesIndices: NotesIndices,
    val clusterService: ClusterService,
    val settings: Settings,
    val xContentRegistry: NamedXContentRegistry,
    val namedWriteableRegistry: NamedWriteableRegistry,
) : HandledTransportAction<ActionRequest, IndexNoteResponse>(
    AlertingActions.INDEX_NOTE_ACTION_NAME,
    transportService,
    actionFilters,
    ::IndexNoteRequest,
),
    SecureTransportAction {
    //    @Volatile private var maxNotes = AlertingSettings.ALERTING_MAX_NOTES.get(settings) // TODO add this setting
//    @Volatile private var requestTimeout = AlertingSettings.ALERTING_MAX_SIZE.get(settings) // TODO add this setting
    @Volatile private var indexTimeout = AlertingSettings.INDEX_TIMEOUT.get(settings)

    // Notes don't really use filterBy setting, this is only here to implement SecureTransportAction interface so that we can
    // use readUserFromThreadContext()
    @Volatile override var filterByEnabled = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
//        clusterService.clusterSettings.addSettingsUpdateConsumer(ALERTING_MAX_NOTES) { maxMonitors = it }
//        clusterService.clusterSettings.addSettingsUpdateConsumer(ALERTING_MAX_SIZE) { requestTimeout = it }
        clusterService.clusterSettings.addSettingsUpdateConsumer(INDEX_TIMEOUT) { indexTimeout = it }
        listenFilterBySettingChange(clusterService)
    }

    override fun doExecute(
        task: Task,
        request: ActionRequest,
        actionListener: ActionListener<IndexNoteResponse>,
    ) {
        val transformedRequest =
            request as? IndexNoteRequest
                ?: recreateObject(request, namedWriteableRegistry) {
                    IndexNoteRequest(it)
                }
        val user = readUserFromThreadContext(client)

        client.threadPool().threadContext.stashContext().use {
            scope.launch {
                IndexNoteHandler(client, actionListener, transformedRequest, user).start()
            }
        }
    }

    inner class IndexNoteHandler(
        private val client: Client,
        private val actionListener: ActionListener<IndexNoteResponse>,
        private val request: IndexNoteRequest,
        private val user: User?,
    ) {
        suspend fun start() {
            notesIndices.createOrUpdateInitialNotesHistoryIndex()
            prepareNotesIndexing()
//                alertingNotesIndices.initNotesIndex(
//                    object : ActionListener<CreateIndexResponse> {
//                        override fun onResponse(response: CreateIndexResponse) {
//                            onCreateMappingsResponse(response.isAcknowledged)
//                        }
//
//                        override fun onFailure(t: Exception) {
//                            // https://github.com/opensearch-project/alerting/issues/646
//                            if (ExceptionsHelper.unwrapCause(t) is ResourceAlreadyExistsException) {
//                                scope.launch {
//                                    // Wait for the yellow status
//                                    val request =
//                                        ClusterHealthRequest()
//                                            .indices(NOTES_INDEX)
//                                            .waitForYellowStatus()
//                                    val response: ClusterHealthResponse =
//                                        client.suspendUntil {
//                                            execute(ClusterHealthAction.INSTANCE, request, it)
//                                        }
//                                    if (response.isTimedOut) {
//                                        log.error("Workflow creation timeout", t)
//                                        actionListener.onFailure(
//                                            OpenSearchException("Cannot determine that the $NOTES_INDEX index is healthy"),
//                                        )
//                                    }
//                                    // Retry mapping of workflow
//                                    onCreateMappingsResponse(true)
//                                }
//                            } else {
//                                log.error("Failed to create workflow", t)
//                                actionListener.onFailure(AlertingException.wrap(t))
//                            }
//                        }
//                    },
//                )
//            } else if (!IndexUtils.notesIndexUpdated) {
//                IndexUtils.updateIndexMapping(
//                    NOTES_INDEX,
//                    NotesIndices.notesMapping(),
//                    clusterService.state(),
//                    client.admin().indices(),
//                    object : ActionListener<AcknowledgedResponse> {
//                        override fun onResponse(response: AcknowledgedResponse) {
//                            onUpdateMappingsResponse(response)
//                        }
//
//                        override fun onFailure(t: Exception) {
//                            log.error("Failed to create workflow", t)
//                            actionListener.onFailure(AlertingException.wrap(t))
//                        }
//                    },
//                )
//            } else {
//                prepareNotesIndexing()
//            }
        }

//        private suspend fun onCreateMappingsResponse(isAcknowledged: Boolean) {
//            if (isAcknowledged) {
//                log.info("Created $NOTES_INDEX with mappings.")
//                prepareNotesIndexing()
//                IndexUtils.notesIndexUpdated()
//            } else {
//                log.info("Create $NOTES_INDEX mappings call not acknowledged.")
//                actionListener.onFailure(
//                    AlertingException.wrap(
//                        OpenSearchStatusException(
//                            "Create $NOTES_INDEX mappings call not acknowledged",
//                            RestStatus.INTERNAL_SERVER_ERROR,
//                        ),
//                    ),
//                )
//            }
//        }
//
//        private suspend fun onUpdateMappingsResponse(response: AcknowledgedResponse) {
//            if (response.isAcknowledged) {
//                log.info("Updated $NOTES_INDEX with mappings.")
//                IndexUtils.scheduledJobIndexUpdated()
//                prepareNotesIndexing()
//            } else {
//                log.error("Update $NOTES_INDEX mappings call not acknowledged.")
//                actionListener.onFailure(
//                    AlertingException.wrap(
//                        OpenSearchStatusException(
//                            "Updated $NOTES_INDEX mappings call not acknowledged.",
//                            RestStatus.INTERNAL_SERVER_ERROR,
//                        ),
//                    ),
//                )
//            }
//        }

        private suspend fun prepareNotesIndexing() {
            // TODO: refactor, create Note object from request here, then pass into updateNote() and indexNote()
            if (request.method == RestRequest.Method.PUT) {
                updateNote()
            } else {
                indexNote()
            }
        }

        private suspend fun indexNote() {
            // need to validate the existence of the Alert that user is trying to add Note to.
            // Also need to check if user has permissions to add a Note to the passed in Alert. To do this,
            // we retrieve the Alert to get its associated monitor user, and use that to
            // check if they have permissions to the Monitor that generated the Alert
            val queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.termsQuery("_id", listOf(request.alertId)))
            val searchSourceBuilder =
                SearchSourceBuilder()
                    .version(true)
                    .seqNoAndPrimaryTerm(true)
                    .query(queryBuilder)

            // search all alerts, since user might want to create a note
            // on a completed alert
            val searchRequest =
                SearchRequest()
                    .indices(AlertIndices.ALL_ALERT_INDEX_PATTERN)
                    .source(searchSourceBuilder)

            val searchResponse: SearchResponse = client.suspendUntil { search(searchRequest, it) }
            val alerts = searchResponse.hits.map { hit ->
                val xcp = XContentHelper.createParser(
                    NamedXContentRegistry.EMPTY,
                    LoggingDeprecationHandler.INSTANCE,
                    hit.sourceRef,
                    XContentType.JSON
                )
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
                val alert = Alert.parse(xcp, hit.id, hit.version)
                alert
            }

            if (alerts.isEmpty()) {
                actionListener.onFailure(
                    AlertingException.wrap(
                        OpenSearchStatusException("Alert with ID ${request.alertId} is not found", RestStatus.NOT_FOUND),
                    )
                )
            }

            val alert = alerts[0] // there should only be 1 Alert that matched the request alert ID
            log.info("checking user permissions in index note")
            checkUserPermissionsWithResource(user, alert.monitorUser, actionListener, "monitor", alert.monitorId)

            val note = Note(alertId = request.alertId, content = request.content, time = Instant.now(), user = user)

            val indexRequest =
                IndexRequest(NOTES_HISTORY_WRITE_INDEX)
                    .source(note.toXContentWithUser(XContentFactory.jsonBuilder()))
                    .setIfSeqNo(request.seqNo)
                    .setIfPrimaryTerm(request.primaryTerm)
                    .timeout(indexTimeout)

            log.info("Creating new note: ${note.toXContentWithUser(XContentFactory.jsonBuilder())}")

            try {
                val indexResponse: IndexResponse = client.suspendUntil { client.index(indexRequest, it) }
                val failureReasons = checkShardsFailure(indexResponse)
                if (failureReasons != null) {
                    actionListener.onFailure(
                        AlertingException.wrap(OpenSearchStatusException(failureReasons.toString(), indexResponse.status())),
                    )
                    return
                }

                actionListener.onResponse(
                    IndexNoteResponse(indexResponse.id, indexResponse.seqNo, indexResponse.primaryTerm, note)
                )
            } catch (t: Exception) {
                actionListener.onFailure(AlertingException.wrap(t))
            }
        }

        private suspend fun updateNote() {
            val getRequest = GetRequest(NOTES_HISTORY_WRITE_INDEX, request.noteId)
            try {
                val getResponse: GetResponse = client.suspendUntil { client.get(getRequest, it) }
                if (!getResponse.isExists) {
                    actionListener.onFailure(
                        AlertingException.wrap(
                            OpenSearchStatusException("Note with ${request.noteId} is not found", RestStatus.NOT_FOUND),
                        ),
                    )
                    return
                }
                val xcp =
                    XContentHelper.createParser(
                        xContentRegistry,
                        LoggingDeprecationHandler.INSTANCE,
                        getResponse.sourceAsBytesRef,
                        XContentType.JSON,
                    )
                log.info("curr token: ${xcp.currentToken()}")
                xcp.nextToken()
                log.info("next token: ${xcp.currentToken()}")
                val note = Note.parse(xcp, getResponse.id)
                log.info("getResponse.id: ${getResponse.id}")
                log.info("note: $note")
                onGetNoteResponse(note)
            } catch (t: Exception) {
                actionListener.onFailure(AlertingException.wrap(t))
            }
        }

        private suspend fun onGetNoteResponse(currentNote: Note) {
            // TODO: where to update time field with creation or last update time, right now it's declared below, but other APIs seem to
            // TODO: declare create/update time in the RestHandler class

//            if (user == null || currentNote.user == null) {
//                // security is not installed, editing notes is not allowed
//                AlertingException.wrap(
//                    OpenSearchStatusException(
//                        "Editing Alerting notes is prohibited when the Security plugin is not installed",
//                        RestStatus.FORBIDDEN,
//                    ),
//                )
//            }

            // check that the user has permissions to edit the note. user can edit note if
            // - user is Admin
            // - user is the author of the note
            if (user != null && !isAdmin(user) && user.name != currentNote.user?.name) {
                actionListener.onFailure(
                    AlertingException.wrap(
                        OpenSearchStatusException(
                            "Note ${request.noteId} created by ${currentNote.user} " +
                                "can only be edited by Admin or ${currentNote.user} ",
                            RestStatus.FORBIDDEN,
                        ),
                    ),
                )
            }

            // TODO: ensure usage of Instant.now() is consistent with index monitor
            // retains everything from the original note except content and time
            val requestNote =
                Note(
                    id = currentNote.id,
                    alertId = currentNote.alertId,
                    content = request.content,
                    time = Instant.now(),
                    user = currentNote.user,
                )

            val indexRequest =
                IndexRequest(NOTES_HISTORY_WRITE_INDEX)
                    .source(requestNote.toXContentWithUser(XContentFactory.jsonBuilder()))
                    .id(requestNote.id)
                    .setIfSeqNo(request.seqNo)
                    .setIfPrimaryTerm(request.primaryTerm)
                    .timeout(indexTimeout)

            log.info(
                "Updating note, ${currentNote.id}, from: " +
                    "${currentNote.content} to: " +
                    requestNote.content,
            )

            try {
                val indexResponse: IndexResponse = client.suspendUntil { client.index(indexRequest, it) }
                val failureReasons = checkShardsFailure(indexResponse)
                if (failureReasons != null) {
                    actionListener.onFailure(
                        AlertingException.wrap(OpenSearchStatusException(failureReasons.toString(), indexResponse.status())),
                    )
                    return
                }

                actionListener.onResponse(
                    IndexNoteResponse(
                        indexResponse.id,
                        indexResponse.seqNo,
                        indexResponse.primaryTerm,
                        requestNote,
                    ),
                )
            } catch (t: Exception) {
                actionListener.onFailure(AlertingException.wrap(t))
            }
        }

        private fun checkShardsFailure(response: IndexResponse): String? {
            val failureReasons = StringBuilder()
            if (response.shardInfo.failed > 0) {
                response.shardInfo.failures.forEach { entry ->
                    failureReasons.append(entry.reason())
                }
                return failureReasons.toString()
            }
            return null
        }
    }
}

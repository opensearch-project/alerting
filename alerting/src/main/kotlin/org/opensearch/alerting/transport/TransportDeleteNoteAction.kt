package org.opensearch.alerting.transport

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.ActionRequest
import org.opensearch.action.delete.DeleteRequest
import org.opensearch.action.delete.DeleteResponse
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.notes.NotesIndices.Companion.ALL_NOTES_INDEX_PATTERN
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.alerting.util.AlertingException
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.action.DeleteNoteRequest
import org.opensearch.commons.alerting.action.DeleteNoteResponse
import org.opensearch.commons.alerting.model.Note
import org.opensearch.commons.authuser.User
import org.opensearch.commons.utils.recreateObject
import org.opensearch.core.action.ActionListener
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParserUtils
import org.opensearch.index.query.QueryBuilders
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService

private val scope: CoroutineScope = CoroutineScope(Dispatchers.IO)
private val log = LogManager.getLogger(TransportDeleteNoteAction::class.java)

class TransportDeleteNoteAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    settings: Settings,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<ActionRequest, DeleteNoteResponse>(
    AlertingActions.DELETE_NOTES_ACTION_NAME, transportService, actionFilters, ::DeleteNoteRequest
),
    SecureTransportAction {

    @Volatile override var filterByEnabled = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        listenFilterBySettingChange(clusterService)
    }

    override fun doExecute(task: Task, request: ActionRequest, actionListener: ActionListener<DeleteNoteResponse>) {
        val transformedRequest = request as? DeleteNoteRequest
            ?: recreateObject(request) { DeleteNoteRequest(it) }

        val user = readUserFromThreadContext(client)
//        val deleteRequest = DeleteRequest(ALL_NOTES_INDEX_PATTERN, transformedRequest.noteId)

        if (!validateUserBackendRoles(user, actionListener)) {
            return
        }
        scope.launch {
            DeleteNoteHandler(
                client,
                actionListener,
                user,
                transformedRequest.noteId
            ).resolveUserAndStart()
        }
    }

    inner class DeleteNoteHandler(
        private val client: Client,
        private val actionListener: ActionListener<DeleteNoteResponse>,
        private val user: User?,
        private val noteId: String
    ) {

        private var sourceIndex: String? = null
        suspend fun resolveUserAndStart() {
            try {
                val note = getNote()

                if (sourceIndex == null) {
                    actionListener.onFailure(
                        AlertingException(
                            "Could not resolve the index the given Note came from",
                            RestStatus.INTERNAL_SERVER_ERROR,
                            IllegalStateException()
                        )
                    )
                }

                // if user is null because security plugin is not installed, anyone can delete any note
                // otherwise, only allow note deletion if the deletion requester is the same as the note's author
                val canDelete = user == null || user.name == note.user?.name || isAdmin(user)

                val deleteRequest = DeleteRequest(sourceIndex, noteId)

                if (canDelete) {
                    val deleteResponse = deleteNote(deleteRequest)
                    actionListener.onResponse(DeleteNoteResponse(deleteResponse.id))
                } else {
                    actionListener.onFailure(
                        AlertingException("Not allowed to delete this note!", RestStatus.FORBIDDEN, IllegalStateException())
                    )
                }
            } catch (t: Exception) {
                log.error("Failed to delete note $noteId", t)
                actionListener.onFailure(AlertingException.wrap(t))
            }
        }

        private suspend fun getNote(): Note {
            val queryBuilder = QueryBuilders
                .boolQuery()
                .must(QueryBuilders.termsQuery("_id", noteId))
            val searchSourceBuilder =
                SearchSourceBuilder()
                    .version(true)
                    .seqNoAndPrimaryTerm(true)
                    .query(queryBuilder)
            val searchRequest = SearchRequest()
                .source(searchSourceBuilder)
                .indices(ALL_NOTES_INDEX_PATTERN)

            val searchResponse: SearchResponse = client.suspendUntil { search(searchRequest, it) }
            if (searchResponse.hits.totalHits.value == 0L) {
                actionListener.onFailure(
                    AlertingException.wrap(
                        OpenSearchStatusException("Note with $noteId is not found", RestStatus.NOT_FOUND)
                    )
                )
            }
            val notes = searchResponse.hits.map { hit ->
                val xcp = XContentHelper.createParser(
                    NamedXContentRegistry.EMPTY,
                    LoggingDeprecationHandler.INSTANCE,
                    hit.sourceRef,
                    XContentType.JSON
                )
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
                val note = Note.parse(xcp, hit.id)
                sourceIndex = hit.index
                note
            }

            return notes[0] // we searched on Note ID, there should only be one Note in the List
        }
    }

    private suspend fun deleteNote(deleteRequest: DeleteRequest): DeleteResponse {
        log.debug("Deleting the note with id ${deleteRequest.id()}")
        return client.suspendUntil { delete(deleteRequest, it) }
    }
}

package org.opensearch.alerting.util

import org.apache.logging.log4j.LogManager
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.alerting.notes.NotesIndices.Companion.ALL_NOTES_INDEX_PATTERN
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.client.Client
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.model.Note
import org.opensearch.core.action.ActionListener
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParserUtils
import org.opensearch.index.query.QueryBuilders
import org.opensearch.index.reindex.BulkByScrollResponse
import org.opensearch.index.reindex.DeleteByQueryAction
import org.opensearch.index.reindex.DeleteByQueryRequestBuilder
import org.opensearch.search.builder.SearchSourceBuilder
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

private val log = LogManager.getLogger(NotesUtils::class.java)

class NotesUtils {
    companion object {
        // Deletes all Notes given by the list of Notes IDs
        suspend fun deleteNotes(client: Client, noteIDs: List<String>) {
            if (noteIDs.isEmpty()) return
            val deleteResponse: BulkByScrollResponse = suspendCoroutine { cont ->
                DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE)
                    .source(ALL_NOTES_INDEX_PATTERN)
                    .filter(QueryBuilders.boolQuery().must(QueryBuilders.termsQuery("_id", noteIDs)))
                    .refresh(true)
                    .execute(
                        object : ActionListener<BulkByScrollResponse> {
                            override fun onResponse(response: BulkByScrollResponse) = cont.resume(response)
                            override fun onFailure(t: Exception) = cont.resumeWithException(t)
                        }
                    )
            }
            deleteResponse.bulkFailures.forEach {
                log.error("Failed to delete Note. Note ID: [${it.id}] cause: [${it.cause}] ")
            }
        }

        // Searches through all Notes history indices and returns a list of all Notes associated
        // with the Entities given by the list of Entity IDs
        // TODO: change this to EntityIDs
        suspend fun getNotesByAlertIDs(client: Client, alertIDs: List<String>): List<Note> {
            val queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.termsQuery("alert_id", alertIDs))
            val searchSourceBuilder =
                SearchSourceBuilder()
                    .version(true)
                    .seqNoAndPrimaryTerm(true)
                    .query(queryBuilder)

            val searchRequest =
                SearchRequest()
                    .indices(ALL_NOTES_INDEX_PATTERN)
                    .source(searchSourceBuilder)

            val searchResponse: SearchResponse = client.suspendUntil { search(searchRequest, it) }
            val notes = searchResponse.hits.map { hit ->
                val xcp = XContentHelper.createParser(
                    NamedXContentRegistry.EMPTY,
                    LoggingDeprecationHandler.INSTANCE,
                    hit.sourceRef,
                    XContentType.JSON
                )
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
                val note = Note.parse(xcp, hit.id)
                note
            }

            return notes
        }

        // Identical to getNotesByAlertIDs, just returns list of Note IDs instead of list of Note objects
        suspend fun getNoteIDsByAlertIDs(client: Client, alertIDs: List<String>): List<String> {
            val notes = getNotesByAlertIDs(client, alertIDs)
            return notes.map { it.id }
        }
    }
}

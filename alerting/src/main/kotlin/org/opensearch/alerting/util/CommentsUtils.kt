/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import org.apache.logging.log4j.LogManager
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.alerting.comments.CommentsIndices.Companion.ALL_COMMENTS_INDEX_PATTERN
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.client.Client
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.model.Comment
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

private val log = LogManager.getLogger(CommentsUtils::class.java)

class CommentsUtils {
    companion object {
        // Deletes all Comments given by the list of Comments IDs
        suspend fun deleteComments(client: Client, commentIDs: List<String>) {
            if (commentIDs.isEmpty()) return
            val deleteResponse: BulkByScrollResponse = suspendCoroutine { cont ->
                DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE)
                    .source(ALL_COMMENTS_INDEX_PATTERN)
                    .filter(QueryBuilders.boolQuery().must(QueryBuilders.termsQuery("_id", commentIDs)))
                    .refresh(true)
                    .execute(
                        object : ActionListener<BulkByScrollResponse> {
                            override fun onResponse(response: BulkByScrollResponse) = cont.resume(response)
                            override fun onFailure(t: Exception) = cont.resumeWithException(t)
                        }
                    )
            }
            deleteResponse.bulkFailures.forEach {
                log.error("Failed to delete Comment. Comment ID: [${it.id}] cause: [${it.cause}] ")
            }
        }

        // Searches through all Comments history indices and returns a list of all Comments associated
        // with the Entities given by the list of Entity IDs
        // TODO: change this to EntityIDs
        suspend fun getCommentsByAlertIDs(client: Client, alertIDs: List<String>): List<Comment> {
            val queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.termsQuery("alert_id", alertIDs))
            val searchSourceBuilder =
                SearchSourceBuilder()
                    .version(true)
                    .seqNoAndPrimaryTerm(true)
                    .query(queryBuilder)

            val searchRequest =
                SearchRequest()
                    .indices(ALL_COMMENTS_INDEX_PATTERN)
                    .source(searchSourceBuilder)

            val searchResponse: SearchResponse = client.suspendUntil { search(searchRequest, it) }
            val comments = searchResponse.hits.map { hit ->
                val xcp = XContentHelper.createParser(
                    NamedXContentRegistry.EMPTY,
                    LoggingDeprecationHandler.INSTANCE,
                    hit.sourceRef,
                    XContentType.JSON
                )
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
                val comment = Comment.parse(xcp, hit.id)
                comment
            }

            return comments
        }

        // Identical to getCommentsByAlertIDs, just returns list of Comment IDs instead of list of Comment objects
        suspend fun getCommentIDsByAlertIDs(client: Client, alertIDs: List<String>): List<String> {
            val comments = getCommentsByAlertIDs(client, alertIDs)
            return comments.map { it.id }
        }

        // TODO: make getCommentsByAlertID and getCommentIDsByAlertID
    }
}

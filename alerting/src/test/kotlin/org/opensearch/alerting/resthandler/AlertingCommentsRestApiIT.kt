/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.apache.http.entity.ContentType
import org.apache.http.entity.StringEntity
import org.opensearch.alerting.AlertingPlugin.Companion.COMMENTS_BASE_URI
import org.opensearch.alerting.AlertingRestTestCase
import org.opensearch.alerting.makeRequest
import org.opensearch.alerting.randomAlert
import org.opensearch.alerting.settings.AlertingSettings.Companion.ALERTING_COMMENTS_ENABLED
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.commons.alerting.model.Comment.Companion.COMMENT_CONTENT_FIELD
import org.opensearch.commons.alerting.util.string
import org.opensearch.core.rest.RestStatus
import org.opensearch.index.query.QueryBuilders
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.test.junit.annotations.TestLogging
import java.util.concurrent.TimeUnit

@TestLogging("level:DEBUG", reason = "Debug for tests.")
@Suppress("UNCHECKED_CAST")
class AlertingCommentsRestApiIT : AlertingRestTestCase() {

    fun `test creating comment`() {
        client().updateSettings(ALERTING_COMMENTS_ENABLED.key, "true")

        val monitor = createRandomMonitor(refresh = true)
        val alert = createAlert(randomAlert(monitor).copy(state = Alert.State.ACTIVE))
        val alertId = alert.id
        val commentContent = "test comment"

        val comment = createAlertComment(alertId, commentContent)

        assertEquals("Comment does not have correct content", commentContent, comment.content)
        assertEquals("Comment does not have correct alert ID", alertId, comment.entityId)
    }

    fun `test updating comment`() {
        client().updateSettings(ALERTING_COMMENTS_ENABLED.key, "true")

        val monitor = createRandomMonitor(refresh = true)
        val alert = createAlert(randomAlert(monitor).copy(state = Alert.State.ACTIVE))
        val alertId = alert.id
        val commentContent = "test comment"

        val commentId = createAlertComment(alertId, commentContent).id

        val updateContent = "updated comment"
        val updateRequestBody = XContentFactory.jsonBuilder()
            .startObject()
            .field(COMMENT_CONTENT_FIELD, updateContent)
            .endObject()
            .string()

        val updateResponse = client().makeRequest(
            "PUT",
            "$COMMENTS_BASE_URI/$commentId",
            StringEntity(updateRequestBody, ContentType.APPLICATION_JSON)
        )

        assertEquals("Update comment failed", RestStatus.OK, updateResponse.restStatus())

        val updateResponseBody = updateResponse.asMap()

        val comment = updateResponseBody["comment"] as Map<*, *>
        val actualContent = comment["content"] as String
        assertEquals("Comment does not have correct content after update", updateContent, actualContent)
    }

    fun `test searching single comment by alert id`() {
        client().updateSettings(ALERTING_COMMENTS_ENABLED.key, "true")

        val monitor = createRandomMonitor(refresh = true)
        val alert = createAlert(randomAlert(monitor).copy(state = Alert.State.ACTIVE))
        val alertId = alert.id
        val commentContent = "test comment"

        createAlertComment(alertId, commentContent)

        OpenSearchTestCase.waitUntil({
            return@waitUntil false
        }, 3, TimeUnit.SECONDS)

        val search = SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).toString()
        val searchResponse = client().makeRequest(
            "GET",
            "$COMMENTS_BASE_URI/_search",
            StringEntity(search, ContentType.APPLICATION_JSON)
        )

        val xcp = createParser(XContentType.JSON.xContent(), searchResponse.entity.content)
        val hits = xcp.map()["hits"]!! as Map<String, Map<String, Any>>
        logger.info("hits: $hits")
        val numberDocsFound = hits["total"]?.get("value")
        assertEquals("No Comments found", 1, numberDocsFound)

        val searchHits = hits["hits"] as List<*>
        val hit = searchHits[0] as Map<*, *>
        val commentHit = hit["_source"] as Map<*, *>
        assertEquals("returned Comment does not match alert id in search query", alertId, commentHit["entity_id"])
        assertEquals("returned Comment does not have expected content", commentContent, commentHit["content"])
    }

    fun `test deleting comments`() {
        client().updateSettings(ALERTING_COMMENTS_ENABLED.key, "true")

        val monitor = createRandomMonitor(refresh = true)
        val alert = createAlert(randomAlert(monitor).copy(state = Alert.State.ACTIVE))
        val alertId = alert.id
        val commentContent = "test comment"

        val commentId = createAlertComment(alertId, commentContent).id
        OpenSearchTestCase.waitUntil({
            return@waitUntil false
        }, 3, TimeUnit.SECONDS)

        val deleteResponse = client().makeRequest(
            "DELETE",
            "$COMMENTS_BASE_URI/$commentId"
        )

        assertEquals("Delete comment failed", RestStatus.OK, deleteResponse.restStatus())

        val deleteResponseBody = deleteResponse.asMap()

        val deletedCommentId = deleteResponseBody["_id"] as String
        assertEquals("Deleted Comment ID does not match Comment ID in delete request", commentId, deletedCommentId)
    }

    // TODO: test list
    /*
    create comment with empty content should fail
    create without alert id should fail
    update without comment id should fail
    search comments across multiple alerts
    (belongs in CommentsIT) create comment thats too large based on cluster setting should fail
    create comment on alert that alrdy has max comments based on cluster setting should fail
    create comment on alert user doesn't have backend roles to view should fail
    search comment on alert user doesn't have backend roles to view should fail
    comments are shown in notifications for query monitor
    comments are shown in notifications for bucket monitor
    (belongs in CommentsIT) update comment that user didn't author should fail
    (belongs in CommentsIT) delete comment that user didn't author should fail
    (belongs in CommentsIT) update comment that user didn't author but user is Admin should succeed
     */
}

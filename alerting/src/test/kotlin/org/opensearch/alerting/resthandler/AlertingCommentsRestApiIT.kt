/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.opensearch.alerting.AlertingRestTestCase
import org.opensearch.alerting.randomAlert
import org.opensearch.alerting.settings.AlertingSettings.Companion.ALERTING_COMMENTS_ENABLED
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.index.query.QueryBuilders
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.test.junit.annotations.TestLogging

@TestLogging("level:DEBUG", reason = "Debug for tests.")
@Suppress("UNCHECKED_CAST")
class AlertingCommentsRestApiIT : AlertingRestTestCase() {

    fun `test creating comment`() {
        client().updateSettings(ALERTING_COMMENTS_ENABLED.key, "true")

        val monitor = createRandomMonitor(refresh = true)
        val alert = createAlert(randomAlert(monitor).copy(state = Alert.State.ACTIVE))
        val alertId = alert.id
        val commentContent = "test comment"

        val comment = createAlertComment(alertId, commentContent, client())

        assertEquals("Comment does not have correct content", commentContent, comment.content)
        assertEquals("Comment does not have correct alert ID", alertId, comment.entityId)
    }

    fun `test updating comment`() {
        client().updateSettings(ALERTING_COMMENTS_ENABLED.key, "true")

        val monitor = createRandomMonitor(refresh = true)
        val alert = createAlert(randomAlert(monitor).copy(state = Alert.State.ACTIVE))
        val alertId = alert.id
        val commentContent = "test comment"

        val commentId = createAlertComment(alertId, commentContent, client()).id

        val updateContent = "updated comment"
        val actualContent = updateAlertComment(commentId, updateContent, client()).content

        assertEquals("Comment does not have correct content after update", updateContent, actualContent)
    }

    fun `test searching single comment by alert id`() {
        client().updateSettings(ALERTING_COMMENTS_ENABLED.key, "true")

        val monitor = createRandomMonitor(refresh = true)
        val alert = createAlert(randomAlert(monitor).copy(state = Alert.State.ACTIVE))
        val alertId = alert.id
        val commentContent = "test comment"

        createAlertComment(alertId, commentContent, client())

        val search = SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
        val xcp = searchAlertComments(search, client())

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

        val commentId = createAlertComment(alertId, commentContent, client()).id

        val deletedCommentId = deleteAlertComment(commentId, client())

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

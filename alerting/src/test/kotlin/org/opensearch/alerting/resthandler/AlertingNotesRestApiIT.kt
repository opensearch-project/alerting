/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.apache.hc.core5.http.ContentType
import org.apache.hc.core5.http.io.entity.StringEntity
import org.opensearch.alerting.ALERTING_BASE_URI
import org.opensearch.alerting.AlertingRestTestCase
import org.opensearch.alerting.makeRequest
import org.opensearch.alerting.randomAlert
import org.opensearch.alerting.settings.AlertingSettings.Companion.ALERTING_NOTES_ENABLED
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.commons.alerting.model.Note.Companion.NOTE_CONTENT_FIELD
import org.opensearch.commons.alerting.util.string
import org.opensearch.core.rest.RestStatus
import org.opensearch.index.query.QueryBuilders
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.test.junit.annotations.TestLogging
import java.util.concurrent.TimeUnit

@TestLogging("level:DEBUG", reason = "Debug for tests.")
@Suppress("UNCHECKED_CAST")
class AlertingNotesRestApiIT : AlertingRestTestCase() {

    fun `test creating note`() {
        client().updateSettings(ALERTING_NOTES_ENABLED.key, "true")

        val monitor = createRandomMonitor(refresh = true)
        val alert = createAlert(randomAlert(monitor).copy(state = Alert.State.ACTIVE))
        val alertId = alert.id
        val noteContent = "test note"

        val note = createAlertNote(alertId, noteContent)

        assertEquals("Note does not have correct content", noteContent, note.content)
        assertEquals("Note does not have correct alert ID", alertId, note.entityId)
    }

    fun `test updating note`() {
        client().updateSettings(ALERTING_NOTES_ENABLED.key, "true")

        val monitor = createRandomMonitor(refresh = true)
        val alert = createAlert(randomAlert(monitor).copy(state = Alert.State.ACTIVE))
        val alertId = alert.id
        val noteContent = "test note"

        val noteId = createAlertNote(alertId, noteContent).id

        val updateContent = "updated note"
        val updateRequestBody = XContentFactory.jsonBuilder()
            .startObject()
            .field(NOTE_CONTENT_FIELD, updateContent)
            .endObject()
            .string()

        val updateResponse = client().makeRequest(
            "PUT",
            "$ALERTING_BASE_URI/alerts/notes/$noteId",
            StringEntity(updateRequestBody, ContentType.APPLICATION_JSON)
        )

        assertEquals("Update note failed", RestStatus.OK, updateResponse.restStatus())

        val updateResponseBody = updateResponse.asMap()

        val note = updateResponseBody["note"] as Map<*, *>
        val actualContent = note["content"] as String
        assertEquals("Note does not have correct content after update", updateContent, actualContent)
    }

    fun `test searching single note by alert id`() {
        client().updateSettings(ALERTING_NOTES_ENABLED.key, "true")

        val monitor = createRandomMonitor(refresh = true)
        val alert = createAlert(randomAlert(monitor).copy(state = Alert.State.ACTIVE))
        val alertId = alert.id
        val noteContent = "test note"

        createAlertNote(alertId, noteContent)

        OpenSearchTestCase.waitUntil({
            return@waitUntil false
        }, 3, TimeUnit.SECONDS)

        val search = SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).toString()
        val searchResponse = client().makeRequest(
            "GET",
            "$ALERTING_BASE_URI/alerts/notes/_search",
            StringEntity(search, ContentType.APPLICATION_JSON)
        )

        val xcp = createParser(XContentType.JSON.xContent(), searchResponse.entity.content)
        val hits = xcp.map()["hits"]!! as Map<String, Map<String, Any>>
        logger.info("hits: $hits")
        val numberDocsFound = hits["total"]?.get("value")
        assertEquals("No Notes found", 1, numberDocsFound)

        val searchHits = hits["hits"] as List<*>
        val hit = searchHits[0] as Map<*, *>
        val noteHit = hit["_source"] as Map<*, *>
        assertEquals("returned Note does not match alert id in search query", alertId, noteHit["entity_id"])
        assertEquals("returned Note does not have expected content", noteContent, noteHit["content"])
    }

    fun `test deleting notes`() {
        client().updateSettings(ALERTING_NOTES_ENABLED.key, "true")

        val monitor = createRandomMonitor(refresh = true)
        val alert = createAlert(randomAlert(monitor).copy(state = Alert.State.ACTIVE))
        val alertId = alert.id
        val noteContent = "test note"

        val noteId = createAlertNote(alertId, noteContent).id
        OpenSearchTestCase.waitUntil({
            return@waitUntil false
        }, 3, TimeUnit.SECONDS)

        val deleteResponse = client().makeRequest(
            "DELETE",
            "$ALERTING_BASE_URI/alerts/notes/$noteId"
        )

        assertEquals("Delete note failed", RestStatus.OK, deleteResponse.restStatus())

        val deleteResponseBody = deleteResponse.asMap()

        val deletedNoteId = deleteResponseBody["_id"] as String
        assertEquals("Deleted Note ID does not match Note ID in delete request", noteId, deletedNoteId)
    }

    // TODO: test list
    /*
    create note with empty content should fail
    create without alert id should fail
    update without note id should fail
    search notes across multiple alerts
    (belongs in NotesIT) create note thats too large based on cluster setting should fail
    create note on alert that alrdy has max notes based on cluster setting should fail
    create note on alert user doesn't have backend roles to view should fail
    search note on alert user doesn't have backend roles to view should fail
    notes are shown in notifications for query monitor
    notes are shown in notifications for bucket monitor
    (belongs in NotesIT) update note that user didn't author should fail
    (belongs in NotesIT) delete note that user didn't author should fail
    (belongs in NotesIT) update note that user didn't author but user is Admin should succeed
     */
}

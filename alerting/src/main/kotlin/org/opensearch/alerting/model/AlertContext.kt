/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model

import org.opensearch.commons.alerting.model.Alert
import org.opensearch.commons.alerting.model.DocLevelQuery
import org.opensearch.commons.alerting.model.Note

/**
 * This model is a wrapper for [Alert] that should only be used to create a more
 * informative alert object to enrich mustache template notification messages.
 */
data class AlertContext(
    val alert: Alert,
    val associatedQueries: List<DocLevelQuery>? = null,
    val sampleDocs: List<Map<String, Any?>>? = null,
    val notes: List<Note>? = null
) {
    fun asTemplateArg(): Map<String, Any?> {
        val queriesContext = associatedQueries?.map {
            mapOf(
                DocLevelQuery.QUERY_ID_FIELD to it.id,
                DocLevelQuery.NAME_FIELD to it.name,
                DocLevelQuery.TAGS_FIELD to it.tags
            )
        }

        val notesContext = notes?.map {
            mapOf(
                Note.NOTE_CREATED_TIME_FIELD to it.createdTime,
                Note.NOTE_LAST_UPDATED_TIME_FIELD to it.lastUpdatedTime,
                Note.NOTE_CONTENT_FIELD to it.content,
                Note.NOTE_USER_FIELD to it.user
            )
        }

        // Compile the custom context fields.
        val customContextFields = mapOf(
            ASSOCIATED_QUERIES_FIELD to queriesContext,
            SAMPLE_DOCS_FIELD to sampleDocs,
            NOTES_FIELD to notesContext
        )

        // Get the alert template args
        val templateArgs = alert.asTemplateArg().toMutableMap()

        // Add the non-null custom context fields to the alert templateArgs.
        customContextFields.forEach { (key, value) ->
            value?.let { templateArgs[key] = it }
        }
        return templateArgs
    }

    companion object {
        const val ASSOCIATED_QUERIES_FIELD = "associated_queries"
        const val SAMPLE_DOCS_FIELD = "sample_documents"
        const val NOTES_FIELD = "notes"
    }
}

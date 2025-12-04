/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model

import org.opensearch.commons.alerting.model.Alert
import org.opensearch.commons.alerting.model.Comment
import org.opensearch.commons.alerting.model.DocLevelQuery

/**
 * This model is a wrapper for [Alert] that should only be used to create a more
 * informative alert object to enrich mustache template notification messages.
 */
data class AlertContext(
    val alert: Alert,
    val associatedQueries: List<DocLevelQuery>? = null,
    val sampleDocs: List<Map<String, Any?>>? = null,
    val comments: List<Comment>? = null,
) {
    fun asTemplateArg(): Map<String, Any?> {
        val queriesContext =
            associatedQueries?.map {
                mapOf(
                    DocLevelQuery.QUERY_ID_FIELD to it.id,
                    DocLevelQuery.NAME_FIELD to it.name,
                    DocLevelQuery.TAGS_FIELD to it.tags,
                )
            }

        val commentsContext =
            comments?.map {
                mapOf(
                    Comment.COMMENT_CREATED_TIME_FIELD to it.createdTime,
                    Comment.COMMENT_LAST_UPDATED_TIME_FIELD to it.lastUpdatedTime,
                    Comment.COMMENT_CONTENT_FIELD to it.content,
                    Comment.COMMENT_USER_FIELD to it.user?.name,
                )
            }

        // Compile the custom context fields.
        val customContextFields =
            mapOf(
                ASSOCIATED_QUERIES_FIELD to queriesContext,
                SAMPLE_DOCS_FIELD to sampleDocs,
                COMMENTS_FIELD to commentsContext,
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
        const val COMMENTS_FIELD = "comments"
    }
}

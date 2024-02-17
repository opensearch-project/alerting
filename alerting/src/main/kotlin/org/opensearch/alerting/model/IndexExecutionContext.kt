/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model

import org.opensearch.commons.alerting.model.DocLevelQuery

data class IndexExecutionContext(
    val queries: List<DocLevelQuery>,
    val lastRunContext: MutableMap<String, Any>,
    val updatedLastRunContext: MutableMap<String, Any>,
    val indexName: String,
    val concreteIndexName: String,
    val conflictingFields: List<String>,
    val docIds: List<String>? = null
)

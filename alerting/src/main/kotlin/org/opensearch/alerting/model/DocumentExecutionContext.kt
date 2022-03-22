package org.opensearch.alerting.model

import org.opensearch.alerting.model.docLevelInput.DocLevelQuery

data class DocumentExecutionContext(
    val queries: List<DocLevelQuery>,
    val lastRunContext: Map<String, Any>,
    val updatedLastRunContext: Map<String, Any>
)

package org.opensearch.alerting.core.model

import org.opensearch.alerting.opensearchapi.asTemplateArg
import org.opensearch.alerting.opensearchapi.convertToMap
import org.opensearch.index.query.QueryBuilders
import org.opensearch.search.builder.SearchSourceBuilder
import kotlin.test.Test
import kotlin.test.assertEquals

class SearchSourceBuilderTests {
    @Test
    fun `test SearchSourceBuilder asTemplateArgs`() {
        val query = SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
        val templateArgs = query.asTemplateArg()
        assertEquals(query.convertToMap(), templateArgs)
    }
}

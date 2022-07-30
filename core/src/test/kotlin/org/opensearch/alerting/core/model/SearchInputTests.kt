package org.opensearch.alerting.core.model

import org.junit.Test
import org.opensearch.alerting.opensearchapi.asTemplateArg
import org.opensearch.index.query.QueryBuilders
import org.opensearch.search.builder.SearchSourceBuilder
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class SearchInputTests {
    @Test
    fun `test SearchInput asTemplateArgs`() {
        val searchInputs = listOf(::simpleSearchInput, ::rangeSearchInput)
        searchInputs.forEach { searchInput ->
            val input = searchInput()
            val templateArgs = input.asTemplateArg()
            assertEquals(
                input.indices.size,
                ((templateArgs[SearchInput.SEARCH_FIELD] as Map<*, *>)[SearchInput.INDICES_FIELD] as List<*>).size
            )
            input.indices.forEach {
                assertTrue(((templateArgs[SearchInput.SEARCH_FIELD] as Map<*, *>)[SearchInput.INDICES_FIELD] as List<*>).contains(it))
            }
            assertEquals((templateArgs[SearchInput.SEARCH_FIELD] as Map<*, *>)[SearchInput.QUERY_FIELD], input.query.asTemplateArg())
        }
    }

    private fun simpleSearchInput(): SearchInput {
        return SearchInput(
            indices = listOf("foo", "bar"),
            query = SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
        )
    }

    private fun rangeSearchInput(): SearchInput {
        return SearchInput(
            indices = listOf("foo", "bar"),
            query = SearchSourceBuilder().query(
                QueryBuilders.rangeQuery("test_strict_date_time")
                    .gt("{{period_end}}||-10d")
                    .lte("{{period_end}}")
                    .format("epoch_millis")
            )
        )
    }
}

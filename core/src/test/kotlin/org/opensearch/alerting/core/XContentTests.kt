/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.core

import org.opensearch.alerting.core.model.Input
import org.opensearch.alerting.core.model.SearchInput
import org.opensearch.alerting.core.model.XContentTestBase
import org.opensearch.alerting.opensearchapi.string
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.index.query.QueryBuilders
import org.opensearch.search.builder.SearchSourceBuilder
import kotlin.test.Test
import kotlin.test.assertEquals

class XContentTests : XContentTestBase {

    @Test
    fun `test input parsing`() {
        val input = randomInput()

        val inputString = input.toXContent(builder(), ToXContent.EMPTY_PARAMS).string()
        val parsedInput = Input.parse(parser(inputString))

        assertEquals(input, parsedInput, "Round tripping input doesn't work")
    }

    private fun randomInput(): Input {
        return SearchInput(
            indices = listOf("foo", "bar"),
            query = SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
        )
    }
}

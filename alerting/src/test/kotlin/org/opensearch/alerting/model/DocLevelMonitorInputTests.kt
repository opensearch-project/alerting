/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *   Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.alerting.model

import org.opensearch.alerting.model.docLevelInput.DocLevelMonitorInput
import org.opensearch.alerting.model.docLevelInput.DocLevelQuery
import org.opensearch.alerting.randomDocLevelMonitorInput
import org.opensearch.alerting.randomDocLevelQuery
import org.opensearch.test.OpenSearchTestCase

class DocLevelMonitorInputTests : OpenSearchTestCase() {
    fun `testing DocLevelQuery asTemplateArgs`() {
        // GIVEN
        val query = randomDocLevelQuery()

        // WHEN
        val templateArgs = query.asTemplateArg()

        // THEN
        assertEquals("Template args 'id' field does not match:", templateArgs[DocLevelQuery.QUERY_ID_FIELD], query.id)
        assertEquals("Template args 'query' field does not match:", templateArgs[DocLevelQuery.QUERY_FIELD], query.query)
        assertEquals("Template args 'severity' field does not match:", templateArgs[DocLevelQuery.SEVERITY_FIELD], query.severity)
        assertEquals("Template args 'tags' field does not match:", templateArgs[DocLevelQuery.TAGS_FIELD], query.tags)
        assertEquals("Template args 'actions' field does not match:", templateArgs[DocLevelQuery.ACTIONS_FIELD], query.actions)
    }

    fun `testing DocLevelMonitorInput asTemplateArgs`() {
        // GIVEN
        val input = randomDocLevelMonitorInput()

        // WHEN
        val templateArgs = input.asTemplateArg()

        // THEN
        assertEquals("Template args 'description' field does not match:", templateArgs[DocLevelMonitorInput.DESCRIPTION_FIELD], input.description)
        assertEquals("Template args 'indices' field does not match:", templateArgs[DocLevelMonitorInput.INDICES_FIELD], input.indices)
        assertEquals("Template args 'queries' field does not match:", templateArgs[DocLevelMonitorInput.QUERIES_FIELD], input.queries)
    }
}

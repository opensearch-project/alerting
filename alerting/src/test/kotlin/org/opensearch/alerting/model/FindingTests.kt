/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model

import org.opensearch.alerting.randomFinding
import org.opensearch.test.OpenSearchTestCase

class FindingTests : OpenSearchTestCase() {
    fun `test finding asTemplateArgs`() {
        // GIVEN
        val finding = randomFinding()

        // WHEN
        val templateArgs = finding.asTemplateArg()

        // THEN
        assertEquals("Template args 'id' field does not match:", templateArgs[Finding.FINDING_ID_FIELD], finding.id)
        assertEquals(
            "Template args 'relatedDocIds' field does not match:",
            templateArgs[Finding.RELATED_DOC_IDS_FIELD],
            finding.relatedDocIds
        )
        assertEquals("Template args 'monitorId' field does not match:", templateArgs[Finding.MONITOR_ID_FIELD], finding.monitorId)
        assertEquals(
            "Template args 'monitorName' field does not match:",
            templateArgs[Finding.MONITOR_NAME_FIELD],
            finding.monitorName
        )
        assertEquals("Template args 'queries' field does not match:", templateArgs[Finding.QUERIES_FIELD], finding.docLevelQueries)
        assertEquals(
            "Template args 'timestamp' field does not match:",
            templateArgs[Finding.TIMESTAMP_FIELD],
            finding.timestamp.toEpochMilli()
        )
    }
}

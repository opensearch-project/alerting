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
        assertNull("Template args 'logEvent' field should be null:", finding.logEvent)
        assertEquals("Template args 'monitorId' field does not match:", templateArgs[Finding.MONITOR_ID_FIELD], finding.monitorId)
        assertEquals("Template args 'monitorName' field does not match:", templateArgs[Finding.MONITOR_NAME_FIELD], finding.monitorName)
        assertEquals("Template args 'queryId' field does not match:", templateArgs[Finding.QUERY_ID_FIELD], finding.queryId)
        assertEquals("Template args 'queryTags' field does not match:", templateArgs[Finding.QUERY_TAGS_FIELD], finding.queryTags)
        assertEquals("Template args 'severity' field does not match:", templateArgs[Finding.SEVERITY_FIELD], finding.severity)
        assertEquals("Template args 'timestamp' field does not match:", templateArgs[Finding.TIMESTAMP_FIELD], finding.timestamp.toEpochMilli())
        assertEquals("Template args 'triggerId' field does not match:", templateArgs[Finding.TRIGGER_ID_FIELD], finding.triggerId)
        assertEquals("Template args 'triggerName' field does not match:", templateArgs[Finding.TRIGGER_NAME_FIELD], finding.triggerName)
    }
}

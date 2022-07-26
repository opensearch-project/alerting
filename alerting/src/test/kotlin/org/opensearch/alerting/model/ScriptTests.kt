/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model

import org.opensearch.alerting.opensearchapi.asTemplateArg
import org.opensearch.alerting.randomScript
import org.opensearch.test.OpenSearchTestCase

class ScriptTests : OpenSearchTestCase() {
    fun `test script as template args`() {
        val script = randomScript()

        val templateArgs = script.asTemplateArg()

        assertEquals("Template args source does not match", templateArgs["source"], script.idOrCode)
        assertEquals("Template args lang does not match", templateArgs["lang"], script.lang)
    }
}

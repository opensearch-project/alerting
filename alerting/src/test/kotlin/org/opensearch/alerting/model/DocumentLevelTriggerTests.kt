/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model

import org.opensearch.alerting.opensearchapi.asTemplateArg
import org.opensearch.alerting.randomDocumentLevelTrigger
import org.opensearch.test.OpenSearchTestCase

class DocumentLevelTriggerTests : OpenSearchTestCase() {
    fun `test document level trigger as template args`() {
        val trigger = randomDocumentLevelTrigger()

        val templateArgs = trigger.asTemplateArg()

        assertEquals("Template args id does not match", templateArgs[Trigger.ID_FIELD], trigger.id)
        assertEquals("Template args name does not match", templateArgs[Trigger.NAME_FIELD], trigger.name)
        assertEquals("Template args severity does not match", templateArgs[Trigger.SEVERITY_FIELD], trigger.severity)
        assertEquals(
            "Template args condition does not match",
            templateArgs[DocumentLevelTrigger.CONDITION_FIELD] as Map<*, *>,
            mapOf(DocumentLevelTrigger.SCRIPT_FIELD to trigger.condition.asTemplateArg())
        )
        assertEquals(
            "Template args actions field does not contain the expected number of actions",
            (templateArgs[Trigger.ACTIONS_FIELD] as List<*>).size,
            trigger.actions.size
        )
        trigger.actions.forEach {
            assertTrue(
                "Template args actions does not match:",
                (templateArgs[Trigger.ACTIONS_FIELD] as List<*>).contains(it.asTemplateArg())
            )
        }
    }
}

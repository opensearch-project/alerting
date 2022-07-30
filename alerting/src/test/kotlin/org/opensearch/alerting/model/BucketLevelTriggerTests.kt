/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model

import org.opensearch.alerting.randomBucketLevelTrigger
import org.opensearch.test.OpenSearchTestCase

class BucketLevelTriggerTests : OpenSearchTestCase() {

    fun `test bucket level trigger as template args`() {
        val trigger = randomBucketLevelTrigger()

        val templateArgs = trigger.asTemplateArg()

        assertEquals("Template args id does not match", templateArgs[Trigger.ID_FIELD], trigger.id)
        assertEquals("Template args name does not match", templateArgs[Trigger.NAME_FIELD], trigger.name)
        assertEquals("Template args severity does not match", templateArgs[Trigger.SEVERITY_FIELD], trigger.severity)
        assertEquals("Template args condition does not match", templateArgs[BucketLevelTrigger.CONDITION_FIELD] as Map<*, *>, trigger.bucketSelector.asTemplateArg())
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

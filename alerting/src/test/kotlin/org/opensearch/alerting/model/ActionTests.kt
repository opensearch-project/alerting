/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model

import org.opensearch.alerting.model.action.Action
import org.opensearch.alerting.randomAction
import org.opensearch.test.OpenSearchTestCase

class ActionTests : OpenSearchTestCase() {
    fun `test action as template args`() {
        // GIVEN
        val action = randomAction()

        // WHEN
        val templateArgs = action.asTemplateArg()

        // THEN
        assertEquals("Template args id does not match", templateArgs[Action.ID_FIELD], action.id)
        assertEquals("Template args name does not match", templateArgs[Action.NAME_FIELD], action.name)
        assertEquals("Template args destination id does not match", templateArgs[Action.DESTINATION_ID_FIELD], action.destinationId)
        assertEquals("Template args throttle enabled value does not match", templateArgs[Action.THROTTLE_ENABLED_FIELD], action.throttleEnabled)
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.test.OpenSearchTestCase
import kotlin.test.Test

class GetDestinationsActionTests : OpenSearchTestCase() {
    @Test
    fun `test get destinations action name`() {
        assertNotNull(GetDestinationsAction.INSTANCE.name())
        assertEquals(GetDestinationsAction.INSTANCE.name(), GetDestinationsAction.NAME)
    }
}

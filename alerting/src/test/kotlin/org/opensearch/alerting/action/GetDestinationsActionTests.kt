/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.test.OpenSearchTestCase

class GetDestinationsActionTests : OpenSearchTestCase() {

    fun `test get destinations action name`() {
        assertNotNull(GetDestinationsAction.INSTANCE.name())
        assertEquals(GetDestinationsAction.INSTANCE.name(), GetDestinationsAction.NAME)
    }
}

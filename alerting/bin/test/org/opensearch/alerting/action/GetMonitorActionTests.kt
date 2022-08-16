/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.test.OpenSearchTestCase

class GetMonitorActionTests : OpenSearchTestCase() {

    fun `test get monitor action name`() {
        assertNotNull(GetMonitorAction.INSTANCE.name())
        assertEquals(GetMonitorAction.INSTANCE.name(), GetMonitorAction.NAME)
    }
}

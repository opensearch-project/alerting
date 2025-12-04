/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.alerting.action

import org.opensearch.test.OpenSearchTestCase
import kotlin.test.Test

class ExecuteMonitorActionTests : OpenSearchTestCase() {
    @Test
    fun `test execute monitor action name`() {
        assertNotNull(ExecuteMonitorAction.INSTANCE.name())
        assertEquals(ExecuteMonitorAction.INSTANCE.name(), ExecuteMonitorAction.NAME)
    }
}

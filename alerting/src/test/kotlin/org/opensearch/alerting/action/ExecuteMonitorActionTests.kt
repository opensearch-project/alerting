/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.alerting.action

import org.opensearch.test.OpenSearchTestCase

class ExecuteMonitorActionTests : OpenSearchTestCase() {

    fun `test execute monitor action name`() {
        assertNotNull(ExecuteMonitorAction.INSTANCE.name())
        assertEquals(ExecuteMonitorAction.INSTANCE.name(), ExecuteMonitorAction.NAME)
    }
}

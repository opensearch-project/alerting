/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.test.OpenSearchTestCase

class IndexMonitorActionTests : OpenSearchTestCase() {

    fun `test index monitor action name`() {
        assertNotNull(IndexMonitorAction.INSTANCE.name())
        assertEquals(IndexMonitorAction.INSTANCE.name(), IndexMonitorAction.NAME)
    }
}

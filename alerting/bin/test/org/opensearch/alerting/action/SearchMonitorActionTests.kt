/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.junit.Assert
import org.opensearch.test.OpenSearchTestCase

class SearchMonitorActionTests : OpenSearchTestCase() {

    fun `test search monitor action name`() {
        Assert.assertNotNull(SearchMonitorAction.INSTANCE.name())
        Assert.assertEquals(SearchMonitorAction.INSTANCE.name(), SearchMonitorAction.NAME)
    }
}

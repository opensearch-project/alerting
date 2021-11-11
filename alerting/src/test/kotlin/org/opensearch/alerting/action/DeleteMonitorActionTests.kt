/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.junit.Assert
import org.opensearch.test.OpenSearchTestCase

class DeleteMonitorActionTests : OpenSearchTestCase() {

    fun `test delete monitor action name`() {
        Assert.assertNotNull(DeleteMonitorAction.INSTANCE.name())
        Assert.assertEquals(DeleteMonitorAction.INSTANCE.name(), DeleteMonitorAction.NAME)
    }
}

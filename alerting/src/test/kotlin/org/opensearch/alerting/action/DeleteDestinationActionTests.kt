/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.junit.Assert
import org.opensearch.test.OpenSearchTestCase

class DeleteDestinationActionTests : OpenSearchTestCase() {

    fun `test delete destination action name`() {
        Assert.assertNotNull(DeleteDestinationAction.INSTANCE.name())
        Assert.assertEquals(DeleteDestinationAction.INSTANCE.name(), DeleteDestinationAction.NAME)
    }
}

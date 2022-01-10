/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.alerting.action

import org.junit.Assert
import org.opensearch.test.OpenSearchTestCase

class IndexDestinationActionTests : OpenSearchTestCase() {

    fun `test delete destination action name`() {
        Assert.assertNotNull(IndexDestinationAction.INSTANCE.name())
        Assert.assertEquals(IndexDestinationAction.INSTANCE.name(), IndexDestinationAction.NAME)
    }
}

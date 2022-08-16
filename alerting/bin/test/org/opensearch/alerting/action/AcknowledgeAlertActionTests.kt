/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.junit.Assert
import org.opensearch.test.OpenSearchTestCase

class AcknowledgeAlertActionTests : OpenSearchTestCase() {

    fun `test ack alert action name`() {
        Assert.assertNotNull(AcknowledgeAlertAction.INSTANCE.name())
        Assert.assertEquals(AcknowledgeAlertAction.INSTANCE.name(), AcknowledgeAlertAction.NAME)
    }
}

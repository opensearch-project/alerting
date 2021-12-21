/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.test.OpenSearchTestCase

class GetAlertsActionTests : OpenSearchTestCase() {

    fun `test get alerts action name`() {
        assertNotNull(GetAlertsAction.INSTANCE.name())
        assertEquals(GetAlertsAction.INSTANCE.name(), GetAlertsAction.NAME)
    }
}

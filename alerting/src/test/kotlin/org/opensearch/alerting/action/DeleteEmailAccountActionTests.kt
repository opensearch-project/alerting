/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.test.OpenSearchTestCase

class DeleteEmailAccountActionTests : OpenSearchTestCase() {

    fun `test delete email account action name`() {
        assertNotNull(DeleteEmailAccountAction.INSTANCE.name())
        assertEquals(DeleteEmailAccountAction.INSTANCE.name(), DeleteEmailAccountAction.NAME)
    }
}

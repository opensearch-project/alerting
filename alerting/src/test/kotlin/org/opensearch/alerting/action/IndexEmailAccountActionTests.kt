/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.test.OpenSearchTestCase

class IndexEmailAccountActionTests : OpenSearchTestCase() {

    fun `test index email account action name`() {
        assertNotNull(IndexEmailAccountAction.INSTANCE.name())
        assertEquals(IndexEmailAccountAction.INSTANCE.name(), IndexEmailAccountAction.NAME)
    }
}

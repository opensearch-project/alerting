/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.test.OpenSearchTestCase

class IndexEmailGroupActionTests : OpenSearchTestCase() {

    fun `test index email group action name`() {
        assertNotNull(IndexEmailGroupAction.INSTANCE.name())
        assertEquals(IndexEmailGroupAction.INSTANCE.name(), IndexEmailGroupAction.NAME)
    }
}

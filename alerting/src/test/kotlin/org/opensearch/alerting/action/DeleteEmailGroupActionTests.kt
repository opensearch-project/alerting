/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.test.OpenSearchTestCase

class DeleteEmailGroupActionTests : OpenSearchTestCase() {

    fun `test delete email group action name`() {
        assertNotNull(DeleteEmailGroupAction.INSTANCE.name())
        assertEquals(DeleteEmailGroupAction.INSTANCE.name(), DeleteEmailGroupAction.NAME)
    }
}

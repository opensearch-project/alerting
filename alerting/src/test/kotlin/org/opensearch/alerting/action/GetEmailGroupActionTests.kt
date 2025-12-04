/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.test.OpenSearchTestCase
import kotlin.test.Test

class GetEmailGroupActionTests : OpenSearchTestCase() {
    @Test
    fun `test get email group name`() {
        assertNotNull(GetEmailGroupAction.INSTANCE.name())
        assertEquals(GetEmailGroupAction.INSTANCE.name(), GetEmailGroupAction.NAME)
    }
}

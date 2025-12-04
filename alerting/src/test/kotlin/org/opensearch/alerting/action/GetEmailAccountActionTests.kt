/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.test.OpenSearchTestCase
import kotlin.test.Test

class GetEmailAccountActionTests : OpenSearchTestCase() {
    @Test
    fun `test get email account name`() {
        assertNotNull(GetEmailAccountAction.INSTANCE.name())
        assertEquals(GetEmailAccountAction.INSTANCE.name(), GetEmailAccountAction.NAME)
    }
}

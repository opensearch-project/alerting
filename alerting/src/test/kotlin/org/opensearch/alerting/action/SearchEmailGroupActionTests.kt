/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.test.OpenSearchTestCase
import kotlin.test.Test

class SearchEmailGroupActionTests : OpenSearchTestCase() {
    @Test
    fun `test search email group action name`() {
        assertNotNull(SearchEmailGroupAction.INSTANCE.name())
        assertEquals(SearchEmailGroupAction.INSTANCE.name(), SearchEmailGroupAction.NAME)
    }
}

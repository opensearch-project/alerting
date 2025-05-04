/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.test.OpenSearchTestCase

class SearchEmailAccountActionTests : OpenSearchTestCase() {

    fun `test search email account action name`() {
        assertNotNull(SearchEmailAccountAction.INSTANCE.name())
        assertEquals(SearchEmailAccountAction.INSTANCE.name(), SearchEmailAccountAction.NAME)
    }
}

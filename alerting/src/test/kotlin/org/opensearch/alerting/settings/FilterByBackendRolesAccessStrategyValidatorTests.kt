/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.settings

import org.opensearch.test.OpenSearchTestCase
import kotlin.test.assertFailsWith

class FilterByBackendRolesAccessStrategyValidatorTests : OpenSearchTestCase() {

    fun `test accepts valid strategies`() {
        val validator = FilterByBackendRolesAccessStrategyValidator()
        validator.validate("all")
        validator.validate("exact")
        validator.validate("intersect")
    }

    fun `test rejects invalid strategy`() {
        val validator = FilterByBackendRolesAccessStrategyValidator()
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException") {
            validator.validate("invalid")
        }
    }
}

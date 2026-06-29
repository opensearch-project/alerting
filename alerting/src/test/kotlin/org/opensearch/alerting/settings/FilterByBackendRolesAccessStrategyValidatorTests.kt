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
        validator.validate("ALL")
        validator.validate("exact")
        validator.validate("EXACT")
        validator.validate("intersect")
        validator.validate("INTERSECT")
    }

    fun `test rejects invalid strategy`() {
        val validator = FilterByBackendRolesAccessStrategyValidator()
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException") {
            validator.validate("invalid")
        }
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException") {
            validator.validate("")
        }
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException") {
            validator.validate(" ")
        }
    }
}

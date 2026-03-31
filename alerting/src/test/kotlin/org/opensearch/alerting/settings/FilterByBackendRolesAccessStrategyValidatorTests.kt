/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.settings

import org.opensearch.test.OpenSearchTestCase
import kotlin.test.assertFailsWith

class FilterByBackendRolesAccessStrategyValidatorTests : OpenSearchTestCase() {

    fun `test accepts strategy of all`() {
        val validator = FilterByBackendRolesAccessStrategyValidator()
        try {
            validator.validate("all")
        } catch (e: Exception) {
            fail("Unexpected exception")
        }
    }

    fun `test accepts strategy of exact`() {
        val validator = FilterByBackendRolesAccessStrategyValidator()
        try {
            validator.validate("exact")
        } catch (e: Exception) {
            fail("Unexpected exception")
        }
    }

    fun `test accepts strategy of intersect`() {
        val validator = FilterByBackendRolesAccessStrategyValidator()
        try {
            validator.validate("intersect")
        } catch (e: Exception) {
            fail("Unexpected exception")
        }
    }

    fun `test rejects invalid strategy`() {
        val validator = FilterByBackendRolesAccessStrategyValidator()
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException") {
            validator.validate("invalid")
        }
    }
}

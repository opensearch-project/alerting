/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.settings

import org.opensearch.common.settings.Setting

class FilterByBackendRolesAccessStrategyValidator : Setting.Validator<String> {
    override fun validate(strategy: String) {
        val allStrategies: List<String> = FilterByBackendRolesAccessStrategy.entries.map { it.strategy }

        when (strategy) {
            FilterByBackendRolesAccessStrategy.ALL.strategy,
            FilterByBackendRolesAccessStrategy.EXACT.strategy,
            FilterByBackendRolesAccessStrategy.INTERSECT.strategy -> {}
            else -> throw IllegalArgumentException(
                "Setting value must be one of [$allStrategies]"
            )
        }
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model.suggestions.rules.util

import org.opensearch.alerting.model.Monitor
import kotlin.reflect.KClass

enum class SuggestionObjectType(val value: KClass<out Any>) {
    MONITOR(Monitor::class);

    companion object {
        fun enumFromClass(type: KClass<*>) = SuggestionObjectType.values().first { it.value == type }
    }
}

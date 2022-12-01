/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model.suggestions.rules.util

/**
 * Stretch Goal:
 * Have multiple ComponentType classes, one class for each object type.
 * Have subinterfaces for each object type that extend the base Rule interface, in those
 * subinterfaces, add the component field, and make their type the ComponentType enum
 * corresponding to their object type, ie
 *
 * MonitorRule : Rule<Monitor> {
 *   component: MonitorComponentType
 * }
 */

enum class ComponentType(val value: String) {
    NOT_SUPPORTED_COMPONENT(""),

    MONITOR_WHOLE("monitor"),
    MONITOR_QUERY("monitor.query");

    override fun toString(): String {
        return value
    }

    companion object {
        fun enumFromStr(value: String) = ComponentType.values().first { it.value == value }
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model.suggestions.rules.util

/**
 * All implementations of this Rule interface must be
 * singleton classes (objects)
 */
interface Rule<in T> {

    val objType: SuggestionObjectType
    val component: ComponentType

    /**
     * Evaluate the given object against the Rule.
     *
     * This is the place to describe what inefficient design
     * pattern or choice your Rule will be in charge of, ie what
     * pattern or design choice it will store and check
     * for in a user-defined configuration.
     * Describe your Rule's design pattern/choice
     * by implementing the very check it will perform.
     *
     * Params:
     * - obj: the object that the Rule will check
     *
     * Returns null if the given object passed the Rule's check
     * ie the Rule could not find its inefficient config
     * choice in the given object, meaning the object
     * doesn't need the Rule's suggestion
     *
     * Returns a String suggestion if the given object failed
     * the Rule's check, ie the Rule found its inefficient config
     * choice in the given object, meaning the object needs this
     * Rule's suggestion
     */
    fun evaluate(obj: T): String?
}

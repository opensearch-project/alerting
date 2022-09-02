/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.rules.util

import org.opensearch.alerting.model.Monitor

/**
 * All implementations of this Rule interface must be
 * singleton classes
 */
interface Rule {
    /**
     * Evaluate the given Monitor against the Rule.
     *
     * This is the place to describe what inefficient design
     * pattern or choice your Rule will be in charge of, ie what
     * pattern or design choice it will store and check
     * for in a Monitor. Describe your Rule's design pattern/choice
     * by implementing the very check it will perform.
     *
     * Returns true if the given Monitor passes the Rule's check
     * ie the Rule could not find its inefficient pattern or
     * design choice in the given Monitor, meaning the Monitor
     * doesn't need the Rule's suggestion
     *
     * Returns false otherwise
     */
    fun evaluate(monitor: Monitor): Boolean

    fun suggestion(): String
}

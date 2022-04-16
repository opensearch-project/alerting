/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.triggercondition.tokens

/**
 * To define all the operators used in the trigger expression
 */
enum class TriggerExpressionOperator(val value: String, val precedence: Int, val rightAssociative: Boolean) : ExpressionToken {

    AND("&&", 2, false),
    OR("||", 2, false),

    NOT("!", 3, true),

    PAR_LEFT("(", 1, false),
    PAR_RIGHT(")", 1, false)
}

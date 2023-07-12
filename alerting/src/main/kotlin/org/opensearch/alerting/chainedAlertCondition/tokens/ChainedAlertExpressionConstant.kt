/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.chainedAlertCondition.tokens

/**
 * To define all the tokens which could be part of expression constant such as query[id=new_id], query[name=new_name],
 * query[tag=new_tag]
 */
class ChainedAlertExpressionConstant(val type: ConstantType) : ExpressionToken {

    enum class ConstantType(val ident: String) {
        MONITOR("monitor"),

        ID("id"),

        BRACKET_LEFT("["),
        BRACKET_RIGHT("]"),

        EQUALS("=")
    }
}

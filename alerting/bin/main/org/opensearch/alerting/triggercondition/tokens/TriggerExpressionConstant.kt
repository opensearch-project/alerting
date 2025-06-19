/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.triggercondition.tokens

/**
 * To define all the tokens which could be part of expression constant such as query[id=new_id], query[name=new_name],
 * query[tag=new_tag]
 */
class TriggerExpressionConstant(val type: ConstantType) : ExpressionToken {

    enum class ConstantType(val ident: String) {
        QUERY("query"),

        TAG("tag"),
        NAME("name"),
        ID("id"),

        BRACKET_LEFT("["),
        BRACKET_RIGHT("]"),

        EQUALS("=")
    }
}

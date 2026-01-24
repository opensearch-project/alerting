/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.chainedAlertCondition.tokens

/**
 * To define the tokens in Trigger expression such as monitor[id=“id1"] or monitor[id=“id2"] and monitor[id=“id3"]
 */
internal data class CAExpressionToken(
    val value: String,
) : ExpressionToken

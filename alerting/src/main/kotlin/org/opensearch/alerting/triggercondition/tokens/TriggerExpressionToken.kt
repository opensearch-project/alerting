/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.triggercondition.tokens

/**
 * To define the tokens in Trigger expression such as query[tag=“sev1"] or query[name=“sev1"] or query[id=“sev1"]
 */
internal data class TriggerExpressionToken(val value: String) : ExpressionToken

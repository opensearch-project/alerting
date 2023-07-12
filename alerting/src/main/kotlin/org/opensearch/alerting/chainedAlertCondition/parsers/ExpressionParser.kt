/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.chainedAlertCondition.parsers

import org.opensearch.alerting.chainedAlertCondition.resolvers.ChainedAlertTriggerResolver

interface ExpressionParser {
    fun parse(): ChainedAlertTriggerResolver
}

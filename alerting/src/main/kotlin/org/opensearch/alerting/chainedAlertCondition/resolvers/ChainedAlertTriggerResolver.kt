/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.chainedAlertCondition.resolvers

interface ChainedAlertTriggerResolver {
    fun evaluate(monitorIdAlertPresentMap: Map<String, Boolean>): Boolean
}

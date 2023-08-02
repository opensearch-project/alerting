/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.chainedAlertCondition.resolvers

interface ChainedAlertTriggerResolver {
    fun getMonitorIds(parsedTriggerCondition: ChainedAlertRPNResolver): Set<String>
    fun evaluate(alertGeneratingMonitors: Set<String>): Boolean
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.chainedAlertCondition.resolvers

import org.opensearch.commons.alerting.model.DocLevelQuery

interface CAResolver {
    fun evaluate(monitorIdAlertPresentMap: Map<String, Boolean>): Boolean
}

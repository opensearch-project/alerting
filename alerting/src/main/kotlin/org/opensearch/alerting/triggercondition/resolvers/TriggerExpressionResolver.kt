/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.triggercondition.resolvers

import org.opensearch.commons.alerting.model.DocLevelQuery

interface TriggerExpressionResolver {
    fun evaluate(queryToDocIds: Map<DocLevelQuery, Set<String>>): Set<String>
}

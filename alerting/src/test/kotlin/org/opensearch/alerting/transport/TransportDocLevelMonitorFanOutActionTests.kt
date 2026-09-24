/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.alerting.transport

import org.opensearch.commons.alerting.model.DocLevelQuery
import org.opensearch.test.OpenSearchTestCase
import java.lang.reflect.Modifier
import java.lang.reflect.ParameterizedType
import java.lang.reflect.Type
import java.lang.reflect.WildcardType

/**
 * [TransportDocLevelMonitorFanOutAction] is a per-node singleton, while fan-out executions run
 * concurrently on Dispatchers.IO. Per-execution state must therefore live in local variables that
 * are threaded through the execution's call chain, never in instance fields on the action.
 *
 * An instance-level `findingsToTriggeredQueries: Map<String, List<DocLevelQuery>>` previously
 * violated this: keyed by finding UUID, it was never reset between executions, so it grew without
 * bound for the lifetime of the node JVM and each `createFindings` call copied the whole
 * accumulated map. These tests fail if such a field is reintroduced.
 */
class TransportDocLevelMonitorFanOutActionTests : OpenSearchTestCase() {

    fun `test fan-out action declares no instance-level map state`() {
        val mapFields = TransportDocLevelMonitorFanOutAction::class.java.declaredFields
            .filter { !Modifier.isStatic(it.modifiers) }
            .filter { Map::class.java.isAssignableFrom(it.type) }
            .map { it.name }

        assertTrue(
            "TransportDocLevelMonitorFanOutAction is a per-node singleton shared by concurrent " +
                "executions, so map state must be scoped to a single execution (created in " +
                "executeMonitor and passed as a parameter), not held as an instance field. " +
                "Offending fields: $mapFields",
            mapFields.isEmpty()
        )
    }

    fun `test fan-out action holds no per-execution DocLevelQuery state as instance fields`() {
        val offending = TransportDocLevelMonitorFanOutAction::class.java.declaredFields
            .filter { !Modifier.isStatic(it.modifiers) }
            .filter { referencesDocLevelQuery(it.genericType) }
            .map { it.name }

        assertTrue(
            "Fields holding DocLevelQuery collections (e.g. findingsToTriggeredQueries) are " +
                "per-execution state and must not be stored on the shared transport action. " +
                "Offending fields: $offending",
            offending.isEmpty()
        )
    }

    private fun referencesDocLevelQuery(type: Type): Boolean {
        return when (type) {
            DocLevelQuery::class.java -> true
            is ParameterizedType ->
                type.rawType == DocLevelQuery::class.java || type.actualTypeArguments.any { referencesDocLevelQuery(it) }
            is WildcardType ->
                type.upperBounds.any { referencesDocLevelQuery(it) } || type.lowerBounds.any { referencesDocLevelQuery(it) }
            else -> false
        }
    }
}

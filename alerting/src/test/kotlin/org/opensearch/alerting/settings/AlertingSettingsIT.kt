/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.settings

import org.opensearch.alerting.ALWAYS_RUN
import org.opensearch.alerting.AlertingRestTestCase
import org.opensearch.alerting.model.Trigger
import org.opensearch.alerting.model.action.Action
import org.opensearch.alerting.randomAction
import org.opensearch.alerting.randomQueryLevelMonitor
import org.opensearch.alerting.randomQueryLevelTrigger

class AlertingSettingsIT : AlertingRestTestCase() {

    fun `test updating setting of max actions per trigger with more actions than allowed actions`() {
        val actions = createActions(4)
        val triggers = createTriggers(1, actions)
        createMonitor(
            randomQueryLevelMonitor(
                triggers = triggers
            )
        )

        try {
            client().updateSettings(AlertingSettings.TOTAL_MAX_ACTIONS_PER_TRIGGER.key, 1)
        } catch (e: Exception) {
            assertTrue(e is IllegalArgumentException)
        } finally {
            client().updateSettings(
                AlertingSettings.TOTAL_MAX_ACTIONS_PER_TRIGGER.key, AlertingSettings.DEFAULT_TOTAL_MAX_ACTIONS_PER_TRIGGER
            )
        }
    }

    fun `test updating setting of max actions per trigger with less actions than allowed actions`() {
        val actions = createActions(10)
        val triggers = createTriggers(1, actions)
        createMonitor(
            randomQueryLevelMonitor(
                triggers = triggers
            )
        )

        client().updateSettings(AlertingSettings.TOTAL_MAX_ACTIONS_PER_TRIGGER.key, 10)
        client().updateSettings(AlertingSettings.TOTAL_MAX_ACTIONS_PER_TRIGGER.key, AlertingSettings.DEFAULT_TOTAL_MAX_ACTIONS_PER_TRIGGER)
    }

    fun `test updating setting of overall max actions with more actions than allowed actions`() {
        val actions = createActions(4)
        val triggers = createTriggers(4, actions)
        createMonitor(
            randomQueryLevelMonitor(
                triggers = triggers
            )
        )

        try {
            client().updateSettings(AlertingSettings.TOTAL_MAX_ACTIONS_ACROSS_TRIGGERS.key, 1)
        } catch (e: Exception) {
            assertTrue(e is IllegalArgumentException)
        } finally {
            client().updateSettings(
                AlertingSettings.TOTAL_MAX_ACTIONS_PER_TRIGGER.key, AlertingSettings.DEFAULT_TOTAL_MAX_ACTIONS_ACROSS_TRIGGERS
            )
        }
    }

    fun `test updating setting of overall max actions with less actions than allowed actions`() {
        val actions = createActions(1)
        val triggers = createTriggers(1, actions)
        createMonitor(
            randomQueryLevelMonitor(
                triggers = triggers
            )
        )

        client().updateSettings(AlertingSettings.TOTAL_MAX_ACTIONS_ACROSS_TRIGGERS.key, 10)
        client().updateSettings(
            AlertingSettings.TOTAL_MAX_ACTIONS_PER_TRIGGER.key, AlertingSettings.DEFAULT_TOTAL_MAX_ACTIONS_ACROSS_TRIGGERS
        )
    }

    private fun createTriggers(amount: Int, actions: List<Action>): List<Trigger> {
        val triggers = mutableListOf<Trigger>()
        for (i in 0..amount)
            triggers.add(randomQueryLevelTrigger(actions = actions, condition = ALWAYS_RUN, destinationId = createDestination().id))

        return triggers
    }

    private fun createActions(amount: Int): List<Action> {
        val actions = mutableListOf<Action>()
        for (i in 0..amount)
            actions.add(randomAction())

        return actions
    }
}

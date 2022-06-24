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

    fun `test updating setting of max actions per trigger with less actions than allowed actions`() {
        val actions = createActions(4)
        val triggers = createTriggers(1, actions)
        createMonitor(
            randomQueryLevelMonitor(
                triggers = triggers
            )
        )

        try {
            client().updateSettings(AlertingSettings.TOTAL_MAX_ACTIONS_ACROSS_TRIGGERS.key, 1)
        } catch (e: Exception) {
            assertTrue(e is IllegalArgumentException)
        }
    }

    fun `test updating setting of max actions per trigger with more actions than allowed actions`() {
        val actions = createActions(4)
        val triggers = createTriggers(1, actions)
        createMonitor(
            randomQueryLevelMonitor(
                triggers = triggers
            )
        )

        client().updateSettings(AlertingSettings.TOTAL_MAX_ACTIONS_ACROSS_TRIGGERS.key, 10)
    }

    fun `test acquisition of multiple triggers`() {
        val alertingSettingsCompanion = AlertingSettings.Companion

        val actions = createActions(2)
        val triggers = createTriggers(2, actions)
        val monitor = createMonitor(
            randomQueryLevelMonitor(
                triggers = triggers
            )
        )

        executeMonitor(monitor.id)

        var amountOfActions = 0
        for (trigger in monitor.triggers)
            amountOfActions += trigger.actions.size

        assertEquals(
            "Monitor contains correct amount of actions",
            amountOfActions, alertingSettingsCompanion.getCurrentAmountOfActions(triggers)
        )
    }

    fun `test acquisition of single trigger`() {
        val alertingSettingsCompanion = AlertingSettings.Companion

        val actions = createActions(3)
        val triggers = createTriggers(1, actions)
        val monitor = createMonitor(
            randomQueryLevelMonitor(
                triggers = triggers
            )
        )

        executeMonitor(monitor.id)

        var amountOfActions = 0
        for (trigger in monitor.triggers)
            amountOfActions += trigger.actions.size

        assertEquals(
            "Monitor contains correct amount of actions",
            amountOfActions, alertingSettingsCompanion.getCurrentAmountOfActions(triggers)
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

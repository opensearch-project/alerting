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
import org.opensearch.test.rest.RestActionTestCase.VerifyingClient

class AlertingSettingsIT : AlertingRestTestCase() {

    fun `test client initialized successfully`() {
        val client = VerifyingClient(randomAlphaOfLength(10))
        AlertingSettings(client)

        assertEquals("Client has been initialized successfully", AlertingSettings.Companion.internalClient, client)
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

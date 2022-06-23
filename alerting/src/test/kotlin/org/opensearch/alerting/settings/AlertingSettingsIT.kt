/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.settings

import org.mockito.ArgumentMatchers.anyList
import org.mockito.Mockito.spy
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.opensearch.alerting.ALWAYS_RUN
import org.opensearch.alerting.AlertingRestTestCase
import org.opensearch.alerting.model.Trigger
import org.opensearch.alerting.model.action.Action
import org.opensearch.alerting.randomAction
import org.opensearch.alerting.randomQueryLevelMonitor
import org.opensearch.alerting.randomQueryLevelTrigger
import org.opensearch.test.rest.RestActionTestCase.VerifyingClient

class AlertingSettingsIT : AlertingRestTestCase() {

    fun `test client not initialized`() {
        AlertingSettings.MAX_ACTIONS_ACROSS_TRIGGERS

        assertEquals("Client has not been initialized", AlertingSettings.Companion.internalClient, null)
    }

    fun `test client initialized successfully`() {
        val client = VerifyingClient(randomAlphaOfLength(10))
        AlertingSettings(client)

        assertEquals("Client has been initialized successfully", AlertingSettings.Companion.internalClient, client)
    }

    fun `test acquisition of triggers client is initialized`() {
        val client = VerifyingClient(randomAlphaOfLength(10))
        AlertingSettings(client)
        val alertingSettingsCompanion = spy(AlertingSettings.Companion)

        verify(alertingSettingsCompanion, times(1)).getCurrentAmountOfActions(anyList())
    }

    fun `test acquisition of triggers`() {
        val alertingSettingsCompanion = spy(AlertingSettings.Companion)

        val actions = createActions(2)
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

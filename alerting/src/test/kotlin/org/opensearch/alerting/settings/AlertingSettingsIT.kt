package org.opensearch.alerting.settings

import org.junit.Before
import org.mockito.Mockito
import org.opensearch.alerting.AlertingRestTestCase
import org.opensearch.client.Client
import org.opensearch.test.OpenSearchIntegTestCase

class AlertingSettingsIT : AlertingRestTestCase() {
    private lateinit var alertingSettings: AlertingSettings

    @Before
    fun setup() {
        alertingSettings = AlertingSettings(OpenSearchIntegTestCase.client())
    }

    suspend fun `test acquisition of triggers client not initialized`() {
        val alertingSettingsCompanionMock = Mockito.mock(AlertingSettings.Companion::class.java)

        AlertingSettings.TOTAL_MAX_ACTIONS_ACROSS_TRIGGERS
        Mockito.verify(alertingSettingsCompanionMock, Mockito.times(0)).getTriggers(Any() as Client)
    }

    suspend fun `test acquisition of triggers client is initialized`() {
        val alertingSettingsMock = Mockito.mock(AlertingSettings::class.java)
        val alertingSettingsCompanionMock = Mockito.mock(AlertingSettings.Companion::class.java)

        Mockito.`when`(alertingSettingsMock.client).thenReturn(OpenSearchIntegTestCase.client())
        AlertingSettings.TOTAL_MAX_ACTIONS_ACROSS_TRIGGERS
        Mockito.verify(alertingSettingsCompanionMock, Mockito.times(1)).getTriggers(OpenSearchIntegTestCase.client())
    }
}

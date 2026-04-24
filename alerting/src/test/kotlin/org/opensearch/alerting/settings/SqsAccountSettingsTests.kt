/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.settings

import org.junit.Before
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.util.PluginSettingSqsAccountIdProvider
import org.opensearch.common.settings.Settings
import org.opensearch.commons.utils.scheduler.SqsAccountIdProvider
import org.opensearch.test.OpenSearchTestCase

class SqsAccountSettingsTests : OpenSearchTestCase() {

    private lateinit var plugin: AlertingPlugin

    @Before
    fun setup() {
        plugin = AlertingPlugin()
    }

    fun `test sqs_account_id setting is registered`() {
        assertTrue(
            "SQS_ACCOUNT_ID not registered",
            plugin.settings.contains(AlertingSettings.SQS_ACCOUNT_ID)
        )
    }

    fun `test account_provider_type setting is registered`() {
        assertTrue(
            "SQS_ACCOUNT_PROVIDER_TYPE not registered",
            plugin.settings.contains(AlertingSettings.SQS_ACCOUNT_PROVIDER_TYPE)
        )
    }

    fun `test sqs_account_id defaults to empty`() {
        assertEquals("", AlertingSettings.SQS_ACCOUNT_ID.get(Settings.EMPTY))
    }

    fun `test account_provider_type defaults to plugin_setting`() {
        assertEquals("plugin_setting", AlertingSettings.SQS_ACCOUNT_PROVIDER_TYPE.get(Settings.EMPTY))
    }

    fun `test sqs_account_id reads configured value`() {
        val settings = Settings.builder()
            .put("plugins.alerting.sqs_account_id", "123456789012")
            .build()
        assertEquals("123456789012", AlertingSettings.SQS_ACCOUNT_ID.get(settings))
    }

    fun `test account_provider_type reads configured value`() {
        val settings = Settings.builder()
            .put("plugins.alerting.sqs_account_provider_type", "custom_provider")
            .build()
        assertEquals("custom_provider", AlertingSettings.SQS_ACCOUNT_PROVIDER_TYPE.get(settings))
    }

    fun `test provider returns account id from settings`() {
        val settings = Settings.builder()
            .put(PluginSettingSqsAccountIdProvider.SETTING_KEY, "111222333444")
            .build()
        val provider = PluginSettingSqsAccountIdProvider()
        provider.initialize(settings)
        assertEquals(listOf("111222333444"), provider.getAccountIds())
    }

    fun `test provider throws when account id not configured`() {
        val provider = PluginSettingSqsAccountIdProvider()
        provider.initialize(Settings.EMPTY)
        expectThrows(IllegalArgumentException::class.java) {
            provider.getAccountIds()
        }
    }

    fun `test find discovers plugin_setting provider and returns account ids`() {
        val settings = Settings.builder()
            .put(PluginSettingSqsAccountIdProvider.SETTING_KEY, "999888777666")
            .build()
        val provider = SqsAccountIdProvider.find("plugin_setting", settings)
        assertEquals("plugin_setting", provider.getType())
        assertEquals(listOf("999888777666"), provider.getAccountIds())
    }

    fun `test find throws for unknown provider type`() {
        expectThrows(IllegalArgumentException::class.java) {
            SqsAccountIdProvider.find("nonexistent", Settings.EMPTY)
        }
    }
}

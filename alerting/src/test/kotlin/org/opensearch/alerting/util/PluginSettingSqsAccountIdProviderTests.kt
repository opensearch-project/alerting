/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import org.opensearch.common.settings.Settings
import org.opensearch.test.OpenSearchTestCase

class PluginSettingSqsAccountIdProviderTests : OpenSearchTestCase() {

    fun `test type returns plugin_setting`() {
        assertEquals("plugin_setting", PluginSettingSqsAccountIdProvider().getType())
    }

    fun `test getAccountIds returns singleton list when setting has value`() {
        val provider = PluginSettingSqsAccountIdProvider()
        provider.initialize(Settings.builder().put(PluginSettingSqsAccountIdProvider.SETTING_KEY, "123456789012").build())
        assertEquals(listOf("123456789012"), provider.getAccountIds())
    }

    fun `test getAccountIds throws when setting is empty`() {
        val provider = PluginSettingSqsAccountIdProvider()
        provider.initialize(Settings.EMPTY)
        expectThrows(IllegalArgumentException::class.java) {
            provider.getAccountIds()
        }
    }

    fun `test getAccountIds throws when setting is blank`() {
        val provider = PluginSettingSqsAccountIdProvider()
        provider.initialize(Settings.builder().put(PluginSettingSqsAccountIdProvider.SETTING_KEY, "   ").build())
        expectThrows(IllegalArgumentException::class.java) {
            provider.getAccountIds()
        }
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.alerting.settings

import org.opensearch.alerting.util.DestinationType
import org.opensearch.common.settings.SecureSetting
import org.opensearch.common.settings.SecureString
import org.opensearch.common.settings.Setting
import org.opensearch.common.settings.Settings
import java.util.function.Function

/**
 * Settings specific to Destinations. This class is separated from the general AlertingSettings since some Destination
 * types require SecureSettings and need additional logic for retrieving and loading them.
 */
class LegacyOpenDistroDestinationSettings {

    companion object {

        const val DESTINATION_SETTING_PREFIX = "opendistro.alerting.destination."
        const val EMAIL_DESTINATION_SETTING_PREFIX = DESTINATION_SETTING_PREFIX + "email."
        val ALLOW_LIST_ALL = DestinationType.values().toList().map { it.value }
        val ALLOW_LIST_NONE = emptyList<String>()

        val ALLOW_LIST: Setting<List<String>> = Setting.listSetting(
            DESTINATION_SETTING_PREFIX + "allow_list",
            ALLOW_LIST_ALL,
            Function.identity(),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic,
            Setting.Property.Deprecated
        )

        val EMAIL_USERNAME: Setting.AffixSetting<SecureString> = Setting.affixKeySetting(
            EMAIL_DESTINATION_SETTING_PREFIX,
            "username",
            // Needed to coerce lambda to Function type for some reason to avoid argument mismatch compile error
            Function { key: String -> SecureSetting.secureString(key, null) }
        )

        val EMAIL_PASSWORD: Setting.AffixSetting<SecureString> = Setting.affixKeySetting(
            EMAIL_DESTINATION_SETTING_PREFIX,
            "password",
            // Needed to coerce lambda to Function type for some reason to avoid argument mismatch compile error
            Function { key: String -> SecureSetting.secureString(key, null) }
        )

        val HOST_DENY_LIST: Setting<List<String>> = Setting.listSetting(
            "opendistro.destination.host.deny_list",
            emptyList<String>(),
            Function.identity(),
            Setting.Property.NodeScope,
            Setting.Property.Final,
            Setting.Property.Deprecated
        )

        fun loadLegacyDestinationSettings(settings: Settings): Map<String, SecureDestinationSettings> {
            // Only loading Email Destination settings for now since those are the only secure settings needed.
            // If this logic needs to be expanded to support other Destinations, different groups can be retrieved similar
            // to emailAccountNames based on the setting namespace and SecureDestinationSettings should be expanded to support
            // these new settings.
            val emailAccountNames: Set<String> = settings.getGroups(EMAIL_DESTINATION_SETTING_PREFIX).keys
            val emailAccounts: MutableMap<String, SecureDestinationSettings> = mutableMapOf()
            for (emailAccountName in emailAccountNames) {
                // Only adding the settings if they exist
                getLegacySecureDestinationSettings(settings, emailAccountName)?.let {
                    emailAccounts[emailAccountName] = it
                }
            }

            return emailAccounts
        }

        private fun getLegacySecureDestinationSettings(settings: Settings, emailAccountName: String): SecureDestinationSettings? {
            // Using 'use' to emulate Java's try-with-resources on multiple closeable resources.
            // Values are cloned so that we maintain a SecureString, the original SecureStrings will be closed after
            // they have left the scope of this function.
            return getLegacyEmailSettingValue(settings, emailAccountName, EMAIL_USERNAME)?.use { emailUsername ->
                getLegacyEmailSettingValue(settings, emailAccountName, EMAIL_PASSWORD)?.use { emailPassword ->
                    SecureDestinationSettings(emailUsername = emailUsername.clone(), emailPassword = emailPassword.clone())
                }
            }
        }

        private fun <T> getLegacyEmailSettingValue(
            settings: Settings,
            emailAccountName: String,
            emailSetting: Setting.AffixSetting<T>
        ): T? {
            val concreteSetting = emailSetting.getConcreteSettingForNamespace(emailAccountName)
            return concreteSetting.get(settings)
        }

        data class SecureDestinationSettings(val emailUsername: SecureString, val emailPassword: SecureString)
    }
}

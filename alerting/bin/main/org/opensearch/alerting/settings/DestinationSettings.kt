/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.settings

import org.opensearch.common.settings.SecureSetting
import org.opensearch.common.settings.SecureString
import org.opensearch.common.settings.Setting
import org.opensearch.common.settings.Setting.AffixSetting
import org.opensearch.common.settings.Settings
import java.util.function.Function

/**
 * Settings specific to Destinations. This class is separated from the general AlertingSettings since some Destination
 * types require SecureSettings and need additional logic for retrieving and loading them.
 */
class DestinationSettings {
    companion object {

        const val DESTINATION_SETTING_PREFIX = "plugins.alerting.destination."
        const val EMAIL_DESTINATION_SETTING_PREFIX = DESTINATION_SETTING_PREFIX + "email."
        val ALLOW_LIST_NONE = emptyList<String>()

        val ALLOW_LIST: Setting<List<String>> = Setting.listSetting(
            DESTINATION_SETTING_PREFIX + "allow_list",
            LegacyOpenDistroDestinationSettings.ALLOW_LIST,
            Function.identity(),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val EMAIL_USERNAME: Setting.AffixSetting<SecureString> = Setting.affixKeySetting(
            EMAIL_DESTINATION_SETTING_PREFIX,
            "username",
            // Needed to coerce lambda to Function type for some reason to avoid argument mismatch compile error
            Function { key: String ->
                SecureSetting.secureString(
                    key,
                    fallback(key, LegacyOpenDistroDestinationSettings.EMAIL_USERNAME, "plugins", "opendistro")
                )
            }
        )

        val EMAIL_PASSWORD: Setting.AffixSetting<SecureString> = Setting.affixKeySetting(
            EMAIL_DESTINATION_SETTING_PREFIX,
            "password",
            // Needed to coerce lambda to Function type for some reason to avoid argument mismatch compile error
            Function { key: String ->
                SecureSetting.secureString(
                    key,
                    fallback(key, LegacyOpenDistroDestinationSettings.EMAIL_PASSWORD, "plugins", "opendistro")
                )
            }
        )

        val HOST_DENY_LIST: Setting<List<String>> = Setting.listSetting(
            "plugins.destination.host.deny_list",
            LegacyOpenDistroDestinationSettings.HOST_DENY_LIST,
            Function.identity(),
            Setting.Property.NodeScope,
            Setting.Property.Final
        )

        fun loadDestinationSettings(settings: Settings): Map<String, SecureDestinationSettings> {
            // Only loading Email Destination settings for now since those are the only secure settings needed.
            // If this logic needs to be expanded to support other Destinations, different groups can be retrieved similar
            // to emailAccountNames based on the setting namespace and SecureDestinationSettings should be expanded to support
            // these new settings.
            val emailAccountNames: Set<String> = settings.getGroups(EMAIL_DESTINATION_SETTING_PREFIX).keys
            val emailAccounts: MutableMap<String, SecureDestinationSettings> = mutableMapOf()
            for (emailAccountName in emailAccountNames) {
                // Only adding the settings if they exist
                getSecureDestinationSettings(settings, emailAccountName)?.let {
                    emailAccounts[emailAccountName] = it
                }
            }

            return emailAccounts
        }

        private fun getSecureDestinationSettings(settings: Settings, emailAccountName: String): SecureDestinationSettings? {
            // Using 'use' to emulate Java's try-with-resources on multiple closeable resources.
            // Values are cloned so that we maintain a SecureString, the original SecureStrings will be closed after
            // they have left the scope of this function.
            return getEmailSettingValue(settings, emailAccountName, EMAIL_USERNAME)?.use { emailUsername ->
                getEmailSettingValue(settings, emailAccountName, EMAIL_PASSWORD)?.use { emailPassword ->
                    SecureDestinationSettings(emailUsername = emailUsername.clone(), emailPassword = emailPassword.clone())
                }
            }
        }

        private fun <T> getEmailSettingValue(settings: Settings, emailAccountName: String, emailSetting: Setting.AffixSetting<T>): T? {
            val concreteSetting = emailSetting.getConcreteSettingForNamespace(emailAccountName)
            return concreteSetting.get(settings)
        }

        private fun <T> fallback(key: String, affixSetting: AffixSetting<T>, regex: String, replacement: String): Setting<T>? {
            return if ("_na_" == key) {
                affixSetting.getConcreteSettingForNamespace(key)
            } else {
                affixSetting.getConcreteSetting(key.replace(regex.toRegex(), replacement))
            }
        }

        data class SecureDestinationSettings(val emailUsername: SecureString, val emailPassword: SecureString)
    }
}

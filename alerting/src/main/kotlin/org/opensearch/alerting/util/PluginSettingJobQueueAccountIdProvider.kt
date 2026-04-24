/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import org.opensearch.common.settings.Settings
import org.opensearch.commons.utils.scheduler.JobQueueAccountIdProvider

/**
 * [JobQueueAccountIdProvider] that reads the job queue account ID from the alerting plugin setting.
 * Throws [IllegalArgumentException] if the setting is missing or blank.
 */
class PluginSettingJobQueueAccountIdProvider : JobQueueAccountIdProvider {

    override fun getType(): String = "plugin_setting"

    private lateinit var settings: Settings

    override fun initialize(settings: Settings) {
        this.settings = settings
    }

    override fun getAccountIds(): List<String> {
        val accountId = settings.get(SETTING_KEY)
        require(!accountId.isNullOrBlank()) { "Setting [$SETTING_KEY] must be configured" }
        return listOf(accountId)
    }

    companion object {
        const val SETTING_KEY = "plugins.alerting.job_queue_account_id"
    }
}

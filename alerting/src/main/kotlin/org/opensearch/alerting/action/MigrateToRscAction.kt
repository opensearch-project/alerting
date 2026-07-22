/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.ActionType

/**
 * Admin-only action that backfills the fields required by the security plugin's resource-sharing
 * framework onto legacy monitor and workflow docs in `.opendistro-alerting-config`. Run once per
 * cluster after enabling `plugins.security.experimental.resource_sharing.enabled` and before
 * calling the security plugin's `POST /_plugins/_security/api/resources/migrate`.
 */
class MigrateToRscAction private constructor() : ActionType<MigrateToRscResponse>(NAME, ::MigrateToRscResponse) {
    companion object {
        val INSTANCE = MigrateToRscAction()
        const val NAME = "cluster:admin/opensearch/alerting/rsc/migrate"
    }
}

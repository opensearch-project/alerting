/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.apache.logging.log4j.LogManager
import org.opensearch.alerting.action.MigrateToRscAction
import org.opensearch.alerting.action.MigrateToRscRequest
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.POST
import org.opensearch.rest.action.RestToXContentListener
import org.opensearch.transport.client.node.NodeClient

private val log = LogManager.getLogger(RestMigrateToRscAction::class.java)

/**
 * `POST /_plugins/_alerting/_migrate_to_rsc`
 *
 * Admin-only trigger that backfills `resource_type` and scratch owner fields onto existing
 * monitor/workflow docs so the security plugin's resource-sharing framework can classify them
 * and admins can then run the security plugin's `POST /_plugins/_security/api/resources/migrate`
 * to seed the sharing index. Idempotent: subsequent invocations noop docs that already carry
 * `resource_type`.
 *
 * Access control: guarded by the transport action name
 * `cluster:admin/opensearch/alerting/rsc/migrate`, which must be granted only via
 * `all_access` (or an equivalent admin role) — not through the per-resource access levels in
 * `resource-access-levels.yml`.
 */
class RestMigrateToRscAction : BaseRestHandler() {

    override fun getName(): String = "migrate_alerting_to_rsc_action"

    override fun routes(): List<Route> =
        listOf(Route(POST, "/_plugins/_alerting/_migrate_to_rsc"))

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        log.info("Received request to migrate alerting docs to resource-sharing format")
        return RestChannelConsumer { channel ->
            client.execute(
                MigrateToRscAction.INSTANCE,
                MigrateToRscRequest(),
                RestToXContentListener(channel),
            )
        }
    }
}

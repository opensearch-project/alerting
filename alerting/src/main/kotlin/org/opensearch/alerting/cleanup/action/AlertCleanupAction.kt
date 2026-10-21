/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.cleanup.action

import org.opensearch.action.ActionType
import org.opensearch.core.common.io.stream.Writeable

/**
 * Announces to the cluster that a job's alerts need to be moved to the history index.
 *
 * The action is an announcement, not a request for the work to be done by the time it returns: each recipient races for
 * the cleanup task's lock and the winner drains asynchronously. A successful response therefore means "the nodes were
 * told", which is all the delete path needs, since the durable cleanup task -- not this message -- is what guarantees
 * the work eventually happens.
 */
class AlertCleanupAction : ActionType<AlertCleanupResponse>(NAME, reader) {
    companion object {
        val INSTANCE = AlertCleanupAction()
        const val NAME = "cluster:admin/opensearch/alerting/alerts/cleanup"

        val reader = Writeable.Reader { AlertCleanupResponse(it) }
    }

    override fun getResponseReader(): Writeable.Reader<AlertCleanupResponse> = reader
}

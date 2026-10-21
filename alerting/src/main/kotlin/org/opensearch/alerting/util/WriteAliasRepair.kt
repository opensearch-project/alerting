/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import org.apache.logging.log4j.LogManager
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.alerting.alerts.AlertIndices.Companion.ALERT_HISTORY_WRITE_INDEX
import org.opensearch.alerting.alerts.AlertIndices.Companion.FINDING_HISTORY_WRITE_INDEX
import org.opensearch.alerting.comments.CommentsIndices.Companion.COMMENTS_HISTORY_WRITE_INDEX
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.snapshots.RestoreContext
import org.opensearch.transport.client.Client
import org.opensearch.transport.client.IndicesAdminClient

/**
 * Fixes a write alias that names two or more indices after a snapshot restore.
 *
 * Alerting creates each write alias on one index and moves it on rollover, so the alias names one index. If this cluster already
 * created its own index behind the alias, restoring the snapshot's index brings a second index with the same alias, because
 * restore keeps aliases. OpenSearch then can't tell which index to write to, so every write and rollover through the alias
 * fails. This keeps the alias on one index.
 */
class WriteAliasRepair(private val client: Client, private val clusterService: ClusterService) {

    /** Data for one index behind the alias: its name, creation date and is_write_index flag (null when not set, as alerting creates it). */
    data class AliasedIndex(val index: String, val creationDate: Long, val isWriteIndex: Boolean?)

    /** Repairs each alias in [WRITE_ALIASES_TO_REPAIR]. A failure on one alias is logged and does not stop the others. */
    suspend fun repair(context: RestoreContext) {
        val metadata = clusterService.state().metadata
        val restored = context.restoredIndices().map { it.name }.toSet()
        WRITE_ALIASES_TO_REPAIR.forEach { alias ->
            try {
                repairAlias(metadata, restored, alias)
            } catch (e: Exception) {
                log.error("Failed to repair write alias [$alias] after restoring snapshot [${context.snapshot()}]", e)
            }
        }
    }

    /**
     * Makes [alias] name only one index, by removing it from the extra indices, which [indicesToDetach] picks. Does nothing
     * unless this restore brought back one of the alias's indices. The extra indices stay searchable, because alerting searches
     * them by index pattern, not by alias.
     */
    private suspend fun repairAlias(metadata: Metadata, restored: Set<String>, alias: String) {
        val aliasedIndices = metadata.indicesLookup[alias]?.indices
            ?.map { AliasedIndex(it.index.name, it.creationDate, it.aliases[alias]?.writeIndex()) } ?: return
        if (aliasedIndices.none { it.index in restored }) return

        val detach = indicesToDetach(aliasedIndices)
        if (detach.isEmpty()) return

        log.warn("Write alias [$alias] names ${aliasedIndices.size} indices and none is the write index. Removing it from $detach.")
        val request = IndicesAliasesRequest()
        // must_exist=false: a second restore finishing at the same time may already have removed it, so don't fail on that.
        detach.forEach { request.addAliasAction(AliasActions.remove().index(it).alias(alias).mustExist(false)) }
        client.admin().indices().suspendUntil<IndicesAdminClient, AcknowledgedResponse> { aliases(request, it) }
    }

    companion object {
        private val log = LogManager.getLogger(WriteAliasRepair::class.java)

        /**
         * The write aliases this repair covers, currently alerting's alert, finding and comments history aliases. An alias added
         * here must have a fixed name (aliases a monitor names in its custom data sources don't), must be one alerting's plugin
         * identity can change, and must be on indices that are restored from a snapshot.
         */
        private val WRITE_ALIASES_TO_REPAIR = listOf(ALERT_HISTORY_WRITE_INDEX, FINDING_HISTORY_WRITE_INDEX, COMMENTS_HISTORY_WRITE_INDEX)

        /**
         * Returns the indices to remove the alias from, or an empty list when the alias is fine.
         *
         * The alias is left alone when it names one index, or when any index has an is_write_index flag (someone chose that
         * layout). Otherwise the index with the highest rollover counter keeps it (-000002 beats -1), because rollover names the
         * next index by adding one to that counter, so the next name is always free. Ties go to the newest creation date, then
         * to the name that sorts last alphabetically, so every repair picks the same index.
         */
        fun indicesToDetach(aliasedIndices: List<AliasedIndex>): List<String> {
            if (aliasedIndices.size <= 1 || aliasedIndices.any { it.isWriteIndex != null }) return emptyList()
            val keep = aliasedIndices.maxWith(compareBy<AliasedIndex>({ rolloverCounter(it.index) }, { it.creationDate }, { it.index }))
            return aliasedIndices.map { it.index }.filter { it != keep.index }.sorted()
        }

        /** The number after the last '-' in the index name, or -1 when there is none, so such an index never wins. */
        private fun rolloverCounter(index: String): Long = index.substringAfterLast('-').toLongOrNull() ?: -1
    }
}

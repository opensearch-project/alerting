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
import org.opensearch.cluster.service.ClusterService
import org.opensearch.snapshots.RestoreContext
import org.opensearch.transport.client.Client
import org.opensearch.transport.client.IndicesAdminClient

/**
 * Fixes a write alias that names two or more indices and has no write index.
 *
 * Alerting creates each write alias on one index and moves it on rollover, so the alias names one index. If this cluster already
 * created its own index behind the alias, restoring the snapshot's index brings a second index with the same alias, because
 * restore keeps aliases. OpenSearch then can't tell which index to write to, so every write and rollover through the alias
 * fails. This keeps the alias on one index.
 *
 * [client] must be the plugin client, so the repair runs as this plugin's identity, which can only change alerting's own system
 * indices. An index restored under another name (rename_pattern) is not one of them, so the repair can't remove the alias from it.
 */
class WriteAliasRepair(private val client: Client, private val clusterService: ClusterService) {

    /** Data for one index behind the alias: its name, creation date and is_write_index flag (null when not set, as alerting creates it). */
    data class AliasedIndex(val index: String, val creationDate: Long, val isWriteIndex: Boolean?)

    /**
     * Repairs each alias in [WRITE_ALIASES_TO_REPAIR] that names an index this restore brought back. A failure on one alias is
     * logged and does not stop the others.
     */
    suspend fun repair(context: RestoreContext) {
        val restored = context.restoredIndices().map { it.name }.toSet()
        WRITE_ALIASES_TO_REPAIR.forEach { alias ->
            try {
                val aliasedIndices = aliasedIndices(alias)
                if (aliasedIndices.any { it.index in restored }) repairAlias(alias, aliasedIndices)
            } catch (e: Exception) {
                log.error("Failed to repair write alias [$alias] after restoring snapshot [${context.snapshot()}]", e)
            }
        }
    }

    /**
     * Repairs [alias] whatever made it name more than one index. Throws if [alias] is not in [WRITE_ALIASES_TO_REPAIR]; any other
     * failure is logged, not thrown.
     */
    suspend fun repair(alias: String) {
        require(alias in WRITE_ALIASES_TO_REPAIR) { "Write alias [$alias] is not one of $WRITE_ALIASES_TO_REPAIR" }
        try {
            repairAlias(alias, aliasedIndices(alias))
        } catch (e: Exception) {
            log.error("Failed to repair write alias [$alias]", e)
        }
    }

    private fun aliasedIndices(alias: String): List<AliasedIndex> =
        clusterService.state().metadata.indicesLookup[alias]?.indices
            ?.map { AliasedIndex(it.index.name, it.creationDate, it.aliases[alias]?.writeIndex()) } ?: emptyList()

    /**
     * Makes [alias] name only one index, by removing it from the extra indices, which [indicesToDetach] picks. The extra indices
     * stay searchable, because alerting searches them by index pattern, not by alias.
     */
    private suspend fun repairAlias(alias: String, aliasedIndices: List<AliasedIndex>) {
        if (aliasedIndices.size <= 1) return
        if (allMarkedNotWrite(aliasedIndices)) {
            log.error(
                "Write alias [$alias] names ${aliasedIndices.map { it.index }.sorted()} and each has is_write_index set to false, so " +
                    "writes through it fail. Set is_write_index to true on one of them, or remove the alias from all but one."
            )
            return
        }
        val detach = indicesToDetach(aliasedIndices)
        if (detach.isEmpty()) return
        val keep = aliasedIndices.first { it.index !in detach }.index

        log.warn("Write alias [$alias] names ${aliasedIndices.size} indices and none is the write index. Removing it from $detach.")
        val request = IndicesAliasesRequest()
        // must_exist=false: a second repair running at the same time may already have removed it, so don't fail on that.
        detach.forEach { request.addAliasAction(AliasActions.remove().index(it).alias(alias).mustExist(false)) }
        val response = client.admin().indices().suspendUntil<IndicesAdminClient, AcknowledgedResponse> { aliases(request, it) }
        // Not acknowledged means the nodes did not confirm in time, not that the change failed, so check the alias again
        if (response.isAcknowledged || aliasedIndices(alias).map { it.index } == listOf(keep)) {
            log.info("Repaired write alias [$alias]. It now names only [$keep].")
        } else {
            log.warn(
                "Could not confirm the repair of write alias [$alias]: the request was not acknowledged and this node still sees it on " +
                    "more than one index. The next history rollover checks it again."
            )
        }
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
         * Returns the indices to remove the alias from, or an empty list when it is left alone.
         *
         * The alias is left alone when it names one index, or when an index has is_write_index set to true (OpenSearch writes to
         * that index, so the alias works). An index with is_write_index set to false never keeps the alias, because someone marked
         * it as not to be written to; when every index is marked that way the alias is left alone. Otherwise the index with the
         * highest rollover counter keeps it (-000002 beats -1), because rollover names the next index by adding one to that
         * counter. Ties go to the newest creation date, then to the name that sorts last alphabetically, so every repair picks the
         * same index.
         */
        fun indicesToDetach(aliasedIndices: List<AliasedIndex>): List<String> {
            if (aliasedIndices.any { it.isWriteIndex == true } || aliasedIndices.all { it.isWriteIndex == false }) return emptyList()
            val keep = aliasedIndices.filter { it.isWriteIndex == null }
                .maxWith(compareBy<AliasedIndex>({ rolloverCounter(it.index) }, { it.creationDate }, { it.index }))
            return aliasedIndices.map { it.index }.filter { it != keep.index }.sorted()
        }

        /** True when each index behind the alias has is_write_index set to false, so it has no write index. */
        fun allMarkedNotWrite(aliasedIndices: List<AliasedIndex>): Boolean = aliasedIndices.all { it.isWriteIndex == false }

        /** The number after the last '-' in the index name, or -1 when there is none, so such an index never wins. */
        private fun rolloverCounter(index: String): Long = index.substringAfterLast('-').toLongOrNull() ?: -1
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import org.opensearch.action.search.SearchRequest
import org.opensearch.client.Client
import org.opensearch.client.node.NodeClient
import org.opensearch.cluster.service.ClusterService
import org.opensearch.commons.alerting.model.ClusterMetricsInput
import org.opensearch.commons.alerting.model.DocLevelMonitorInput
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.SearchInput

class CrossClusterMonitorUtils {
    companion object {

        /**
         * Uses the monitor inputs to determine whether the monitor makes calls to remote clusters.
         * @param monitor The monitor to evaluate.
         * @param localClusterName The name of the local cluster.
         * @return TRUE if the monitor makes calls to remote clusters; otherwise returns FALSE.
         */
        @JvmStatic
        fun isRemoteMonitor(monitor: Monitor, localClusterName: String): Boolean {
            var isRemoteMonitor = false
            monitor.inputs.forEach inputCheck@{
                when (it) {
                    is ClusterMetricsInput -> {
                        it.clusters.forEach { clusterName ->
                            if (clusterName != localClusterName) {
                                isRemoteMonitor = true
                                return@inputCheck
                            }
                        }
                    }
                    is SearchInput -> {
                        // Remote indexes follow the pattern "<CLUSTER_NAME>:<INDEX_NAME>".
                        // Index entries without a CLUSTER_NAME indicate they're store on the local cluster.
                        it.indices.forEach { index ->
                            val clusterName = parseClusterName(index)
                            if (clusterName != localClusterName) {
                                isRemoteMonitor = true
                                return@inputCheck
                            }
                        }
                    }
                    is DocLevelMonitorInput -> {
                        // TODO: When document level monitors are supported, this check will be similar to SearchInput.
                        throw IllegalArgumentException("Per document monitors do not currently support cross-cluster search.")
                    }
                    else -> {
                        throw IllegalArgumentException("Unsupported input type: ${it.name()}.")
                    }
                }
            }
            return isRemoteMonitor
        }

        /**
         * Uses the monitor inputs to determine whether the monitor makes calls to remote clusters.
         * @param monitor The monitor to evaluate.
         * @param clusterService Used to retrieve the name of the local cluster.
         * @return TRUE if the monitor makes calls to remote clusters; otherwise returns FALSE.
         */
        @JvmStatic
        fun isRemoteMonitor(monitor: Monitor, clusterService: ClusterService): Boolean {
            return isRemoteMonitor(monitor = monitor, localClusterName = clusterService.clusterName.value())
        }

        /**
         * Parses the list of indexes into a map of CLUSTER_NAME to List<INDEX_NAME>.
         * @param indexes A list of index names in "<CLUSTER_NAME>:<INDEX_NAME>" format.
         * @param localClusterName The name of the local cluster.
         * @return A map of CLUSTER_NAME to List<INDEX_NAME>
         */
        @JvmStatic
        fun separateClusterIndexes(indexes: List<String>, localClusterName: String): HashMap<String, MutableList<String>> {
            val output = hashMapOf<String, MutableList<String>>()
            indexes.forEach { index ->
                var clusterName = parseClusterName(index)
                val indexName = parseIndexName(index)

                // If the index entry does not have a CLUSTER_NAME, it indicates the index is on the local cluster.
                if (clusterName.isEmpty()) clusterName = localClusterName

                output.getOrPut(clusterName) { mutableListOf() }.add(indexName)
            }
            return output
        }

        /**
         * Parses the list of indexes into a map of CLUSTER_NAME to List<INDEX_NAME>.
         * @param indexes A list of index names in "<CLUSTER_NAME>:<INDEX_NAME>" format.
         *      Local indexes can also be in "<INDEX_NAME>" format.
         * @param clusterService Used to retrieve the name of the local cluster.
         * @return A map of CLUSTER_NAME to List<INDEX_NAME>
         */
        @JvmStatic
        fun separateClusterIndexes(indexes: List<String>, clusterService: ClusterService): HashMap<String, MutableList<String>> {
            return separateClusterIndexes(indexes = indexes, localClusterName = clusterService.clusterName.value())
        }

        /**
         * The [NodeClient] used by the plugin cannot execute searches against local indexes
         * using format "<LOCAL_CLUSTER_NAME>:<INDEX_NAME>". That format only supports querying remote indexes.
         * This function formats a list of indexes to be supplied directly to a [SearchRequest].
         * @param indexes A list of index names in "<CLUSTER_NAME>:<INDEX_NAME>" format.
         * @param localClusterName The name of the local cluster.
         * @return A list of indexes with any remote indexes in "<CLUSTER_NAME>:<INDEX_NAME>" format,
         *      and any local indexes in "<INDEX_NAME>" format.
         */
        @JvmStatic
        fun parseIndexesForRemoteSearch(indexes: List<String>, localClusterName: String): List<String> {
            return indexes.map {
                var index = it
                val clusterName = parseClusterName(it)
                if (clusterName.isNotEmpty() && clusterName == localClusterName) {
                    index = parseIndexName(it)
                }
                index
            }
        }

        /**
         * The [NodeClient] used by the plugin cannot execute searches against local indexes
         * using format "<LOCAL_CLUSTER_NAME>:<INDEX_NAME>". That format only supports querying remote indexes.
         * This function formats a list of indexes to be supplied directly to a [SearchRequest].
         * @param indexes A list of index names in "<CLUSTER_NAME>:<INDEX_NAME>" format.
         * @param clusterService Used to retrieve the name of the local cluster.
         * @return A list of indexes with any remote indexes in "<CLUSTER_NAME>:<INDEX_NAME>" format,
         *      and any local indexes in "<INDEX_NAME>" format.
         */
        @JvmStatic
        fun parseIndexesForRemoteSearch(indexes: List<String>, clusterService: ClusterService): List<String> {
            return parseIndexesForRemoteSearch(indexes = indexes, localClusterName = clusterService.clusterName.value())
        }

        /**
         * Uses the clusterName to determine whether the target client is the local or a remote client,
         * and returns the appropriate client.
         * @param clusterName The name of the cluster to evaluate.
         * @param client The local [NodeClient].
         * @param localClusterName The name of the local cluster.
         * @return The local [NodeClient] for the local cluster, or a remote client for a remote cluster.
         */
        @JvmStatic
        fun getClientForCluster(clusterName: String, client: Client, localClusterName: String): Client {
            return if (clusterName == localClusterName) client else client.getRemoteClusterClient(clusterName)
        }

        /**
         * Uses the clusterName to determine whether the target client is the local or a remote client,
         * and returns the appropriate client.
         * @param clusterName The name of the cluster to evaluate.
         * @param client The local [NodeClient].
         * @param clusterService Used to retrieve the name of the local cluster.
         * @return The local [NodeClient] for the local cluster, or a remote client for a remote cluster.
         */
        @JvmStatic
        fun getClientForCluster(clusterName: String, client: Client, clusterService: ClusterService): Client {
            return getClientForCluster(clusterName = clusterName, client = client, localClusterName = clusterService.clusterName.value())
        }

        /**
         * Uses the index name to determine whether the target client is the local or a remote client,
         * and returns the appropriate client.
         * @param index The name of the index to evaluate.
         *      Can be in either "<CLUSTER_NAME>:<INDEX_NAME>" or "<INDEX_NAME>" format.
         * @param client The local [NodeClient].
         * @param localClusterName The name of the local cluster.
         * @return The local [NodeClient] for the local cluster, or a remote client for a remote cluster.
         */
        @JvmStatic
        fun getClientForIndex(index: String, client: Client, localClusterName: String): Client {
            val clusterName = parseClusterName(index)
            return if (clusterName.isNotEmpty() && clusterName != localClusterName)
                client.getRemoteClusterClient(clusterName) else client
        }

        /**
         * Uses the index name to determine whether the target client is the local or a remote client,
         * and returns the appropriate client.
         * @param index The name of the index to evaluate.
         *      Can be in either "<CLUSTER_NAME>:<INDEX_NAME>" or "<INDEX_NAME>" format.
         * @param client The local [NodeClient].
         * @param clusterService Used to retrieve the name of the local cluster.
         * @return The local [NodeClient] for the local cluster, or a remote client for a remote cluster.
         */
        @JvmStatic
        fun getClientForIndex(index: String, client: Client, clusterService: ClusterService): Client {
            return getClientForIndex(index = index, client = client, localClusterName = clusterService.clusterName.value())
        }

        /**
         * @param index The name of the index to evaluate.
         *      Can be in either "<CLUSTER_NAME>:<INDEX_NAME>" or "<INDEX_NAME>" format.
         * @return The cluster name if present; else an empty string.
         */
        @JvmStatic
        fun parseClusterName(index: String): String {
            return if (index.contains(":")) index.split(":").getOrElse(0) { "" }
            else ""
        }

        /**
         * @param index The name of the index to evaluate.
         *      Can be in either "<CLUSTER_NAME>:<INDEX_NAME>" or "<INDEX_NAME>" format.
         * @return The index name.
         */
        @JvmStatic
        fun parseIndexName(index: String): String {
            return if (index.contains(":")) index.split(":").getOrElse(1) { index }
            else index
        }

        /**
         * If clusterName is provided, combines the inputs into "<CLUSTER_NAME>:<INDEX_NAME>" format.
         * @param clusterName
         * @param indexName
         * @return The formatted string.
         */
        @JvmStatic
        fun formatClusterAndIndexName(clusterName: String, indexName: String): String {
            return if (clusterName.isNotEmpty()) "$clusterName:$indexName"
            else indexName
        }

        fun isRemoteClusterIndex(index: String, clusterService: ClusterService): Boolean {
            val clusterName = parseClusterName(index)
            return clusterName.isNotEmpty() && clusterService.clusterName.value() != clusterName
        }
    }
}

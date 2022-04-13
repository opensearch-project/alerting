/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import org.opensearch.action.ActionListener
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.alerting.alerts.AlertIndices
import org.opensearch.alerting.core.ScheduledJobIndices
import org.opensearch.client.IndicesAdminClient
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentType

class IndexUtils {

    companion object {
        const val _META = "_meta"
        const val SCHEMA_VERSION = "schema_version"
        const val NO_SCHEMA_VERSION = 0

        var scheduledJobIndexSchemaVersion: Int
            private set
        var alertIndexSchemaVersion: Int
            private set
        var findingIndexSchemaVersion: Int
            private set

        var scheduledJobIndexUpdated: Boolean = false
            private set
        var alertIndexUpdated: Boolean = false
            private set
        var findingIndexUpdated: Boolean = false
            private set
        var lastUpdatedAlertHistoryIndex: String? = null
        var lastUpdatedFindingHistoryIndex: String? = null

        init {
            scheduledJobIndexSchemaVersion = getSchemaVersion(ScheduledJobIndices.scheduledJobMappings())
            alertIndexSchemaVersion = getSchemaVersion(AlertIndices.alertMapping())
            findingIndexSchemaVersion = getSchemaVersion(AlertIndices.findingMapping())
        }

        @JvmStatic
        fun scheduledJobIndexUpdated() {
            scheduledJobIndexUpdated = true
        }

        @JvmStatic
        fun alertIndexUpdated() {
            alertIndexUpdated = true
        }

        @JvmStatic
        fun findingIndexUpdated() {
            findingIndexUpdated = true
        }

        @JvmStatic
        fun getSchemaVersion(mapping: String): Int {
            val xcp = XContentType.JSON.xContent().createParser(
                NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE, mapping
            )

            while (!xcp.isClosed) {
                val token = xcp.currentToken()
                if (token != null && token != XContentParser.Token.END_OBJECT && token != XContentParser.Token.START_OBJECT) {
                    if (xcp.currentName() != _META) {
                        xcp.nextToken()
                        xcp.skipChildren()
                    } else {
                        while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                            when (xcp.currentName()) {
                                SCHEMA_VERSION -> {
                                    val version = xcp.intValue()
                                    require(version > -1)
                                    return version
                                }
                                else -> xcp.nextToken()
                            }
                        }
                    }
                }
                xcp.nextToken()
            }
            return NO_SCHEMA_VERSION
        }

        @JvmStatic
        fun getIndexNameWithAlias(clusterState: ClusterState, alias: String): String {
            return clusterState.metadata.indices.first { it.value.aliases.containsKey(alias) }.key
        }

        @JvmStatic
        fun shouldUpdateIndex(index: IndexMetadata, mapping: String): Boolean {
            var oldVersion = NO_SCHEMA_VERSION
            val newVersion = getSchemaVersion(mapping)

            val indexMapping = index.mapping()?.sourceAsMap()
            if (indexMapping != null && indexMapping.containsKey(_META) && indexMapping[_META] is HashMap<*, *>) {
                val metaData = indexMapping[_META] as HashMap<*, *>
                if (metaData.containsKey(SCHEMA_VERSION)) {
                    oldVersion = metaData[SCHEMA_VERSION] as Int
                }
            }
            return newVersion > oldVersion
        }

        @JvmStatic
        fun updateIndexMapping(
            index: String,
            type: String,
            mapping: String,
            clusterState: ClusterState,
            client: IndicesAdminClient,
            actionListener: ActionListener<AcknowledgedResponse>
        ) {
            if (clusterState.metadata.indices.containsKey(index)) {
                if (shouldUpdateIndex(clusterState.metadata.indices[index], mapping)) {
                    val putMappingRequest: PutMappingRequest = PutMappingRequest(index).type(type).source(mapping, XContentType.JSON)
                    client.putMapping(putMappingRequest, actionListener)
                } else {
                    actionListener.onResponse(AcknowledgedResponse(true))
                }
            }
        }
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.commons.alerting.util.IndexUtils.Companion.INDEX_PATTERN_REGEX
import org.opensearch.commons.utils.CLUSTER_PATTERN_REGEX
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import java.io.IOException

class GetRemoteIndexesRequest : ActionRequest {
    var indexes: List<String> = listOf()
    var includeMappings: Boolean

    constructor(indexes: List<String>, includeMappings: Boolean) : super() {
        this.indexes = indexes
        this.includeMappings = includeMappings
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        sin.readStringList(),
        sin.readBoolean()
    )

    override fun validate(): ActionRequestValidationException? {
        return null
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeStringArray(indexes.toTypedArray())
        out.writeBoolean(includeMappings)
    }

    /**
     * Validates the request [indexes].
     * @return TRUE if all entries are valid; else FALSE.
     */
    fun isValid(): Boolean {
        return indexes.isNotEmpty() && indexes.all { validPattern(it) }
    }

    /**
     * Validates individual entries in the request [indexes].
     *
     * @param pattern The entry to evaluate. The expected patterns are `<index-pattern>` for a local index, and
     * `<cluster-pattern>:<index-pattern>` for remote indexes. These patterns are consistent with the `GET _resolve/index` API.
     * @return TRUE if the entry is valid; else FALSE.
     */
    private fun validPattern(pattern: String): Boolean {
        // In some situations, `<cluster-pattern>` could contain a `:` character.
        // Identifying the `<index-pattern>` based on the last occurrence of `:` in the pattern.
        val separatorIndex = pattern.lastIndexOf(":")
        return if (separatorIndex == -1) {
            // Indicates a local index pattern.
            INDEX_PATTERN_REGEX.matches(pattern)
        } else {
            // Indicates a remote index pattern.
            val clusterPattern = pattern.substring(0, separatorIndex)
            val indexPattern = pattern.substring(separatorIndex + 1)
            CLUSTER_PATTERN_REGEX.matches(clusterPattern) && INDEX_PATTERN_REGEX.matches(indexPattern)
        }
    }

    companion object {
        const val INVALID_PATTERN_MESSAGE = "Indexes includes an invalid pattern."
        const val INDEXES_FIELD = "indexes"
        const val INCLUDE_MAPPINGS_FIELD = "include_mappings"
    }
}

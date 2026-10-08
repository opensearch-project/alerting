/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import org.opensearch.Version
import org.opensearch.alerting.parser
import org.opensearch.cluster.ClusterName
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.test.OpenSearchTestCase
import java.lang.NumberFormatException
import kotlin.test.assertFailsWith

class IndexUtilsTests : OpenSearchTestCase() {

    fun `test get schema version`() {
        val message = "{\"user\":{ \"name\":\"test\"},\"_meta\":{\"schema_version\": 1}}"

        val schemaVersion = IndexUtils.getSchemaVersion(message)
        assertEquals(1, schemaVersion)
    }

    fun `test get schema version without _meta`() {
        val message = "{\"user\":{ \"name\":\"test\"}}"

        val schemaVersion = IndexUtils.getSchemaVersion(message)
        assertEquals(0, schemaVersion)
    }

    fun `test get schema version without schema_version`() {
        val message = "{\"user\":{ \"name\":\"test\"},\"_meta\":{\"test\": 1}}"

        val schemaVersion = IndexUtils.getSchemaVersion(message)
        assertEquals(0, schemaVersion)
    }

    fun `test get schema version with negative schema_version`() {
        val message = "{\"user\":{ \"name\":\"test\"},\"_meta\":{\"schema_version\": -1}}"

        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException") {
            IndexUtils.getSchemaVersion(message)
        }
    }

    fun `test get schema version with wrong schema_version`() {
        val message = "{\"user\":{ \"name\":\"test\"},\"_meta\":{\"schema_version\": \"wrong\"}}"

        assertFailsWith(NumberFormatException::class, "Expected NumberFormatException") {
            IndexUtils.getSchemaVersion(message)
        }
    }

    fun `test should update index without original version`() {
        val indexContent = "{\"testIndex\":{\"settings\":{\"index\":{\"creation_date\":\"1558407515699\"," +
            "\"number_of_shards\":\"1\",\"number_of_replicas\":\"1\",\"uuid\":\"t-VBBW6aR6KpJ3XP5iISOA\"," +
            "\"version\":{\"created\":\"6040399\"},\"provided_name\":\"data_test\"}},\"mapping_version\":123," +
            "\"settings_version\":123,\"aliases_version\":1,\"mappings\":{\"_doc\":{\"properties\":{\"name\":{\"type\":\"keyword\"}}}}}}"
        val newMapping = "{\"_meta\":{\"schema_version\":10},\"properties\":{\"name\":{\"type\":\"keyword\"}}}"
        val index: IndexMetadata = IndexMetadata.fromXContent(parser(indexContent))

        val shouldUpdateIndex = IndexUtils.shouldUpdateIndex(index, newMapping)
        assertTrue(shouldUpdateIndex)
    }

    fun `test should update index with lagged version`() {
        val indexContent = "{\"testIndex\":{\"settings\":{\"index\":{\"creation_date\":\"1558407515699\"," +
            "\"number_of_shards\":\"1\",\"number_of_replicas\":\"1\",\"uuid\":\"t-VBBW6aR6KpJ3XP5iISOA\"," +
            "\"version\":{\"created\":\"6040399\"},\"provided_name\":\"data_test\"}},\"mapping_version\":123," +
            "\"settings_version\":123,\"aliases_version\":1,\"mappings\":{\"_doc\":{\"_meta\":{\"schema_version\":1},\"properties\":" +
            "{\"name\":{\"type\":\"keyword\"}}}}}}"
        val newMapping = "{\"_meta\":{\"schema_version\":10},\"properties\":{\"name\":{\"type\":\"keyword\"}}}"
        val index: IndexMetadata = IndexMetadata.fromXContent(parser(indexContent))

        val shouldUpdateIndex = IndexUtils.shouldUpdateIndex(index, newMapping)
        assertTrue(shouldUpdateIndex)
    }

    fun `test should update index with same version`() {
        val indexContent = "{\"testIndex\":{\"settings\":{\"index\":{\"creation_date\":\"1558407515699\"," +
            "\"number_of_shards\":\"1\",\"number_of_replicas\":\"1\",\"uuid\":\"t-VBBW6aR6KpJ3XP5iISOA\"," +
            "\"version\":{\"created\":\"6040399\"},\"provided_name\":\"data_test\"}},\"mapping_version\":\"1\"," +
            "\"settings_version\":\"1\",\"aliases_version\":\"1\",\"mappings\":" +
            "{\"_doc\":{\"_meta\":{\"schema_version\":1},\"properties\":{\"name\":{\"type\":\"keyword\"}}}}}}"
        val newMapping = "{\"_meta\":{\"schema_version\":1},\"properties\":{\"name\":{\"type\":\"keyword\"}}}"
        val xContentParser = parser(indexContent)
        val index: IndexMetadata = IndexMetadata.fromXContent(xContentParser)

        val shouldUpdateIndex = IndexUtils.shouldUpdateIndex(index, newMapping)
        assertFalse(shouldUpdateIndex)
    }

    fun `test get oldest creation date`() {
        val metadata = Metadata.builder()
        mapOf("index-1" to 300L, "index-2" to 100L, "index-3" to 200L).forEach { (name, creationDate) ->
            metadata.put(
                IndexMetadata.builder(name)
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .creationDate(creationDate)
                    .build(),
                false
            )
        }
        val clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build()

        assertEquals(100L, IndexUtils.getOldestCreationDate(listOf("index-1", "index-2", "index-3"), clusterState))
        assertEquals(200L, IndexUtils.getOldestCreationDate(listOf("index-1", "index-3"), clusterState))
        assertNull(IndexUtils.getOldestCreationDate(listOf(), clusterState))
    }
}

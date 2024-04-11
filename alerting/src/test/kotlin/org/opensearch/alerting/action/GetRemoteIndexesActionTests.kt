/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.test.OpenSearchTestCase

class GetRemoteIndexesActionTests : OpenSearchTestCase() {
    private val validPatterns = listOf(
        "local-index-name",
        "localindexname",
        "local-index-*-pattern-*",
        "*local-index-*-pattern-*",
        "cluster-name:remote-index-name",
        "cluster-name:remoteindexname",
        "cluster-name:remote-index-*-pattern-*",
        "cluster-name:*remote-index-*-pattern-*",
        "cluster-*pattern-*:remote-index-name",
        "cluster-*pattern-*:remoteindexname",
        "cluster-*pattern-*:remote-index-*-pattern-*",
        "cluster-*pattern-*:*remote-index-*-pattern-*",
        "*cluster-*pattern-*:remote-index-*-pattern-*",
        "cluster-*:pattern-*:remote-index-name",
        "cluster-*:pattern-*:remoteindexname",
        "cluster-*:pattern-*:remote-index-*-pattern-*",
        "*cluster-*:pattern-*:remote-index-*-pattern-*",
    )

    private val invalidPatterns = listOf(
        // `<cluster-pattern>` character length less than 1 should return FALSE
        ":remote-index-name",

        // `<cluster-pattern>` character length greater than 63 should return FALSE
        "${randomAlphaOfLength(256)}:remote-index-name",

        // Invalid characters should return FALSE
        "local-index#-name",
        "cluster-name:remote-#index-name",
        "cluster-#name:remote-index-name",
        "cluster-#name:remote-#index-name",

        // More than 1 `:` character in `<cluster-pattern>` should return FALSE
        "bad:cluster:name:remote-index-name",
    )

    fun `test get remote indexes action name`() {
        assertNotNull(GetRemoteIndexesAction.INSTANCE.name())
        assertEquals(GetRemoteIndexesAction.INSTANCE.name(), GetRemoteIndexesAction.NAME)
    }

    fun `test GetRemoteIndexesRequest isValid with empty array`() {
        val request = GetRemoteIndexesRequest(
            indexes = emptyList(),
            includeMappings = false
        )
        assertFalse(request.isValid())
    }

    fun `test GetRemoteIndexesRequest isValid with one valid entry`() {
        validPatterns.forEach {
            val request = GetRemoteIndexesRequest(
                indexes = listOf(it),
                includeMappings = false
            )
            assertTrue("Expected pattern '$it' to be valid.", request.isValid())
        }
    }

    fun `test GetRemoteIndexesRequest isValid with multiple valid entries`() {
        val request = GetRemoteIndexesRequest(
            indexes = validPatterns,
            includeMappings = false
        )
        assertTrue(request.isValid())
    }

    fun `test GetRemoteIndexesRequest isValid with one invalid entry`() {
        invalidPatterns.forEach {
            val request = GetRemoteIndexesRequest(
                indexes = listOf(it),
                includeMappings = false
            )
            assertFalse("Expected pattern '$it' to be invalid.", request.isValid())
        }
    }

    fun `test GetRemoteIndexesRequest isValid with multiple invalid entries`() {
        val request = GetRemoteIndexesRequest(
            indexes = invalidPatterns,
            includeMappings = false
        )
        assertFalse(request.isValid())
    }

    fun `test GetRemoteIndexesRequest isValid with valid and invalid entries`() {
        val request = GetRemoteIndexesRequest(
            indexes = validPatterns + invalidPatterns,
            includeMappings = false
        )
        assertFalse(request.isValid())
    }
}

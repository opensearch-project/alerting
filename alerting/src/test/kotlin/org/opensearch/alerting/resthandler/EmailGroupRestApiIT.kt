/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.apache.http.entity.ContentType
import org.apache.http.nio.entity.NStringEntity
import org.opensearch.alerting.AlertingPlugin.Companion.EMAIL_GROUP_BASE_URI
import org.opensearch.alerting.AlertingRestTestCase
import org.opensearch.alerting.makeRequest
import org.opensearch.alerting.model.destination.email.EmailEntry
import org.opensearch.alerting.model.destination.email.EmailGroup
import org.opensearch.alerting.randomEmailGroup
import org.opensearch.client.ResponseException
import org.opensearch.common.xcontent.XContentType
import org.opensearch.index.query.QueryBuilders
import org.opensearch.rest.RestStatus
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.test.junit.annotations.TestLogging

@TestLogging("level:DEBUG", reason = "Debug for tests.")
@Suppress("UNCHECKED_CAST")
class EmailGroupRestApiIT : AlertingRestTestCase() {

    fun `test creating an email group`() {
        val emailGroup = EmailGroup(
            name = "test",
            emails = listOf(EmailEntry("test@email.com"))
        )
        val createdEmailGroup = createEmailGroup(emailGroup = emailGroup)
        assertEquals("Incorrect email group name", createdEmailGroup.name, "test")
        assertEquals("Incorrect email group email entry", createdEmailGroup.emails[0].email, "test@email.com")
    }

    fun `test creating an email group with PUT fails`() {
        try {
            val emailGroup = randomEmailGroup()
            client().makeRequest("PUT", EMAIL_GROUP_BASE_URI, emptyMap(), emailGroup.toHttpEntity())
            fail("Expected 405 Method Not Allowed respone")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.METHOD_NOT_ALLOWED, e.response.restStatus())
        }
    }

    fun `test creating an email group when email destination is disallowed fails`() {
        try {
            removeEmailFromAllowList()
            createRandomEmailGroup()
            fail("Expected 403 Method FORBIDDEN response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.FORBIDDEN, e.response.restStatus())
        }
    }

    fun `test getting an email group`() {
        val emailGroup = createRandomEmailGroup()
        val storedEmailGroup = getEmailGroup(emailGroup.id)
        assertEquals("Indexed and retrieved email group differ", emailGroup, storedEmailGroup)
    }

    fun `test getting an email group that doesn't exist`() {
        try {
            getEmailGroup(randomAlphaOfLength(20))
            fail("Expected response exception")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.NOT_FOUND, e.response.restStatus())
        }
    }

    fun `test getting an email group when email destination is disallowed fails`() {
        val emailGroup = createRandomEmailGroup()

        try {
            removeEmailFromAllowList()
            getEmailGroup(emailGroup.id)
            fail("Expected 403 Method FORBIDDEN response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.FORBIDDEN, e.response.restStatus())
        }
    }

    fun `test checking if an email group exists`() {
        val emailGroup = createRandomEmailGroup()

        val headResponse = client().makeRequest("HEAD", "$EMAIL_GROUP_BASE_URI/${emailGroup.id}")
        assertEquals("Unable to HEAD email group", RestStatus.OK, headResponse.restStatus())
        assertNull("Response contains unexpected body", headResponse.entity)
    }

    fun `test checking if a non-existent email group exists`() {
        val headResponse = client().makeRequest("HEAD", "$EMAIL_GROUP_BASE_URI/foobar")
        assertEquals("Unexpected status", RestStatus.NOT_FOUND, headResponse.restStatus())
    }

    fun `test querying an email group that exists`() {
        val emailGroup = createRandomEmailGroup()

        val search = SearchSourceBuilder().query(QueryBuilders.termQuery("_id", emailGroup.id)).toString()
        val searchResponse = client().makeRequest(
            "GET",
            "$EMAIL_GROUP_BASE_URI/_search",
            emptyMap(),
            NStringEntity(search, ContentType.APPLICATION_JSON)
        )
        assertEquals("Search email group failed", RestStatus.OK, searchResponse.restStatus())
        val xcp = createParser(XContentType.JSON.xContent(), searchResponse.entity.content)
        val hits = xcp.map()["hits"]!! as Map<String, Map<String, Any>>
        val numberOfDocsFound = hits["total"]?.get("value")
        assertEquals("Email group not found during search", 1, numberOfDocsFound)
    }

    fun `test querying an email group that exists with POST`() {
        val emailGroup = createRandomEmailGroup()

        val search = SearchSourceBuilder().query(QueryBuilders.termQuery("_id", emailGroup.id)).toString()
        val searchResponse = client().makeRequest(
            "POST",
            "$EMAIL_GROUP_BASE_URI/_search",
            emptyMap(),
            NStringEntity(search, ContentType.APPLICATION_JSON)
        )
        assertEquals("Search email group failed", RestStatus.OK, searchResponse.restStatus())
        val xcp = createParser(XContentType.JSON.xContent(), searchResponse.entity.content)
        val hits = xcp.map()["hits"]!! as Map<String, Map<String, Any>>
        val numberOfDocsFound = hits["total"]?.get("value")
        assertEquals("Email group not found during search", 1, numberOfDocsFound)
    }

    fun `test querying an email group that doesn't exist`() {
        // Create a random email group to create the ScheduledJob index. Otherwise the test will fail with a 404 index not found error.
        createRandomEmailGroup()
        val search = SearchSourceBuilder()
            .query(
                QueryBuilders.termQuery(
                    OpenSearchTestCase.randomAlphaOfLength(5),
                    OpenSearchTestCase.randomAlphaOfLength(5)
                )
            ).toString()

        val searchResponse = client().makeRequest(
            "GET",
            "$EMAIL_GROUP_BASE_URI/_search",
            emptyMap(),
            NStringEntity(search, ContentType.APPLICATION_JSON)
        )
        assertEquals("Search email group failed", RestStatus.OK, searchResponse.restStatus())
        val xcp = createParser(XContentType.JSON.xContent(), searchResponse.entity.content)
        val hits = xcp.map()["hits"]!! as Map<String, Map<String, Any>>
        val numberOfDocsFound = hits["total"]?.get("value")
        assertEquals("Email group found during search when no document was present", 0, numberOfDocsFound)
    }

    fun `test querying an email group when email destination is disallowed fails`() {
        val emailGroup = createRandomEmailGroup()

        try {
            removeEmailFromAllowList()
            val search = SearchSourceBuilder().query(QueryBuilders.termQuery("_id", emailGroup.id)).toString()
            client().makeRequest(
                "GET",
                "$EMAIL_GROUP_BASE_URI/_search",
                emptyMap(),
                NStringEntity(search, ContentType.APPLICATION_JSON)
            )
            fail("Expected 403 Method FORBIDDEN response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.FORBIDDEN, e.response.restStatus())
        }
    }
}

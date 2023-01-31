/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.apache.hc.core5.http.ContentType
import org.apache.hc.core5.http.io.entity.StringEntity
import org.opensearch.alerting.AlertingPlugin.Companion.EMAIL_ACCOUNT_BASE_URI
import org.opensearch.alerting.AlertingRestTestCase
import org.opensearch.alerting.makeRequest
import org.opensearch.alerting.model.destination.email.EmailAccount
import org.opensearch.alerting.randomEmailAccount
import org.opensearch.client.ResponseException
import org.opensearch.common.xcontent.XContentType
import org.opensearch.index.query.QueryBuilders
import org.opensearch.rest.RestStatus
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.test.junit.annotations.TestLogging

@TestLogging("level:DEBUG", reason = "Debug for tests.")
@Suppress("UNCHECKED_CAST")
class EmailAccountRestApiIT : AlertingRestTestCase() {

    fun `test creating an email account`() {
        val emailAccount = EmailAccount(
            name = "test",
            email = "test@email.com",
            host = "smtp.com",
            port = 25,
            method = EmailAccount.MethodType.NONE,
            username = null,
            password = null
        )
        val createdEmailAccount = createEmailAccount(emailAccount = emailAccount)
        assertEquals("Incorrect email account name", createdEmailAccount.name, "test")
        assertEquals("Incorrect email account email", createdEmailAccount.email, "test@email.com")
        assertEquals("Incorrect email account host", createdEmailAccount.host, "smtp.com")
        assertEquals("Incorrect email account port", createdEmailAccount.port, 25)
        assertEquals("Incorrect email account method", createdEmailAccount.method, EmailAccount.MethodType.NONE)
    }

    fun `test creating an email account with PUT fails`() {
        try {
            val emailAccount = randomEmailAccount()
            client().makeRequest("PUT", EMAIL_ACCOUNT_BASE_URI, emptyMap(), emailAccount.toHttpEntity())
            fail("Expected 405 Method Not Allowed response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.METHOD_NOT_ALLOWED, e.response.restStatus())
        }
    }

    fun `test creating an email account when email destination is disallowed fails`() {
        try {
            removeEmailFromAllowList()
            createRandomEmailAccount()
            fail("Expected 403 Method FORBIDDEN response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.FORBIDDEN, e.response.restStatus())
        }
    }

    fun `test getting an email account`() {
        val emailAccount = createRandomEmailAccount()
        val storedEmailAccount = getEmailAccount(emailAccount.id)
        assertEquals("Indexed and retrieved email account differ", emailAccount, storedEmailAccount)
    }

    fun `test getting an email account that doesn't exist`() {
        try {
            getEmailAccount(randomAlphaOfLength(20))
            fail("Expected response exception")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.NOT_FOUND, e.response.restStatus())
        }
    }

    fun `test getting an email account when email destination is disallowed fails`() {
        val emailAccount = createRandomEmailAccount()

        try {
            removeEmailFromAllowList()
            getEmailAccount(emailAccount.id)
            fail("Expected 403 Method FORBIDDEN response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.FORBIDDEN, e.response.restStatus())
        }
    }

    fun `test checking if an email account exists`() {
        val emailAccount = createRandomEmailAccount()

        val headResponse = client().makeRequest("HEAD", "$EMAIL_ACCOUNT_BASE_URI/${emailAccount.id}")
        assertEquals("Unable to HEAD email account", RestStatus.OK, headResponse.restStatus())
        assertNull("Response contains unexpected body", headResponse.entity)
    }

    fun `test checking if a non-existent email account exists`() {
        val headResponse = client().makeRequest("HEAD", "$EMAIL_ACCOUNT_BASE_URI/foobar")
        assertEquals("Unexpected status", RestStatus.NOT_FOUND, headResponse.restStatus())
    }

    fun `test querying an email account that exists`() {
        val emailAccount = createRandomEmailAccount()

        val search = SearchSourceBuilder().query(QueryBuilders.termQuery("_id", emailAccount.id)).toString()
        val searchResponse = client().makeRequest(
            "GET",
            "$EMAIL_ACCOUNT_BASE_URI/_search",
            emptyMap(),
            StringEntity(search, ContentType.APPLICATION_JSON)
        )
        assertEquals("Search email account failed", RestStatus.OK, searchResponse.restStatus())
        val xcp = createParser(XContentType.JSON.xContent(), searchResponse.entity.content)
        val hits = xcp.map()["hits"]!! as Map<String, Map<String, Any>>
        val numberOfDocsFound = hits["total"]?.get("value")
        assertEquals("Email account not found during search", 1, numberOfDocsFound)
    }

    fun `test querying an email account that exists with POST`() {
        val emailAccount = createRandomEmailAccount()

        val search = SearchSourceBuilder().query(QueryBuilders.termQuery("_id", emailAccount.id)).toString()
        val searchResponse = client().makeRequest(
            "POST",
            "$EMAIL_ACCOUNT_BASE_URI/_search",
            emptyMap(),
            StringEntity(search, ContentType.APPLICATION_JSON)
        )
        assertEquals("Search email account failed", RestStatus.OK, searchResponse.restStatus())
        val xcp = createParser(XContentType.JSON.xContent(), searchResponse.entity.content)
        val hits = xcp.map()["hits"]!! as Map<String, Map<String, Any>>
        val numberOfDocsFound = hits["total"]?.get("value")
        assertEquals("Email account not found during search", 1, numberOfDocsFound)
    }

    fun `test querying an email account that doesn't exist`() {
        // Create a random email account to create the ScheduledJob index. Otherwise the test will fail with a 404 index not found error.
        createRandomEmailAccount()
        val search = SearchSourceBuilder()
            .query(
                QueryBuilders.termQuery(
                    OpenSearchTestCase.randomAlphaOfLength(5),
                    OpenSearchTestCase.randomAlphaOfLength(5)
                )
            ).toString()

        val searchResponse = client().makeRequest(
            "GET",
            "$EMAIL_ACCOUNT_BASE_URI/_search",
            emptyMap(),
            StringEntity(search, ContentType.APPLICATION_JSON)
        )
        assertEquals("Search email account failed", RestStatus.OK, searchResponse.restStatus())
        val xcp = createParser(XContentType.JSON.xContent(), searchResponse.entity.content)
        val hits = xcp.map()["hits"]!! as Map<String, Map<String, Any>>
        val numberOfDocsFound = hits["total"]?.get("value")
        assertEquals("Email account found during search when no document was present", 0, numberOfDocsFound)
    }

    fun `test querying an email account when email destination is disallowed fails`() {
        val emailAccount = createRandomEmailAccount()

        try {
            removeEmailFromAllowList()
            val search = SearchSourceBuilder().query(QueryBuilders.termQuery("_id", emailAccount.id)).toString()
            client().makeRequest(
                "GET",
                "$EMAIL_ACCOUNT_BASE_URI/_search",
                emptyMap(),
                StringEntity(search, ContentType.APPLICATION_JSON)
            )
            fail("Expected 403 Method FORBIDDEN response")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.FORBIDDEN, e.response.restStatus())
        }
    }
}

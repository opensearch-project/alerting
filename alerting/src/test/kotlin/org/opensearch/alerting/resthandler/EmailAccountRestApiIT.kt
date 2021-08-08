/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.alerting.resthandler

import org.opensearch.alerting.AlertingPlugin.Companion.EMAIL_ACCOUNT_BASE_URI
import org.opensearch.alerting.AlertingRestTestCase
import org.opensearch.alerting.makeRequest
import org.opensearch.alerting.model.destination.email.EmailAccount
import org.opensearch.alerting.randomEmailAccount
import org.opensearch.client.ResponseException
import org.opensearch.rest.RestStatus
import org.opensearch.test.junit.annotations.TestLogging

@TestLogging("level:DEBUG", reason = "Debug for tests.")
@Suppress("UNCHECKED_CAST")
class EmailAccountRestApiIT : AlertingRestTestCase() {

    fun `test creating an email account`() {
        if (isNotificationPluginInstalled()) {
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
    }

    fun `test creating an email account with PUT fails`() {
        if (isNotificationPluginInstalled()) {
            try {
                val emailAccount = randomEmailAccount()
                client().makeRequest("PUT", EMAIL_ACCOUNT_BASE_URI, emptyMap(), emailAccount.toHttpEntity())
                fail("Expected 405 Method Not Allowed response")
            } catch (e: ResponseException) {
                assertEquals("Unexpected status", RestStatus.METHOD_NOT_ALLOWED, e.response.restStatus())
            }
        }
    }

    fun `test creating an email account with an existing name fails`() {
        if (isNotificationPluginInstalled()) {
            val emailAccount = createRandomEmailAccount()

            try {
                val emailAccountWithExistingName = randomEmailAccount(name = emailAccount.name)
                client().makeRequest("POST", EMAIL_ACCOUNT_BASE_URI, emptyMap(), emailAccountWithExistingName.toHttpEntity())
            } catch (e: ResponseException) {
                assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
            }
        }
    }

    fun `test creating an email account when email destination is disallowed fails`() {
        if (isNotificationPluginInstalled()) {
            try {
                removeEmailFromAllowList()
                createRandomEmailAccount()
                fail("Expected 403 Method FORBIDDEN response")
            } catch (e: ResponseException) {
                assertEquals("Unexpected status", RestStatus.FORBIDDEN, e.response.restStatus())
            }
        }
    }

    fun `test updating an email account`() {
        if (isNotificationPluginInstalled()) {
            val emailAccount = createEmailAccount()
            val updatedEmailAccount = updateEmailAccount(
                emailAccount.copy(
                    name = "updatedName",
                    port = 465,
                    method = EmailAccount.MethodType.SSL
                )
            )
            assertEquals("Incorrect email account name after update", updatedEmailAccount.name, "updatedName")
            assertEquals("Incorrect email account port after update", updatedEmailAccount.port, 465)
            assertEquals("Incorrect email account method after update", updatedEmailAccount.method, EmailAccount.MethodType.SSL)
        }
    }

    fun `test updating an email account to an existing name fails`() {
        if (isNotificationPluginInstalled()) {
            val emailAccount1 = createRandomEmailAccount()
            val emailAccount2 = createRandomEmailAccount()

            try {
                val updatedEmailAccount = emailAccount1.copy(name = emailAccount2.name)
                client().makeRequest(
                    "PUT",
                    "$EMAIL_ACCOUNT_BASE_URI/${emailAccount1.id}",
                    emptyMap(),
                    updatedEmailAccount.toHttpEntity()
                )
            } catch (e: ResponseException) {
                assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
            }
        }
    }

    fun `test getting an email account`() {
        if (isNotificationPluginInstalled()) {
            val emailAccount = createRandomEmailAccount()
            val storedEmailAccount = getEmailAccount(emailAccount.id)
            assertEquals("Indexed and retrieved email account differ", emailAccount, storedEmailAccount)
        }
    }

    fun `test getting an email account that doesn't exist`() {
        if (isNotificationPluginInstalled()) {
            try {
                getEmailAccount(randomAlphaOfLength(20))
                fail("Expected response exception")
            } catch (e: ResponseException) {
                assertEquals("Unexpected status", RestStatus.NOT_FOUND, e.response.restStatus())
            }
        }
    }

    fun `test getting an email account when email destination is disallowed fails`() {
        if (isNotificationPluginInstalled()) {
            val emailAccount = createRandomEmailAccount()

            try {
                removeEmailFromAllowList()
                getEmailAccount(emailAccount.id)
                fail("Expected 403 Method FORBIDDEN response")
            } catch (e: ResponseException) {
                assertEquals("Unexpected status", RestStatus.FORBIDDEN, e.response.restStatus())
            }
        }
    }

    fun `test checking if an email account exists`() {
        if (isNotificationPluginInstalled()) {
            val emailAccount = createRandomEmailAccount()

            val headResponse = client().makeRequest("HEAD", "$EMAIL_ACCOUNT_BASE_URI/${emailAccount.id}")
            assertEquals("Unable to HEAD email account", RestStatus.OK, headResponse.restStatus())
            assertNull("Response contains unexpected body", headResponse.entity)
        }
    }

    fun `test checking if a non-existent email account exists`() {
        if (isNotificationPluginInstalled()) {
            val headResponse = client().makeRequest("HEAD", "$EMAIL_ACCOUNT_BASE_URI/foobar")
            assertEquals("Unexpected status", RestStatus.NOT_FOUND, headResponse.restStatus())
        }
    }

    fun `test deleting an email account`() {
        if (isNotificationPluginInstalled()) {
            val emailAccount = createRandomEmailAccount()

            val deleteResponse = client().makeRequest("DELETE", "$EMAIL_ACCOUNT_BASE_URI/${emailAccount.id}")
            assertEquals("Delete failed", RestStatus.OK, deleteResponse.restStatus())

            val headResponse = client().makeRequest("HEAD", "$EMAIL_ACCOUNT_BASE_URI/${emailAccount.id}")
            assertEquals("Deleted email account still exists", RestStatus.NOT_FOUND, headResponse.restStatus())
        }
    }

    fun `test deleting an email account that doesn't exist`() {
        if (isNotificationPluginInstalled()) {
            try {
                client().makeRequest("DELETE", "$EMAIL_ACCOUNT_BASE_URI/foobar")
                fail("Expected 404 response exception")
            } catch (e: ResponseException) {
                assertEquals(RestStatus.NOT_FOUND, e.response.restStatus())
            }
        }
    }

    fun `test deleting an email account when email destination is disallowed fails`() {
        if (isNotificationPluginInstalled()) {
            val emailAccount = createRandomEmailAccount()

            try {
                removeEmailFromAllowList()
                client().makeRequest("DELETE", "$EMAIL_ACCOUNT_BASE_URI/${emailAccount.id}")
                fail("Expected 403 Method FORBIDDEN response")
            } catch (e: ResponseException) {
                assertEquals("Unexpected status", RestStatus.FORBIDDEN, e.response.restStatus())
            }
        }
    }

    /**
     * TODO: Make sure these tests are working after this issue, https://github.com/opensearch-project/notifications/issues/255, is resolved
     * and the SearchEmailAccount API is integrated with the notification plugin.
     */
//    fun `test querying an email account that exists`() {
//        if (isNotificationPluginInstalled()) {
//            val emailAccount = createRandomEmailAccount()
//
//            val search = SearchSourceBuilder().query(QueryBuilders.termQuery("_id", emailAccount.id)).toString()
//            val searchResponse = client().makeRequest(
//                "GET",
//                "$EMAIL_ACCOUNT_BASE_URI/_search",
//                emptyMap(),
//                NStringEntity(search, ContentType.APPLICATION_JSON)
//            )
//            assertEquals("Search email account failed", RestStatus.OK, searchResponse.restStatus())
//            val xcp = createParser(XContentType.JSON.xContent(), searchResponse.entity.content)
//            val hits = xcp.map()["hits"]!! as Map<String, Map<String, Any>>
//            val numberOfDocsFound = hits["total"]?.get("value")
//            assertEquals("Email account not found during search", 1, numberOfDocsFound)
//        }
//    }
//
//    fun `test querying an email account that exists with POST`() {
//        if (isNotificationPluginInstalled()) {
//            val emailAccount = createRandomEmailAccount()
//
//            val search = SearchSourceBuilder().query(QueryBuilders.termQuery("_id", emailAccount.id)).toString()
//            val searchResponse = client().makeRequest(
//                "POST",
//                "$EMAIL_ACCOUNT_BASE_URI/_search",
//                emptyMap(),
//                NStringEntity(search, ContentType.APPLICATION_JSON)
//            )
//            assertEquals("Search email account failed", RestStatus.OK, searchResponse.restStatus())
//            val xcp = createParser(XContentType.JSON.xContent(), searchResponse.entity.content)
//            val hits = xcp.map()["hits"]!! as Map<String, Map<String, Any>>
//            val numberOfDocsFound = hits["total"]?.get("value")
//            assertEquals("Email account not found during search", 1, numberOfDocsFound)
//        }
//    }
//
//    fun `test querying an email account that doesn't exist`() {
//        if (isNotificationPluginInstalled()) {
//            // Create a random email account to create the ScheduledJob index. Otherwise the test will fail with a 404 index not found error.
//            createRandomEmailAccount()
//            val search = SearchSourceBuilder()
//                .query(
//                    QueryBuilders.termQuery(
//                        OpenSearchTestCase.randomAlphaOfLength(5),
//                        OpenSearchTestCase.randomAlphaOfLength(5)
//                    )
//                ).toString()
//
//            val searchResponse = client().makeRequest(
//                "GET",
//                "$EMAIL_ACCOUNT_BASE_URI/_search",
//                emptyMap(),
//                NStringEntity(search, ContentType.APPLICATION_JSON)
//            )
//            assertEquals("Search email account failed", RestStatus.OK, searchResponse.restStatus())
//            val xcp = createParser(XContentType.JSON.xContent(), searchResponse.entity.content)
//            val hits = xcp.map()["hits"]!! as Map<String, Map<String, Any>>
//            val numberOfDocsFound = hits["total"]?.get("value")
//            assertEquals("Email account found during search when no document was present", 0, numberOfDocsFound)
//        }
//    }
//
//    fun `test querying an email account when email destination is disallowed fails`() {
//        if (isNotificationPluginInstalled()) {
//            val emailAccount = createRandomEmailAccount()
//
//            try {
//                removeEmailFromAllowList()
//                val search = SearchSourceBuilder().query(QueryBuilders.termQuery("_id", emailAccount.id)).toString()
//                client().makeRequest(
//                    "GET",
//                    "$EMAIL_ACCOUNT_BASE_URI/_search",
//                    emptyMap(),
//                    NStringEntity(search, ContentType.APPLICATION_JSON)
//                )
//                fail("Expected 403 Method FORBIDDEN response")
//            } catch (e: ResponseException) {
//                assertEquals("Unexpected status", RestStatus.FORBIDDEN, e.response.restStatus())
//            }
//        }
//    }
}

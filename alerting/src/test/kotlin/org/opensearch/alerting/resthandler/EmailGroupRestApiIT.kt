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

import org.opensearch.alerting.AlertingPlugin.Companion.EMAIL_GROUP_BASE_URI
import org.opensearch.alerting.AlertingRestTestCase
import org.opensearch.alerting.makeRequest
import org.opensearch.alerting.model.destination.email.EmailEntry
import org.opensearch.alerting.model.destination.email.EmailGroup
import org.opensearch.alerting.randomEmailGroup
import org.opensearch.client.ResponseException
import org.opensearch.rest.RestStatus
import org.opensearch.test.junit.annotations.TestLogging

@TestLogging("level:DEBUG", reason = "Debug for tests.")
@Suppress("UNCHECKED_CAST")
class EmailGroupRestApiIT : AlertingRestTestCase() {

    fun `test creating an email group`() {
        if (isNotificationPluginInstalled()) {
            val emailGroup = EmailGroup(
                name = "test",
                emails = listOf(EmailEntry("test@email.com"))
            )
            val createdEmailGroup = createEmailGroup(emailGroup = emailGroup)
            assertEquals("Incorrect email group name", createdEmailGroup.name, "test")
            assertEquals("Incorrect email group email entry", createdEmailGroup.emails[0].email, "test@email.com")
        }
    }

    fun `test creating an email group with PUT fails`() {
        if (isNotificationPluginInstalled()) {
            try {
                val emailGroup = randomEmailGroup()
                client().makeRequest("PUT", EMAIL_GROUP_BASE_URI, emptyMap(), emailGroup.toHttpEntity())
                fail("Expected 405 Method Not Allowed respone")
            } catch (e: ResponseException) {
                assertEquals("Unexpected status", RestStatus.METHOD_NOT_ALLOWED, e.response.restStatus())
            }
        }
    }

    fun `test creating an email group when email destination is disallowed fails`() {
        if (isNotificationPluginInstalled()) {
            try {
                removeEmailFromAllowList()
                createRandomEmailGroup()
                fail("Expected 403 Method FORBIDDEN response")
            } catch (e: ResponseException) {
                assertEquals("Unexpected status", RestStatus.FORBIDDEN, e.response.restStatus())
            }
        }
    }

    fun `test updating an email group`() {
        if (isNotificationPluginInstalled()) {
            val emailGroup = createEmailGroup()
            val updatedEmailGroup = updateEmailGroup(
                emailGroup.copy(
                    name = "updatedName",
                    emails = listOf(EmailEntry("test@email.com"))
                )
            )
            assertEquals("Incorrect email group name after update", updatedEmailGroup.name, "updatedName")
            assertEquals("Incorrect email group email entry after update", updatedEmailGroup.emails[0].email, "test@email.com")
        }
    }

    fun `test getting an email group`() {
        if (isNotificationPluginInstalled()) {
            val emailGroup = createRandomEmailGroup()
            val storedEmailGroup = getEmailGroup(emailGroup.id)
            assertEquals("Indexed and retrieved email group differ", emailGroup, storedEmailGroup)
        }
    }

    fun `test getting an email group that doesn't exist`() {
        if (isNotificationPluginInstalled()) {
            try {
                getEmailGroup(randomAlphaOfLength(20))
                fail("Expected response exception")
            } catch (e: ResponseException) {
                assertEquals("Unexpected status", RestStatus.NOT_FOUND, e.response.restStatus())
            }
        }
    }

    fun `test getting an email group when email destination is disallowed fails`() {
        if (isNotificationPluginInstalled()) {
            val emailGroup = createRandomEmailGroup()

            try {
                removeEmailFromAllowList()
                getEmailGroup(emailGroup.id)
                fail("Expected 403 Method FORBIDDEN response")
            } catch (e: ResponseException) {
                assertEquals("Unexpected status", RestStatus.FORBIDDEN, e.response.restStatus())
            }
        }
    }

    fun `test checking if an email group exists`() {
        if (isNotificationPluginInstalled()) {
            val emailGroup = createRandomEmailGroup()

            val headResponse = client().makeRequest("HEAD", "$EMAIL_GROUP_BASE_URI/${emailGroup.id}")
            assertEquals("Unable to HEAD email group", RestStatus.OK, headResponse.restStatus())
            assertNull("Response contains unexpected body", headResponse.entity)
        }
    }

    fun `test checking if a non-existent email group exists`() {
        if (isNotificationPluginInstalled()) {
            val headResponse = client().makeRequest("HEAD", "$EMAIL_GROUP_BASE_URI/foobar")
            assertEquals("Unexpected status", RestStatus.NOT_FOUND, headResponse.restStatus())
        }
    }

    fun `test deleting an email group`() {
        if (isNotificationPluginInstalled()) {
            val emailGroup = createRandomEmailGroup()

            val deleteResponse = client().makeRequest("DELETE", "$EMAIL_GROUP_BASE_URI/${emailGroup.id}")
            assertEquals("Delete failed", RestStatus.OK, deleteResponse.restStatus())

            val headResponse = client().makeRequest("HEAD", "$EMAIL_GROUP_BASE_URI/${emailGroup.id}")
            assertEquals("Deleted email group still exists", RestStatus.NOT_FOUND, headResponse.restStatus())
        }
    }

    fun `test deleting an email group that doesn't exist`() {
        if (isNotificationPluginInstalled()) {
            try {
                client().makeRequest("DELETE", "$EMAIL_GROUP_BASE_URI/foobar")
                fail("Expected 404 response exception")
            } catch (e: ResponseException) {
                assertEquals(RestStatus.NOT_FOUND, e.response.restStatus())
            }
        }
    }

    fun `test deleting an email group when email destination is disallowed fails`() {
        if (isNotificationPluginInstalled()) {
            val emailGroup = createRandomEmailGroup()

            try {
                removeEmailFromAllowList()
                client().makeRequest("DELETE", "$EMAIL_GROUP_BASE_URI/${emailGroup.id}")
                fail("Expected 403 Method FORBIDDEN response")
            } catch (e: ResponseException) {
                assertEquals("Unexpected status", RestStatus.FORBIDDEN, e.response.restStatus())
            }
        }
    }

    /**
     * TODO: Make sure these tests are working after this issue, https://github.com/opensearch-project/notifications/issues/255, is resolved
     * and the SearchEmailGroup API is integrated with the notification plugin.
     */
//    fun `test querying an email group that exists`() {
//        if (isNotificationPluginInstalled()) {
//            val emailGroup = createRandomEmailGroup()
//
//            val search = SearchSourceBuilder().query(QueryBuilders.termQuery("_id", emailGroup.id)).toString()
//            val searchResponse = client().makeRequest(
//                "GET",
//                "$EMAIL_GROUP_BASE_URI/_search",
//                emptyMap(),
//                NStringEntity(search, ContentType.APPLICATION_JSON)
//            )
//            assertEquals("Search email group failed", RestStatus.OK, searchResponse.restStatus())
//            val xcp = createParser(XContentType.JSON.xContent(), searchResponse.entity.content)
//            val hits = xcp.map()["hits"]!! as Map<String, Map<String, Any>>
//            val numberOfDocsFound = hits["total"]?.get("value")
//            assertEquals("Email group not found during search", 1, numberOfDocsFound)
//        }
//    }
//
//    fun `test querying an email group that exists with POST`() {
//        if (isNotificationPluginInstalled()) {
//            val emailGroup = createRandomEmailGroup()
//
//            val search = SearchSourceBuilder().query(QueryBuilders.termQuery("_id", emailGroup.id)).toString()
//            val searchResponse = client().makeRequest(
//                "POST",
//                "$EMAIL_GROUP_BASE_URI/_search",
//                emptyMap(),
//                NStringEntity(search, ContentType.APPLICATION_JSON)
//            )
//            assertEquals("Search email group failed", RestStatus.OK, searchResponse.restStatus())
//            val xcp = createParser(XContentType.JSON.xContent(), searchResponse.entity.content)
//            val hits = xcp.map()["hits"]!! as Map<String, Map<String, Any>>
//            val numberOfDocsFound = hits["total"]?.get("value")
//            assertEquals("Email group not found during search", 1, numberOfDocsFound)
//        }
//    }
//
//    fun `test querying an email group that doesn't exist`() {
//        if (isNotificationPluginInstalled()) {
//            // Create a random monitor to create the ScheduledJob index. Otherwise the test will fail with a 404 index not found error.
//            createRandomMonitor()
//            createRandomEmailGroup()
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
//                "$EMAIL_GROUP_BASE_URI/_search",
//                emptyMap(),
//                NStringEntity(search, ContentType.APPLICATION_JSON)
//            )
//            assertEquals("Search email group failed", RestStatus.OK, searchResponse.restStatus())
//            val xcp = createParser(XContentType.JSON.xContent(), searchResponse.entity.content)
//            val hits = xcp.map()["hits"]!! as Map<String, Map<String, Any>>
//            val numberOfDocsFound = hits["total"]?.get("value")
//            assertEquals("Email group found during search when no document was present", 0, numberOfDocsFound)
//        }
//    }
//
//    fun `test querying an email group when email destination is disallowed fails`() {
//        if (isNotificationPluginInstalled()) {
//            val emailGroup = createRandomEmailGroup()
//
//            try {
//                removeEmailFromAllowList()
//                val search = SearchSourceBuilder().query(QueryBuilders.termQuery("_id", emailGroup.id)).toString()
//                client().makeRequest(
//                    "GET",
//                    "$EMAIL_GROUP_BASE_URI/_search",
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

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.opensearch.alerting.ALERTING_ACK_ALERTS_ROLE
import org.opensearch.alerting.ALERTING_FULL_ACCESS_ROLE
import org.opensearch.alerting.ALERTING_READ_ONLY_ACCESS
import org.opensearch.alerting.AlertingRestTestCase
import org.opensearch.alerting.randomAlert
import org.opensearch.alerting.settings.AlertingSettings.Companion.ALERTING_COMMENTS_ENABLED
import org.opensearch.client.ResponseException
import org.opensearch.client.RestClient
import org.opensearch.commons.alerting.model.Alert
import org.opensearch.commons.rest.SecureRestClientBuilder
import org.opensearch.core.rest.RestStatus
import org.opensearch.index.query.QueryBuilders
import org.opensearch.search.builder.SearchSourceBuilder

class SecureAlertingCommentsRestApiIT : AlertingRestTestCase() {

    companion object {
        @BeforeClass
        @JvmStatic fun setup() {
            // things to execute once and keep around for the class
            org.junit.Assume.assumeTrue(System.getProperty("security", "false")!!.toBoolean())
        }
    }

    val userA = "userA"
    val userB = "userB"
    var userAClient: RestClient? = null
    var userBClient: RestClient? = null

    @Before
    fun create() {
        if (userAClient == null) {
            createUser(userA, arrayOf())
            userAClient = SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), userA, password)
                .setSocketTimeout(60000)
                .build()
        }
        if (userBClient == null) {
            createUser(userB, arrayOf())
            userBClient = SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), userB, password)
                .setSocketTimeout(6000)
                .build()
        }
        client().updateSettings(ALERTING_COMMENTS_ENABLED.key, "true")
    }

    @After
    fun cleanup() {
        userAClient?.close()
        userBClient?.close()
        deleteUser(userA)
        deleteUser(userB)
    }

    fun `test user with alerting full access can create comment`() {
        createUserWithRoles(
            userA,
            listOf(ALERTING_FULL_ACCESS_ROLE),
            listOf(),
            false
        )
        val monitor = createRandomMonitor(refresh = true)
        val alert = createAlert(randomAlert(monitor).copy(state = Alert.State.ACTIVE))
        val alertId = alert.id
        val commentContent = "test comment"

        createAlertComment(alertId, commentContent, userAClient!!).id

        deleteRoleMapping(ALERTING_FULL_ACCESS_ROLE)
    }

    fun `test user with alerting full access can view comments`() {
        createUserWithRoles(
            userA,
            listOf(ALERTING_FULL_ACCESS_ROLE),
            listOf(),
            false
        )
        val monitor = createRandomMonitor(refresh = true)
        val alert = createAlert(randomAlert(monitor).copy(state = Alert.State.ACTIVE))
        val alertId = alert.id
        val comment1Content = "test comment 1"
        val comment2Content = "test comment 2"

        createAlertComment(alertId, comment1Content, userAClient!!).id
        createAlertComment(alertId, comment2Content, userAClient!!).id

        val search = SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
        val xcp = searchAlertComments(search, userAClient!!)

        val hits = xcp.map()["hits"]!! as Map<String, Map<String, Any>>
        val numberDocsFound = hits["total"]?.get("value")
        assertEquals("Incorrect number of comments found", 2, numberDocsFound)

        deleteRoleMapping(ALERTING_FULL_ACCESS_ROLE)
    }

    fun `test user with alerting full access can edit comment`() {
        createUserWithRoles(
            userA,
            listOf(ALERTING_FULL_ACCESS_ROLE),
            listOf(),
            false
        )
        val monitor = createRandomMonitor(refresh = true)
        val alert = createAlert(randomAlert(monitor).copy(state = Alert.State.ACTIVE))
        val alertId = alert.id
        val commentContent = "test comment"

        val commentId = createAlertComment(alertId, commentContent, userAClient!!).id

        val updatedContent = "updated comment"
        updateAlertComment(commentId, updatedContent, userAClient!!)

        deleteRoleMapping(ALERTING_FULL_ACCESS_ROLE)
    }

    fun `test user with alerting full access can delete comment`() {
        createUserWithRoles(
            userA,
            listOf(ALERTING_FULL_ACCESS_ROLE),
            listOf(),
            false
        )
        val monitor = createRandomMonitor(refresh = true)
        val alert = createAlert(randomAlert(monitor).copy(state = Alert.State.ACTIVE))
        val alertId = alert.id
        val commentContent = "test comment"

        val commentId = createAlertComment(alertId, commentContent, userAClient!!).id

        deleteAlertComment(commentId, userAClient!!)

        deleteRoleMapping(ALERTING_FULL_ACCESS_ROLE)
    }

    fun `test user with alerting ack alerts can create comment`() {
        createUserWithRoles(
            userA,
            listOf(ALERTING_ACK_ALERTS_ROLE),
            listOf(),
            false
        )
        val monitor = createRandomMonitor(refresh = true)
        val alert = createAlert(randomAlert(monitor).copy(state = Alert.State.ACTIVE))
        val alertId = alert.id
        val commentContent = "test comment"

        createAlertComment(alertId, commentContent, userAClient!!).id

        deleteRoleMapping(ALERTING_ACK_ALERTS_ROLE)
    }

    fun `test user with alerting ack alerts can view comments`() {
        createUserWithRoles(
            userA,
            listOf(ALERTING_ACK_ALERTS_ROLE),
            listOf(),
            false
        )
        val monitor = createRandomMonitor(refresh = true)
        val alert = createAlert(randomAlert(monitor).copy(state = Alert.State.ACTIVE))
        val alertId = alert.id
        val comment1Content = "test comment 1"
        val comment2Content = "test comment 2"

        createAlertComment(alertId, comment1Content, userAClient!!).id
        createAlertComment(alertId, comment2Content, userAClient!!).id

        val search = SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
        val xcp = searchAlertComments(search, userAClient!!)

        val hits = xcp.map()["hits"]!! as Map<String, Map<String, Any>>
        val numberDocsFound = hits["total"]?.get("value")
        assertEquals("Incorrect number of comments found", 2, numberDocsFound)

        deleteRoleMapping(ALERTING_ACK_ALERTS_ROLE)
    }

    fun `test user with alerting ack alerts can edit comment`() {
        createUserWithRoles(
            userA,
            listOf(ALERTING_ACK_ALERTS_ROLE),
            listOf(),
            false
        )
        val monitor = createRandomMonitor(refresh = true)
        val alert = createAlert(randomAlert(monitor).copy(state = Alert.State.ACTIVE))
        val alertId = alert.id
        val commentContent = "test comment"

        val commentId = createAlertComment(alertId, commentContent, userAClient!!).id

        val updatedContent = "updated comment"
        updateAlertComment(commentId, updatedContent, userAClient!!)

        deleteRoleMapping(ALERTING_ACK_ALERTS_ROLE)
    }

    fun `test user with alerting ack alerts can delete comment`() {
        createUserWithRoles(
            userA,
            listOf(ALERTING_ACK_ALERTS_ROLE),
            listOf(),
            false
        )
        val monitor = createRandomMonitor(refresh = true)
        val alert = createAlert(randomAlert(monitor).copy(state = Alert.State.ACTIVE))
        val alertId = alert.id
        val commentContent = "test comment"

        val commentId = createAlertComment(alertId, commentContent, userAClient!!).id

        deleteAlertComment(commentId, userAClient!!)

        deleteRoleMapping(ALERTING_ACK_ALERTS_ROLE)
    }

    fun `test user with alerting read access cannot create comment`() {
        createUserWithRoles(
            userA,
            listOf(ALERTING_READ_ONLY_ACCESS),
            listOf(),
            false
        )

        val monitor = createRandomMonitor(refresh = true)
        val alert = createAlert(randomAlert(monitor).copy(state = Alert.State.ACTIVE))
        val alertId = alert.id
        val commentContent = "test comment"

        try {
            createAlertComment(alertId, commentContent, userAClient!!)
            fail("User with read access was able to create comment")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.FORBIDDEN, e.response.restStatus())
        } finally {
            deleteRoleMapping(ALERTING_READ_ONLY_ACCESS)
        }
    }

    fun `test user with alerting read access can view comments`() {
        createUserWithRoles(
            userA,
            listOf(ALERTING_READ_ONLY_ACCESS),
            listOf(),
            false
        )
        createUserWithRoles(
            userB,
            listOf(ALERTING_FULL_ACCESS_ROLE),
            listOf(),
            false
        )
        val monitor = createRandomMonitor(refresh = true)
        val alert = createAlert(randomAlert(monitor).copy(state = Alert.State.ACTIVE))
        val alertId = alert.id
        val comment1Content = "test comment 1"
        val comment2Content = "test comment 2"

        createAlertComment(alertId, comment1Content, userBClient!!).id
        createAlertComment(alertId, comment2Content, userBClient!!).id

        val search = SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
        val xcp = searchAlertComments(search, userAClient!!)

        val hits = xcp.map()["hits"]!! as Map<String, Map<String, Any>>
        val numberDocsFound = hits["total"]?.get("value")
        assertEquals("Incorrect number of comments found", 2, numberDocsFound)

        deleteRoleMapping(ALERTING_READ_ONLY_ACCESS)
        deleteRoleMapping(ALERTING_FULL_ACCESS_ROLE)
    }

    fun `test user with no roles cannot create comment`() {
        createUserWithRoles(
            userA,
            listOf(),
            listOf(),
            false
        )

        val monitor = createRandomMonitor(refresh = true)
        val alert = createAlert(randomAlert(monitor).copy(state = Alert.State.ACTIVE))
        val alertId = alert.id
        val commentContent = "test comment"

        try {
            createAlertComment(alertId, commentContent, userAClient!!)
            fail("User with no access was able to create comment")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.FORBIDDEN, e.response.restStatus())
        }
    }

    fun `test user with no roles cannot view comments`() {
        createUserWithRoles(
            userA,
            listOf(),
            listOf(),
            false
        )
        createUserWithRoles(
            userB,
            listOf(ALERTING_FULL_ACCESS_ROLE),
            listOf(),
            false
        )
        val monitor = createRandomMonitor(refresh = true)
        val alert = createAlert(randomAlert(monitor).copy(state = Alert.State.ACTIVE))
        val alertId = alert.id
        val comment1Content = "test comment 1"
        val comment2Content = "test comment 2"

        createAlertComment(alertId, comment1Content, userBClient!!).id
        createAlertComment(alertId, comment2Content, userBClient!!).id

        val search = SearchSourceBuilder().query(QueryBuilders.matchAllQuery())

        try {
            searchAlertComments(search, userAClient!!)
            fail("User with no roles was able to view comment")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.FORBIDDEN, e.response.restStatus())
        } finally {
            deleteRoleMapping(ALERTING_FULL_ACCESS_ROLE)
        }
    }

    fun `test user cannot edit someone else's comment`() {
        createUser(userA, listOf<String>().toTypedArray())
        createUser(userB, listOf<String>().toTypedArray())
        createUserRolesMapping(ALERTING_FULL_ACCESS_ROLE, listOf(userA, userB).toTypedArray())

        val monitor = createRandomMonitor(refresh = true)
        val alert = createAlert(randomAlert(monitor).copy(state = Alert.State.ACTIVE))
        val alertId = alert.id
        val commentContent = "test comment"

        val commentId = createAlertComment(alertId, commentContent, userAClient!!).id

        try {
            updateAlertComment(commentId, commentContent, userBClient!!)
            fail("User was able to edit someone else's comment")
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.FORBIDDEN, e.response.restStatus())
        } finally {
            deleteRoleMapping(ALERTING_FULL_ACCESS_ROLE)
        }
    }

    // TODO: this will cause security ITs to fail because the getSystemIndexDescriptors() change
    // introduced will not yet be consumed by Security plugin to allow this test to pass.
    // Will uncomment this in a later PR once Security plugin has consumed the getSystemIndexDescriptors()
    // change
//    fun `test user cannot directly search comments system index`() {
//        createUserWithRoles(
//            userA,
//            listOf(ALERTING_FULL_ACCESS_ROLE),
//            listOf(),
//            false
//        )
//
//        val monitor = createRandomMonitor(refresh = true)
//        val alert = createAlert(randomAlert(monitor).copy(state = Alert.State.ACTIVE))
//        val alertId = alert.id
//        val commentContent = "test comment"
//
//        createAlertComment(alertId, commentContent, userAClient!!).id
//
//        val query = SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
//        val searchResponse = userAClient!!.makeRequest(
//            "GET",
//            ".opensearch-alerting-comments-history-*/_search",
//            StringEntity(query.toString(), APPLICATION_JSON)
//        )
//
//        val xcp = createParser(XContentType.JSON.xContent(), searchResponse.entity.content)
//        val hits = xcp.map()["hits"]!! as Map<String, Map<String, Any>>
//        val numberDocsFound = hits["total"]?.get("value")
//        assertEquals("User was able to directly inspect alerting comments system index docs", 0, numberDocsFound)
//    }
}

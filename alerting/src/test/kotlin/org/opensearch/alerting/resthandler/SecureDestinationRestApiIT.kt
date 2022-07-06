/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.apache.http.HttpHeaders
import org.apache.http.message.BasicHeader
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.opensearch.alerting.ALERTING_GET_DESTINATION_ACCESS
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.AlertingRestTestCase
import org.opensearch.alerting.TEST_HR_BACKEND_ROLE
import org.opensearch.alerting.TEST_HR_INDEX
import org.opensearch.alerting.TEST_HR_ROLE
import org.opensearch.alerting.makeRequest
import org.opensearch.alerting.model.destination.Chime
import org.opensearch.alerting.model.destination.Destination
import org.opensearch.alerting.model.destination.Slack
import org.opensearch.alerting.randomUser
import org.opensearch.alerting.util.DestinationType
import org.opensearch.client.RestClient
import org.opensearch.commons.rest.SecureRestClientBuilder
import org.opensearch.rest.RestStatus
import org.opensearch.test.junit.annotations.TestLogging
import java.time.Instant

@TestLogging("level:DEBUG", reason = "Debug for tests.")
@Suppress("UNCHECKED_CAST")
class SecureDestinationRestApiIT : AlertingRestTestCase() {

    companion object {

        @BeforeClass
        @JvmStatic fun setup() {
            // things to execute once and keep around for the class
            org.junit.Assume.assumeTrue(System.getProperty("security", "false")!!.toBoolean())
        }
    }

    val user = "userA"
    var userClient: RestClient? = null

    @Before
    fun create() {

        if (userClient == null) {
            createUser(user, user, arrayOf())
            userClient = SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), user, user).setSocketTimeout(60000).build()
        }
    }

    @After
    fun cleanup() {

        userClient?.close()
        deleteUser(user)
    }

    fun `test create destination with disable filter by`() {
        disableFilterBy()

        val chime = Chime("http://abc.com")
        val destination = Destination(
            type = DestinationType.CHIME,
            name = "test",
            user = randomUser(),
            lastUpdateTime = Instant.now(),
            chime = chime,
            slack = null,
            customWebhook = null,
            email = null
        )
        val createdDestination = createDestination(destination = destination)
        assertEquals("Incorrect destination name", createdDestination.name, "test")
        assertEquals("Incorrect destination type", createdDestination.type, DestinationType.CHIME)
    }

    fun `test get destinations with a destination type and disable filter by`() {
        disableFilterBy()
        val slack = Slack("url")
        val destination = Destination(
            type = DestinationType.SLACK,
            name = "testSlack",
            user = randomUser(),
            lastUpdateTime = Instant.now(),
            chime = null,
            slack = slack,
            customWebhook = null,
            email = null
        )

        // 1. create a destination as admin user
        createDestination(destination, true)

        val inputMap = HashMap<String, Any>()
        inputMap["missing"] = "_last"
        inputMap["destinationType"] = "slack"

        // 2. get destinations as admin user
        val adminResponse = getDestinations(client(), inputMap)
        assertEquals(1, adminResponse.size)
    }

    fun `test get destinations with a destination type and filter by`() {
        enableFilterBy()

        val slack = Slack("url")
        val destination = Destination(
            type = DestinationType.SLACK,
            name = "testSlack",
            user = randomUser(),
            lastUpdateTime = Instant.now(),
            chime = null,
            slack = slack,
            customWebhook = null,
            email = null
        )

        // 1. create a destination as admin user
        createDestination(destination, true)

        val inputMap = HashMap<String, Any>()
        inputMap["missing"] = "_last"
        inputMap["destinationType"] = "slack"

        // 2. get destinations as admin user
        val adminResponse = getDestinations(client(), inputMap)
        assertEquals(1, adminResponse.size)
    }

    // Destination related tests

    fun `test get destination with an user with get destination role`() {
        createUserWithTestDataAndCustomRole(
            user,
            TEST_HR_INDEX,
            TEST_HR_ROLE,
            TEST_HR_BACKEND_ROLE,
            getClusterPermissionsFromCustomRole(ALERTING_GET_DESTINATION_ACCESS)
        )

        createDestination(getTestDestination())

        try {
            val getDestinationResponse = userClient?.makeRequest(
                "GET",
                AlertingPlugin.DESTINATION_BASE_URI,
                null,
                BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
            )
            assertEquals("Index Email Group failed", RestStatus.OK, getDestinationResponse?.restStatus())
        } finally {
            deleteRoleAndRoleMapping(TEST_HR_ROLE)
        }
    }
}

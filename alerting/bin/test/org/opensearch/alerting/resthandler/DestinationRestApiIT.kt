/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.junit.Assert
import org.opensearch.alerting.AlertingRestTestCase
import org.opensearch.alerting.model.destination.Chime
import org.opensearch.alerting.model.destination.CustomWebhook
import org.opensearch.alerting.model.destination.Destination
import org.opensearch.alerting.model.destination.Slack
import org.opensearch.alerting.model.destination.email.Email
import org.opensearch.alerting.model.destination.email.Recipient
import org.opensearch.alerting.randomUser
import org.opensearch.alerting.util.DestinationType
import org.opensearch.test.junit.annotations.TestLogging
import java.time.Instant

@TestLogging("level:DEBUG", reason = "Debug for tests.")
@Suppress("UNCHECKED_CAST")
class DestinationRestApiIT : AlertingRestTestCase() {

    fun `test creating a chime destination`() {
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
        Assert.assertNotNull("chime object should not be null", createdDestination.chime)
    }

    fun `test creating a custom webhook destination with url`() {
        val customWebhook = CustomWebhook("http://abc.com", null, null, 80, null, "PUT", emptyMap(), emptyMap(), null, null)
        val destination = Destination(
            type = DestinationType.CUSTOM_WEBHOOK,
            name = "test",
            user = randomUser(),
            lastUpdateTime = Instant.now(),
            chime = null,
            slack = null,
            customWebhook = customWebhook,
            email = null
        )
        val createdDestination = createDestination(destination = destination)
        assertEquals("Incorrect destination name", createdDestination.name, "test")
        assertEquals("Incorrect destination type", createdDestination.type, DestinationType.CUSTOM_WEBHOOK)
        Assert.assertNotNull("custom webhook object should not be null", createdDestination.customWebhook)
    }

    fun `test creating a custom webhook destination with host`() {
        val customWebhook = CustomWebhook(
            "", "http", "abc.com", 80, "a/b/c", "PATCH",
            mapOf("foo" to "1", "bar" to "2"), mapOf("h1" to "1", "h2" to "2"), null, null
        )
        val destination = Destination(
            type = DestinationType.CUSTOM_WEBHOOK,
            name = "test",
            user = randomUser(),
            lastUpdateTime = Instant.now(),
            chime = null,
            slack = null,
            customWebhook = customWebhook,
            email = null
        )
        val createdDestination = createDestination(destination = destination)
        assertEquals("Incorrect destination name", createdDestination.name, "test")
        assertEquals("Incorrect destination type", createdDestination.type, DestinationType.CUSTOM_WEBHOOK)
        assertEquals("Incorrect destination host", createdDestination.customWebhook?.host, "abc.com")
        assertEquals("Incorrect destination port", createdDestination.customWebhook?.port, 80)
        assertEquals("Incorrect destination path", createdDestination.customWebhook?.path, "a/b/c")
        assertEquals("Incorrect destination scheme", createdDestination.customWebhook?.scheme, "http")
        assertEquals("Incorrect destination method", createdDestination.customWebhook?.method, "PATCH")
        Assert.assertNotNull("custom webhook object should not be null", createdDestination.customWebhook)
    }

    fun `test creating an email destination`() {
        val recipient = Recipient(type = Recipient.RecipientType.EMAIL, emailGroupID = null, email = "test@email.com")
        val email = Email("", listOf(recipient))
        val destination = Destination(
            type = DestinationType.EMAIL,
            name = "test",
            user = randomUser(),
            lastUpdateTime = Instant.now(),
            chime = null,
            slack = null,
            customWebhook = null,
            email = email
        )

        val createdDestination = createDestination(destination = destination)
        Assert.assertNotNull("Email object should not be null", createdDestination.email)
        assertEquals("Incorrect destination name", createdDestination.name, "test")
        assertEquals("Incorrect destination type", createdDestination.type, DestinationType.EMAIL)
        assertEquals(
            "Incorrect email destination recipient type", createdDestination.email?.recipients?.get(0)?.type,
            Recipient.RecipientType.EMAIL
        )
        assertEquals(
            "Incorrect email destination recipient email", createdDestination.email?.recipients?.get(0)?.email,
            "test@email.com"
        )
    }

    fun `test get destination`() {
        val destination = createDestination()
        val getDestinationResponse = getDestination(destination)
        assertEquals(destination.id, getDestinationResponse["id"])
        assertEquals(destination.type.value, getDestinationResponse["type"])
        assertEquals(destination.seqNo, getDestinationResponse["seq_no"])
        assertEquals(destination.lastUpdateTime.toEpochMilli(), getDestinationResponse["last_update_time"])
        assertEquals(destination.primaryTerm, getDestinationResponse["primary_term"])
    }

    fun `test get destinations with slack destination type`() {
        val slack = Slack("url")
        val dest = Destination(
            type = DestinationType.SLACK,
            name = "testSlack",
            user = randomUser(),
            lastUpdateTime = Instant.now(),
            chime = null,
            slack = slack,
            customWebhook = null,
            email = null
        )

        val inputMap = HashMap<String, Any>()
        inputMap["missing"] = "_last"
        inputMap["destinationType"] = "slack"

        val destination = createDestination(dest)
        val destination2 = createDestination()
        val getDestinationsResponse = getDestinations(inputMap)

        assertEquals(1, getDestinationsResponse.size)
        val getDestinationResponse = getDestinationsResponse[0]

        assertEquals(destination.id, getDestinationResponse["id"])
        assertNotEquals(destination2.id, getDestinationResponse["id"])
        assertEquals(destination.type.value, getDestinationResponse["type"])
        assertEquals(destination.seqNo, getDestinationResponse["seq_no"])
        assertEquals(destination.lastUpdateTime.toEpochMilli(), getDestinationResponse["last_update_time"])
        assertEquals(destination.primaryTerm, getDestinationResponse["primary_term"])
    }

    fun `test get destinations matching a given name`() {
        val slack = Slack("url")
        val dest = Destination(
            type = DestinationType.SLACK,
            name = "testSlack",
            user = randomUser(),
            lastUpdateTime = Instant.now(),
            chime = null,
            slack = slack,
            customWebhook = null,
            email = null
        )

        val inputMap = HashMap<String, Any>()
        inputMap["searchString"] = "testSlack"

        val destination = createDestination(dest)
        val destination2 = createDestination()
        val getDestinationsResponse = getDestinations(inputMap)

        assertEquals(1, getDestinationsResponse.size)
        val getDestinationResponse = getDestinationsResponse[0]

        assertEquals(destination.id, getDestinationResponse["id"])
        assertNotEquals(destination2.id, getDestinationResponse["id"])
        assertEquals(destination.type.value, getDestinationResponse["type"])
        assertEquals(destination.seqNo, getDestinationResponse["seq_no"])
        assertEquals(destination.lastUpdateTime.toEpochMilli(), getDestinationResponse["last_update_time"])
        assertEquals(destination.primaryTerm, getDestinationResponse["primary_term"])
    }
}

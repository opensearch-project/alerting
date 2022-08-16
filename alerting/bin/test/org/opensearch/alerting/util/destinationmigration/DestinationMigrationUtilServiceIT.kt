/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util.destinationmigration

import org.opensearch.alerting.AlertingRestTestCase
import org.opensearch.alerting.core.model.ScheduledJob.Companion.SCHEDULED_JOBS_INDEX
import org.opensearch.alerting.makeRequest
import org.opensearch.alerting.model.destination.Destination
import org.opensearch.alerting.model.destination.email.Email
import org.opensearch.alerting.model.destination.email.EmailAccount
import org.opensearch.alerting.model.destination.email.EmailEntry
import org.opensearch.alerting.model.destination.email.EmailGroup
import org.opensearch.alerting.model.destination.email.Recipient
import org.opensearch.alerting.randomUser
import org.opensearch.alerting.toJsonString
import org.opensearch.alerting.util.DestinationType
import org.opensearch.client.ResponseException
import org.opensearch.rest.RestStatus
import java.time.Instant
import java.util.UUID

class DestinationMigrationUtilServiceIT : AlertingRestTestCase() {

    fun `test migrateData`() {
        if (isNotificationPluginInstalled()) {
            // Create alerting config index
            createRandomMonitor()

            val emailAccount = EmailAccount(
                name = "test",
                email = "test@email.com",
                host = "smtp.com",
                port = 25,
                method = EmailAccount.MethodType.NONE,
                username = null,
                password = null
            )
            val emailAccountDoc = "{\"email_account\" : ${emailAccount.toJsonString()}}"
            val emailGroup = EmailGroup(
                name = "test",
                emails = listOf(EmailEntry("test@email.com"))
            )
            val emailGroupDoc = "{\"email_group\" : ${emailGroup.toJsonString()}}"
            val emailAccountId = UUID.randomUUID().toString()
            val emailGroupId = UUID.randomUUID().toString()
            indexDocWithAdminClient(SCHEDULED_JOBS_INDEX, emailAccountId, emailAccountDoc)
            indexDocWithAdminClient(SCHEDULED_JOBS_INDEX, emailGroupId, emailGroupDoc)

            val recipient = Recipient(Recipient.RecipientType.EMAIL, null, "test@email.com")
            val email = Email(emailAccountId, listOf(recipient))
            val emailDest = Destination(
                id = UUID.randomUUID().toString(),
                type = DestinationType.EMAIL,
                name = "test",
                user = randomUser(),
                lastUpdateTime = Instant.now(),
                chime = null,
                slack = null,
                customWebhook = null,
                email = email
            )
            val slackDestination = getSlackDestination().copy(id = UUID.randomUUID().toString())
            val chimeDestination = getChimeDestination().copy(id = UUID.randomUUID().toString())
            val customWebhookDestination = getCustomWebhookDestination().copy(id = UUID.randomUUID().toString())

            val destinations = listOf(emailDest, slackDestination, chimeDestination, customWebhookDestination)

            val ids = mutableListOf(emailAccountId, emailGroupId)
            for (destination in destinations) {
                val dest = """
                    {
                      "destination" : ${destination.toJsonString()}
                    }
                """.trimIndent()
                indexDocWithAdminClient(SCHEDULED_JOBS_INDEX, destination.id, dest)
                ids.add(destination.id)
            }

            // Create cluster change event and wait for migration service to complete migrating data over
            client().updateSettings("indices.recovery.max_bytes_per_sec", "40mb")
            Thread.sleep(120000)

            for (id in ids) {
                val response = client().makeRequest(
                    "GET",
                    "_plugins/_notifications/configs/$id"
                )
                assertEquals(RestStatus.OK, response.restStatus())

                try {
                    client().makeRequest(
                        "GET",
                        ".opendistro-alerting-config/_doc/$id"
                    )
                    fail("Expecting ResponseException")
                } catch (e: ResponseException) {
                    assertEquals(RestStatus.NOT_FOUND, e.response.restStatus())
                }
            }
        }
    }
}

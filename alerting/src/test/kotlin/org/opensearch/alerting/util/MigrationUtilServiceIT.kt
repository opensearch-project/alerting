package org.opensearch.alerting.util

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
import org.opensearch.client.ResponseException
// import org.opensearch.common.xcontent.LoggingDeprecationHandler
// import org.opensearch.common.xcontent.NamedXContentRegistry
// import org.opensearch.common.xcontent.json.JsonXContent
import org.opensearch.rest.RestStatus
import java.time.Instant
import java.util.UUID

class MigrationUtilServiceIT : AlertingRestTestCase() {

    fun `test migrateData`() {
        if (isNotificationPluginInstalled()) {
            val destinations = mutableListOf<Destination>()

            createRandomMonitor()


//        val emailAccount = getTestEmailAccount()


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
            indexDoc(SCHEDULED_JOBS_INDEX, emailAccountId, emailAccountDoc)
            indexDoc(SCHEDULED_JOBS_INDEX, emailGroupId, emailGroupDoc)


            destinations.add(getSlackDestination().copy(id = UUID.randomUUID().toString()))
            destinations.add(getChimeDestination().copy(id = UUID.randomUUID().toString()))
            destinations.add(getCustomWebhookDestination().copy(id = UUID.randomUUID().toString()))
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
            destinations.add(emailDest)

//        destinations.add(getEmailDestination().copy(id = UUID.randomUUID().toString()))

//        val dest = "\"destination\" : {${destination.toJsonString()}}"


            val ids = mutableListOf<String>()
            for (destination in destinations) {
                val dest = """
              {
                "destination" : ${destination.toJsonString()}
              }
            """.trimIndent()
                indexDoc(SCHEDULED_JOBS_INDEX, destination.id, dest)
                logger.info("Destination: ${destination.type} - ${destination.id}")
                ids.add(destination.id)
            }

//        val dest = """
//          {
//            "destination" : ${destination.toJsonString()}
//          }
//        """.trimIndent()
//        val dest = """
//          {
//            "destination" : {
//              "id" : "$id",
//              "type" : "chime",
//              "name" : "pentestChime111",
//              "user" : {
//                "name" : "",
//                "backend_roles" : [ ],
//                "roles" : [ ],
//                "custom_attribute_names" : [ ],
//                "user_requested_tenant" : null
//              },
//              "schema_version" : 3,
//              "seq_no" : 0,
//              "primary_term" : 0,
//              "last_update_time" : 1628637838944,
//              "chime" : {
//                "url" : "https://www.example4.com"
//              }
//            }
//          }
//        """.trimIndent()
//        wipeAllODFEIndices()
//        createRandomMonitor()
//        indexDoc(SCHEDULED_JOBS_INDEX, id, dest)
            client().updateSettings("indices.recovery.max_bytes_per_sec", "40mb")
            Thread.sleep(120000)

//            for (destination in destinations) {
//                ids.add(destination.id)
//            }
            ids.add(emailAccountId)
            ids.add(emailGroupId)
            for (id in ids) {
                val response = client().makeRequest(
                    "GET",
                    "_plugins/_notifications/configs/$id"
                )
                assertEquals(RestStatus.OK, response.restStatus())
//            val valJson = JsonXContent.jsonXContent.createParser(
//                NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE,
//                response.entity.content
//            ).map()
//            logger.info("Notification data: $valJson")

                try {
                    client().makeRequest(
                        "GET",
                        ".opendistro-alerting-config/_doc/$id", // "_plugins/_notifications/configs"
                    )
                    fail("Expecting ResponseException")
                } catch (e: ResponseException) {
                    assertEquals(RestStatus.NOT_FOUND, e.response.restStatus())
                }
//        val valJson2 = JsonXContent.jsonXContent.createParser(
//            NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE,
//            response2.entity.content
//        ).map()
//        logger.info("config index response: ${response2.restStatus()}")
//        logger.info("config index data: $valJson2")
//            assertEquals(RestStatus.OK, response.restStatus())
            }
        }
    }
}

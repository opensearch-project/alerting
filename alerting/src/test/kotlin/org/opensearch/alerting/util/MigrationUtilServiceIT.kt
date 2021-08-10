package org.opensearch.alerting.util

import org.opensearch.alerting.AlertingRestTestCase
import org.opensearch.alerting.core.model.ScheduledJob.Companion.SCHEDULED_JOBS_INDEX
import org.opensearch.alerting.makeRequest
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.json.JsonXContent
import java.util.UUID

class MigrationUtilServiceIT : AlertingRestTestCase() {

    fun `test migrateData`() {
        val destination = getSlackDestination()
        val id = UUID.randomUUID().toString()
        wipeAllODFEIndices()
        createAlertingConfigIndex()
        indexDoc(SCHEDULED_JOBS_INDEX, id, destination.toJsonString())
        Thread.sleep(10000)
        val response = client().makeRequest(
            "GET",
            "_plugins/_notifications/configs"
        )
        val valJson = JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE,
            response.entity.content
        ).map()
        logger.info("Notification data: $valJson")

        val response2 = client().makeRequest(
            "GET",
            "_plugins/_alerting/destinations", // "_plugins/_notifications/configs"
        )
        val valJson2 = JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE,
            response2.entity.content
        ).map()
        logger.info("destination data: $valJson2")
        assertEquals("random", valJson)
    }
}

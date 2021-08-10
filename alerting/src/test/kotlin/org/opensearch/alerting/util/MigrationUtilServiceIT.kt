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
        val id = "bHz3xHoB77vMD-zLfb9o"
        wipeAllODFEIndices()
        createRandomMonitor()
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
            ".opendistro-alerting-config/_doc/$id", // "_plugins/_notifications/configs"
        )
        val valJson2 = JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE,
            response2.entity.content
        ).map()
        logger.info("config index data: $valJson2")
        assertEquals(id, valJson)
    }
}

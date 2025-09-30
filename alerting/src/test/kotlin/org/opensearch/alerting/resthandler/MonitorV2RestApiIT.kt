package org.opensearch.alerting.resthandler

import org.opensearch.alerting.AlertingRestTestCase
import org.opensearch.alerting.resthandler.MonitorRestApiIT.Companion.USE_TYPED_KEYS
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.core.common.bytes.BytesReference
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.test.junit.annotations.TestLogging

@TestLogging("level:DEBUG", reason = "Debug for tests.")
@Suppress("UNCHECKED_CAST")
class MonitorV2RestApiIT : AlertingRestTestCase() {
    fun `test parsing monitor v2 as a scheduled job`() {
        val monitorV2 = createRandomPPLMonitor()

        val builder = monitorV2.toXContentWithUser(XContentBuilder.builder(XContentType.JSON.xContent()), USE_TYPED_KEYS)
        val string = BytesReference.bytes(builder).utf8ToString()
        val xcp = createParser(XContentType.JSON.xContent(), string)
        val scheduledJob = ScheduledJob.parse(xcp, monitorV2.id, monitorV2.version)
        assertEquals(monitorV2, scheduledJob)
    }
}

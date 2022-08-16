/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.core

import org.joda.time.DateTime
import org.junit.Test
import org.opensearch.alerting.core.schedule.JobSchedulerMetrics
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.test.OpenSearchTestCase.assertEquals

class WriteableTests {

    @Test
    fun `test jobschedule metrics as stream`() {
        val metrics = JobSchedulerMetrics("test", DateTime.now().millis, false)
        val out = BytesStreamOutput()
        metrics.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newMetrics = JobSchedulerMetrics(sin)
        assertEquals("Round tripping metrics doesn't work", metrics.scheduledJobId, newMetrics.scheduledJobId)
    }
}

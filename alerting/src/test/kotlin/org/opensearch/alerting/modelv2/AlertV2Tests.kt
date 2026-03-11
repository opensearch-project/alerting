/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.modelv2

import org.opensearch.alerting.assertAlertV2sEqual
import org.opensearch.alerting.modelv2.AlertV2.Companion.ALERT_V2_ID_FIELD
import org.opensearch.alerting.modelv2.AlertV2.Companion.ALERT_V2_VERSION_FIELD
import org.opensearch.alerting.modelv2.AlertV2.Companion.ERROR_MESSAGE_FIELD
import org.opensearch.alerting.modelv2.AlertV2.Companion.EXECUTION_ID_FIELD
import org.opensearch.alerting.modelv2.AlertV2.Companion.SEVERITY_FIELD
import org.opensearch.alerting.randomAlertV2
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.test.OpenSearchTestCase

class AlertV2Tests : OpenSearchTestCase() {
    fun `test alertv2 as stream`() {
        val alertV2 = randomAlertV2()
        val out = BytesStreamOutput()
        alertV2.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newAlertV2 = AlertV2(sin)
        assertAlertV2sEqual(alertV2, newAlertV2)
    }

    fun `test alertv2 asTemplateArgs`() {
        val alertV2 = randomAlertV2()
        val templateArgs = alertV2.asTemplateArg()

        assertEquals(
            "Template args field $ALERT_V2_ID_FIELD doesn't match",
            alertV2.id,
            templateArgs[ALERT_V2_ID_FIELD]
        )
        assertEquals(
            "Template args field $ALERT_V2_VERSION_FIELD doesn't match",
            alertV2.version,
            templateArgs[ALERT_V2_VERSION_FIELD]
        )
        assertEquals(
            "Template args field $ERROR_MESSAGE_FIELD doesn't match",
            alertV2.errorMessage,
            templateArgs[ERROR_MESSAGE_FIELD]
        )
        assertEquals(
            "Template args field $EXECUTION_ID_FIELD doesn't match",
            alertV2.executionId,
            templateArgs[EXECUTION_ID_FIELD]
        )
        assertEquals(
            "Template args field $SEVERITY_FIELD doesn't match",
            alertV2.severity.value,
            templateArgs[SEVERITY_FIELD]
        )
    }
}

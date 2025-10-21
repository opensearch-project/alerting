/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.modelv2

import org.opensearch.alerting.assertPplTriggersEqual
import org.opensearch.alerting.modelv2.PPLTrigger.Companion.CONDITION_TYPE_FIELD
import org.opensearch.alerting.modelv2.PPLTrigger.Companion.CUSTOM_CONDITION_FIELD
import org.opensearch.alerting.modelv2.PPLTrigger.Companion.MODE_FIELD
import org.opensearch.alerting.modelv2.PPLTrigger.Companion.NUM_RESULTS_CONDITION_FIELD
import org.opensearch.alerting.modelv2.PPLTrigger.Companion.NUM_RESULTS_VALUE_FIELD
import org.opensearch.alerting.modelv2.TriggerV2.Companion.ACTIONS_FIELD
import org.opensearch.alerting.modelv2.TriggerV2.Companion.EXPIRE_FIELD
import org.opensearch.alerting.modelv2.TriggerV2.Companion.ID_FIELD
import org.opensearch.alerting.modelv2.TriggerV2.Companion.NAME_FIELD
import org.opensearch.alerting.modelv2.TriggerV2.Companion.SEVERITY_FIELD
import org.opensearch.alerting.modelv2.TriggerV2.Companion.THROTTLE_FIELD
import org.opensearch.alerting.randomPPLTrigger
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.test.OpenSearchTestCase
import java.lang.IllegalArgumentException

class TriggerV2Tests : OpenSearchTestCase() {
    fun `test min throttle duration`() {
        try {
            randomPPLTrigger(throttleDuration = 0)
            fail("Trigger with throttle duration less than 1 should be rejected")
        } catch (_: IllegalArgumentException) {}
    }

    fun `test min expire duration`() {
        try {
            randomPPLTrigger(expireDuration = 0)
            fail("Trigger with expire duration less than 1 should be rejected")
        } catch (_: IllegalArgumentException) {}
    }

    fun `test ppl trigger as stream`() {
        val pplTrigger = randomPPLTrigger()
        val out = BytesStreamOutput()
        pplTrigger.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newPplTrigger = PPLTrigger(sin)
        assertPplTriggersEqual(pplTrigger, newPplTrigger)
    }

    fun `test ppl trigger asTemplateArgs`() {
        val pplTrigger = randomPPLTrigger()
        val templateArgs = pplTrigger.asTemplateArg()

        assertEquals(
            "Template args field $ID_FIELD doesn't match",
            pplTrigger.id,
            templateArgs[ID_FIELD]
        )
        assertEquals(
            "Template args field $NAME_FIELD doesn't match",
            pplTrigger.name,
            templateArgs[NAME_FIELD]
        )
        assertEquals(
            "Template args field $SEVERITY_FIELD doesn't match",
            pplTrigger.severity.value,
            templateArgs[SEVERITY_FIELD]
        )
        assertEquals(
            "Template args field $THROTTLE_FIELD doesn't match",
            pplTrigger.throttleDuration,
            templateArgs[THROTTLE_FIELD]
        )
        assertEquals(
            "Template args field $EXPIRE_FIELD doesn't match",
            pplTrigger.expireDuration,
            templateArgs[EXPIRE_FIELD]
        )
        assertEquals(
            "Template args field $EXPIRE_FIELD doesn't match",
            pplTrigger.expireDuration,
            templateArgs[EXPIRE_FIELD]
        )
        val actions = templateArgs[ACTIONS_FIELD] as List<*>
        assertEquals("number of trigger actions doesn't match", pplTrigger.actions.size, actions.size)
        assertEquals(
            "Template args field $MODE_FIELD doesn't match",
            pplTrigger.mode.value,
            templateArgs[MODE_FIELD]
        )
        assertEquals(
            "Template args field $CONDITION_TYPE_FIELD doesn't match",
            pplTrigger.conditionType.value,
            templateArgs[CONDITION_TYPE_FIELD]
        )
        assertEquals(
            "Template args field $NUM_RESULTS_CONDITION_FIELD doesn't match",
            pplTrigger.numResultsCondition?.value,
            templateArgs[NUM_RESULTS_CONDITION_FIELD]
        )
        assertEquals(
            "Template args field $NUM_RESULTS_VALUE_FIELD doesn't match",
            pplTrigger.numResultsValue,
            templateArgs[NUM_RESULTS_VALUE_FIELD]
        )
        assertEquals(
            "Template args field $CUSTOM_CONDITION_FIELD doesn't match",
            pplTrigger.customCondition,
            templateArgs[CUSTOM_CONDITION_FIELD]
        )
    }
}

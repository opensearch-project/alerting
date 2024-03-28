/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model

import org.junit.Assert
import org.opensearch.alerting.model.destination.email.EmailAccount
import org.opensearch.alerting.model.destination.email.EmailGroup
import org.opensearch.alerting.randomBucketLevelMonitorRunResult
import org.opensearch.alerting.randomBucketLevelTriggerRunResult
import org.opensearch.alerting.randomDocumentLevelMonitorRunResult
import org.opensearch.alerting.randomEmailAccount
import org.opensearch.alerting.randomEmailGroup
import org.opensearch.alerting.randomInputRunResults
import org.opensearch.alerting.randomQueryLevelMonitorRunResult
import org.opensearch.alerting.randomQueryLevelTriggerRunResult
import org.opensearch.common.UUIDs
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.commons.alerting.model.SearchInput
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.test.OpenSearchTestCase
import java.time.Instant

class WriteableTests : OpenSearchTestCase() {

    fun `test actionrunresult as stream`() {
        val actionRunResult = randomActionRunResult()
        val out = BytesStreamOutput()
        actionRunResult.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newActionRunResult = ActionRunResult(sin)
        assertEquals("Round tripping ActionRunResult doesn't work", actionRunResult, newActionRunResult)
    }

    fun `test query-level triggerrunresult as stream`() {
        val runResult = randomQueryLevelTriggerRunResult()
        val out = BytesStreamOutput()
        runResult.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newRunResult = QueryLevelTriggerRunResult(sin)
        assertEquals(runResult.triggerName, newRunResult.triggerName)
        assertEquals(runResult.triggered, newRunResult.triggered)
        assertEquals(runResult.error, newRunResult.error)
        assertEquals(runResult.actionResults, newRunResult.actionResults)
    }

    fun `test bucket-level triggerrunresult as stream`() {
        val runResult = randomBucketLevelTriggerRunResult()
        val out = BytesStreamOutput()
        runResult.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newRunResult = BucketLevelTriggerRunResult(sin)
        assertEquals("Round tripping ActionRunResult doesn't work", runResult, newRunResult)
    }

    fun `test doc-level triggerrunresult as stream`() {
        val runResult = randomDocumentLevelTriggerRunResult()
        val out = BytesStreamOutput()
        runResult.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newRunResult = DocumentLevelTriggerRunResult(sin)
        assertEquals("Round tripping ActionRunResult doesn't work", runResult, newRunResult)
    }

    fun `test inputrunresult as stream`() {
        val runResult = randomInputRunResults()
        val out = BytesStreamOutput()
        runResult.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newRunResult = InputRunResults.readFrom(sin)
        assertEquals("Round tripping InputRunResults doesn't work", runResult, newRunResult)
    }

    fun `test query-level monitorrunresult as stream`() {
        val runResult = randomQueryLevelMonitorRunResult()
        val out = BytesStreamOutput()
        runResult.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newRunResult = MonitorRunResult<QueryLevelTriggerRunResult>(sin)
        assertEquals("Round tripping MonitorRunResult doesn't work", runResult, newRunResult)
    }

    fun `test bucket-level monitorrunresult as stream`() {
        val runResult = randomBucketLevelMonitorRunResult()
        val out = BytesStreamOutput()
        runResult.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newRunResult = MonitorRunResult<BucketLevelTriggerRunResult>(sin)
        assertEquals("Round tripping MonitorRunResult doesn't work", runResult, newRunResult)
    }

    fun `test doc-level monitorrunresult as stream`() {
        val runResult = randomDocumentLevelMonitorRunResult()
        val out = BytesStreamOutput()
        runResult.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newRunResult = MonitorRunResult<DocumentLevelTriggerRunResult>(sin)
        assertEquals("Round tripping MonitorRunResult doesn't work", runResult, newRunResult)
    }

    fun `test searchinput as stream`() {
        val input = SearchInput(emptyList(), SearchSourceBuilder())
        val out = BytesStreamOutput()
        input.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newInput = SearchInput(sin)
        assertEquals("Round tripping MonitorRunResult doesn't work", input, newInput)
    }

    fun `test emailaccount as stream`() {
        val emailAccount = randomEmailAccount()
        val out = BytesStreamOutput()
        emailAccount.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newEmailAccount = EmailAccount.readFrom(sin)
        assertEquals("Round tripping EmailAccount doesn't work", emailAccount, newEmailAccount)
    }

    fun `test emailgroup as stream`() {
        val emailGroup = randomEmailGroup()
        val out = BytesStreamOutput()
        emailGroup.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newEmailGroup = EmailGroup.readFrom(sin)
        assertEquals("Round tripping EmailGroup doesn't work", emailGroup, newEmailGroup)
    }

    fun `test DocumentLevelTriggerRunResult as stream`() {
        val workflow = randomDocumentLevelTriggerRunResult()
        val out = BytesStreamOutput()
        workflow.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newWorkflow = DocumentLevelTriggerRunResult(sin)
        Assert.assertEquals("Round tripping dltrr failed", newWorkflow, workflow)
    }

    fun randomDocumentLevelTriggerRunResult(): DocumentLevelTriggerRunResult {
        val map = mutableMapOf<String, ActionRunResult>()
        map.plus(Pair("key1", randomActionRunResult()))
        map.plus(Pair("key2", randomActionRunResult()))
        return DocumentLevelTriggerRunResult(
            "trigger-name",
            mutableListOf(UUIDs.randomBase64UUID().toString()),
            null,
            mutableMapOf(Pair("alertId", map))
        )
    }

    fun randomActionRunResult(): ActionRunResult {
        val map = mutableMapOf<String, String>()
        map.plus(Pair("key1", "val1"))
        map.plus(Pair("key2", "val2"))
        return ActionRunResult(
            "1234", "test-action", map,
            false, Instant.now(), null
        )
    }
}

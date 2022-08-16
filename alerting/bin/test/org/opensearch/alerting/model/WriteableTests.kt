/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model

import org.opensearch.alerting.core.model.SearchInput
import org.opensearch.alerting.model.action.Action
import org.opensearch.alerting.model.action.ActionExecutionPolicy
import org.opensearch.alerting.model.action.Throttle
import org.opensearch.alerting.model.destination.email.EmailAccount
import org.opensearch.alerting.model.destination.email.EmailGroup
import org.opensearch.alerting.randomAction
import org.opensearch.alerting.randomActionExecutionPolicy
import org.opensearch.alerting.randomActionRunResult
import org.opensearch.alerting.randomBucketLevelMonitorRunResult
import org.opensearch.alerting.randomBucketLevelTrigger
import org.opensearch.alerting.randomBucketLevelTriggerRunResult
import org.opensearch.alerting.randomDocumentLevelMonitorRunResult
import org.opensearch.alerting.randomDocumentLevelTrigger
import org.opensearch.alerting.randomDocumentLevelTriggerRunResult
import org.opensearch.alerting.randomEmailAccount
import org.opensearch.alerting.randomEmailGroup
import org.opensearch.alerting.randomInputRunResults
import org.opensearch.alerting.randomQueryLevelMonitor
import org.opensearch.alerting.randomQueryLevelMonitorRunResult
import org.opensearch.alerting.randomQueryLevelTrigger
import org.opensearch.alerting.randomQueryLevelTriggerRunResult
import org.opensearch.alerting.randomThrottle
import org.opensearch.alerting.randomUser
import org.opensearch.alerting.randomUserEmpty
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.commons.authuser.User
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.test.OpenSearchTestCase

class WriteableTests : OpenSearchTestCase() {

    fun `test throttle as stream`() {
        val throttle = randomThrottle()
        val out = BytesStreamOutput()
        throttle.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newThrottle = Throttle(sin)
        assertEquals("Round tripping Throttle doesn't work", throttle, newThrottle)
    }

    fun `test action as stream`() {
        val action = randomAction()
        val out = BytesStreamOutput()
        action.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newAction = Action(sin)
        assertEquals("Round tripping Action doesn't work", action, newAction)
    }

    fun `test action as stream with null subject template`() {
        val action = randomAction().copy(subjectTemplate = null)
        val out = BytesStreamOutput()
        action.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newAction = Action(sin)
        assertEquals("Round tripping Action doesn't work", action, newAction)
    }

    fun `test action as stream with null throttle`() {
        val action = randomAction().copy(throttle = null)
        val out = BytesStreamOutput()
        action.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newAction = Action(sin)
        assertEquals("Round tripping Action doesn't work", action, newAction)
    }

    fun `test action as stream with throttled enabled and null throttle`() {
        val action = randomAction().copy(throttle = null).copy(throttleEnabled = true)
        val out = BytesStreamOutput()
        action.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newAction = Action(sin)
        assertEquals("Round tripping Action doesn't work", action, newAction)
    }

    fun `test query-level monitor as stream`() {
        val monitor = randomQueryLevelMonitor().copy(inputs = listOf(SearchInput(emptyList(), SearchSourceBuilder())))
        val out = BytesStreamOutput()
        monitor.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newMonitor = Monitor(sin)
        assertEquals("Round tripping QueryLevelMonitor doesn't work", monitor, newMonitor)
    }

    fun `test query-level trigger as stream`() {
        val trigger = randomQueryLevelTrigger()
        val out = BytesStreamOutput()
        trigger.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newTrigger = QueryLevelTrigger.readFrom(sin)
        assertEquals("Round tripping QueryLevelTrigger doesn't work", trigger, newTrigger)
    }

    fun `test bucket-level trigger as stream`() {
        val trigger = randomBucketLevelTrigger()
        val out = BytesStreamOutput()
        trigger.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newTrigger = BucketLevelTrigger.readFrom(sin)
        assertEquals("Round tripping BucketLevelTrigger doesn't work", trigger, newTrigger)
    }

    fun `test doc-level trigger as stream`() {
        val trigger = randomDocumentLevelTrigger()
        val out = BytesStreamOutput()
        trigger.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newTrigger = DocumentLevelTrigger.readFrom(sin)
        assertEquals("Round tripping DocumentLevelTrigger doesn't work", trigger, newTrigger)
    }

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
        assertEquals("Round tripping ActionRunResult doesn't work", runResult, newRunResult)
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

    fun `test user as stream`() {
        val user = randomUser()
        val out = BytesStreamOutput()
        user.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newUser = User(sin)
        assertEquals("Round tripping User doesn't work", user, newUser)
    }

    fun `test empty user as stream`() {
        val user = randomUserEmpty()
        val out = BytesStreamOutput()
        user.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newUser = User(sin)
        assertEquals("Round tripping User doesn't work", user, newUser)
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

    fun `test action execution policy as stream`() {
        val actionExecutionPolicy = randomActionExecutionPolicy()
        val out = BytesStreamOutput()
        actionExecutionPolicy.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newActionExecutionPolicy = ActionExecutionPolicy.readFrom(sin)
        assertEquals("Round tripping ActionExecutionPolicy doesn't work", actionExecutionPolicy, newActionExecutionPolicy)
    }
}

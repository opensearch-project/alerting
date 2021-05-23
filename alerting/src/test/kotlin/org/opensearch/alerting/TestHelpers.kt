/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */
package org.opensearch.alerting

import org.apache.http.Header
import org.apache.http.HttpEntity
import org.opensearch.alerting.core.model.Input
import org.opensearch.alerting.core.model.IntervalSchedule
import org.opensearch.alerting.core.model.Schedule
import org.opensearch.alerting.core.model.SearchInput
import org.opensearch.alerting.elasticapi.string
import org.opensearch.alerting.model.ActionExecutionResult
import org.opensearch.alerting.model.ActionRunResult
import org.opensearch.alerting.model.Alert
import org.opensearch.alerting.model.InputRunResults
import org.opensearch.alerting.model.Monitor
import org.opensearch.alerting.model.MonitorRunResult
import org.opensearch.alerting.model.Trigger
import org.opensearch.alerting.model.TriggerRunResult
import org.opensearch.alerting.model.action.Action
import org.opensearch.alerting.model.action.Throttle
import org.opensearch.alerting.model.destination.email.EmailAccount
import org.opensearch.alerting.model.destination.email.EmailEntry
import org.opensearch.alerting.model.destination.email.EmailGroup
import org.opensearch.client.Request
import org.opensearch.client.RequestOptions
import org.opensearch.client.Response
import org.opensearch.client.RestClient
import org.opensearch.client.WarningsHandler
import org.opensearch.common.UUIDs
import org.opensearch.common.settings.SecureString
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.authuser.User
import org.opensearch.index.query.QueryBuilders
import org.opensearch.script.Script
import org.opensearch.script.ScriptType
import org.opensearch.search.SearchModule
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.test.OpenSearchTestCase.randomInt
import org.opensearch.test.OpenSearchTestCase.randomIntBetween
import org.opensearch.test.rest.OpenSearchRestTestCase
import java.time.Instant
import java.time.temporal.ChronoUnit

fun randomMonitor(
    name: String = OpenSearchRestTestCase.randomAlphaOfLength(10),
    user: User = randomUser(),
    inputs: List<Input> = listOf(SearchInput(emptyList(), SearchSourceBuilder().query(QueryBuilders.matchAllQuery()))),
    schedule: Schedule = IntervalSchedule(interval = 5, unit = ChronoUnit.MINUTES),
    enabled: Boolean = OpenSearchTestCase.randomBoolean(),
    triggers: List<Trigger> = (1..randomInt(10)).map { randomTrigger() },
    enabledTime: Instant? = if (enabled) Instant.now().truncatedTo(ChronoUnit.MILLIS) else null,
    lastUpdateTime: Instant = Instant.now().truncatedTo(ChronoUnit.MILLIS),
    withMetadata: Boolean = false
): Monitor {
    return Monitor(
        name = name, enabled = enabled, inputs = inputs, schedule = schedule, triggers = triggers,
        enabledTime = enabledTime, lastUpdateTime = lastUpdateTime,
        user = user, uiMetadata = if (withMetadata) mapOf("foo" to "bar") else mapOf()
    )
}

// Monitor of older versions without security.
fun randomMonitorWithoutUser(
    name: String = OpenSearchRestTestCase.randomAlphaOfLength(10),
    inputs: List<Input> = listOf(SearchInput(emptyList(), SearchSourceBuilder().query(QueryBuilders.matchAllQuery()))),
    schedule: Schedule = IntervalSchedule(interval = 5, unit = ChronoUnit.MINUTES),
    enabled: Boolean = OpenSearchTestCase.randomBoolean(),
    triggers: List<Trigger> = (1..randomInt(10)).map { randomTrigger() },
    enabledTime: Instant? = if (enabled) Instant.now().truncatedTo(ChronoUnit.MILLIS) else null,
    lastUpdateTime: Instant = Instant.now().truncatedTo(ChronoUnit.MILLIS),
    withMetadata: Boolean = false
): Monitor {
    return Monitor(
        name = name, enabled = enabled, inputs = inputs, schedule = schedule, triggers = triggers,
        enabledTime = enabledTime, lastUpdateTime = lastUpdateTime,
        user = null, uiMetadata = if (withMetadata) mapOf("foo" to "bar") else mapOf()
    )
}

fun randomTrigger(
    id: String = UUIDs.base64UUID(),
    name: String = OpenSearchRestTestCase.randomAlphaOfLength(10),
    severity: String = "1",
    condition: Script = randomScript(),
    actions: List<Action> = mutableListOf(),
    destinationId: String = ""
): Trigger {
    return Trigger(
        id = id,
        name = name,
        severity = severity,
        condition = condition,
        actions = if (actions.isEmpty()) (0..randomInt(10)).map { randomAction(destinationId = destinationId) } else actions
    )
}

fun randomEmailAccount(
    name: String = OpenSearchRestTestCase.randomAlphaOfLength(10),
    email: String = OpenSearchRestTestCase.randomAlphaOfLength(5) + "@email.com",
    host: String = OpenSearchRestTestCase.randomAlphaOfLength(10),
    port: Int = randomIntBetween(1, 100),
    method: EmailAccount.MethodType = randomEmailAccountMethod(),
    username: SecureString? = null,
    password: SecureString? = null
): EmailAccount {
    return EmailAccount(
        name = name,
        email = email,
        host = host,
        port = port,
        method = method,
        username = username,
        password = password
    )
}

fun randomEmailGroup(
    name: String = OpenSearchRestTestCase.randomAlphaOfLength(10),
    emails: List<EmailEntry> = (1..randomInt(10)).map { EmailEntry(email = OpenSearchRestTestCase.randomAlphaOfLength(5) + "@email.com") }
): EmailGroup {
    return EmailGroup(name = name, emails = emails)
}

fun randomScript(source: String = "return " + OpenSearchRestTestCase.randomBoolean().toString()): Script = Script(source)

val ALERTING_BASE_URI = "/_plugins/_alerting/monitors"
val DESTINATION_BASE_URI = "/_plugins/_alerting/destinations"
val LEGACY_OPENDISTRO_ALERTING_BASE_URI = "/_opendistro/_alerting/monitors"
val LEGACY_OPENDISTRO_DESTINATION_BASE_URI = "/_opendistro/_alerting/destinations"
val ALWAYS_RUN = Script("return true")
val NEVER_RUN = Script("return false")
val DRYRUN_MONITOR = mapOf("dryrun" to "true")

fun randomTemplateScript(
    source: String,
    params: Map<String, String> = emptyMap()
): Script = Script(ScriptType.INLINE, Script.DEFAULT_TEMPLATE_LANG, source, params)

fun randomAction(
    name: String = OpenSearchRestTestCase.randomUnicodeOfLength(10),
    template: Script = randomTemplateScript("Hello World"),
    destinationId: String = "",
    throttleEnabled: Boolean = false,
    throttle: Throttle = randomThrottle()
) = Action(name, destinationId, template, template, throttleEnabled, throttle)

fun randomThrottle(
    value: Int = randomIntBetween(60, 120),
    unit: ChronoUnit = ChronoUnit.MINUTES
) = Throttle(value, unit)

fun randomAlert(monitor: Monitor = randomMonitor()): Alert {
    val trigger = randomTrigger()
    val actionExecutionResults = mutableListOf(randomActionExecutionResult(), randomActionExecutionResult())
    return Alert(
        monitor, trigger, Instant.now().truncatedTo(ChronoUnit.MILLIS), null,
        actionExecutionResults = actionExecutionResults
    )
}

fun randomEmailAccountMethod(): EmailAccount.MethodType {
    val methodValues = EmailAccount.MethodType.values().map { it.value }
    val randomValue = methodValues[randomInt(methodValues.size - 1)]
    return EmailAccount.MethodType.getByValue(randomValue)!!
}

fun randomActionExecutionResult(
    actionId: String = UUIDs.base64UUID(),
    lastExecutionTime: Instant = Instant.now().truncatedTo(ChronoUnit.MILLIS),
    throttledCount: Int = randomInt()
) = ActionExecutionResult(actionId, lastExecutionTime, throttledCount)

fun randomMonitorRunResult(): MonitorRunResult {
    val triggerResults = mutableMapOf<String, TriggerRunResult>()
    val triggerRunResult = randomTriggerRunResult()
    triggerResults.plus(Pair("test", triggerRunResult))

    return MonitorRunResult(
        "test-monitor",
        Instant.now(),
        Instant.now(),
        null,
        randomInputRunResults(),
        triggerResults
    )
}

fun randomInputRunResults(): InputRunResults {
    return InputRunResults(listOf(), null)
}

fun randomTriggerRunResult(): TriggerRunResult {
    val map = mutableMapOf<String, ActionRunResult>()
    map.plus(Pair("key1", randomActionRunResult()))
    map.plus(Pair("key2", randomActionRunResult()))
    return TriggerRunResult("trigger-name", true, null, map)
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

fun Monitor.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder).string()
}

fun randomUser(): User {
    return User(
        OpenSearchRestTestCase.randomAlphaOfLength(10),
        listOf(
            OpenSearchRestTestCase.randomAlphaOfLength(10),
            OpenSearchRestTestCase.randomAlphaOfLength(10)
        ),
        listOf(OpenSearchRestTestCase.randomAlphaOfLength(10), "all_access"),
        listOf("test_attr=test")
    )
}

fun randomUserEmpty(): User {
    return User("", listOf(), listOf(), listOf())
}

fun EmailAccount.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder).string()
}

fun EmailGroup.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder).string()
}

/**
 * Wrapper for [RestClient.performRequest] which was deprecated in ES 6.5 and is used in tests. This provides
 * a single place to suppress deprecation warnings. This will probably need further work when the API is removed entirely
 * but that's an exercise for another day.
 */
@Suppress("DEPRECATION")
fun RestClient.makeRequest(
    method: String,
    endpoint: String,
    params: Map<String, String> = emptyMap(),
    entity: HttpEntity? = null,
    vararg headers: Header
): Response {
    val request = Request(method, endpoint)
    // TODO: remove PERMISSIVE option after moving system index access to REST API call
    val options = RequestOptions.DEFAULT.toBuilder()
    options.setWarningsHandler(WarningsHandler.PERMISSIVE)
    headers.forEach { options.addHeader(it.name, it.value) }
    request.options = options.build()
    params.forEach { request.addParameter(it.key, it.value) }
    if (entity != null) {
        request.entity = entity
    }
    return performRequest(request)
}

/**
 * Wrapper for [RestClient.performRequest] which was deprecated in ES 6.5 and is used in tests. This provides
 * a single place to suppress deprecation warnings. This will probably need further work when the API is removed entirely
 * but that's an exercise for another day.
 */
@Suppress("DEPRECATION")
fun RestClient.makeRequest(
    method: String,
    endpoint: String,
    entity: HttpEntity? = null,
    vararg headers: Header
): Response {
    val request = Request(method, endpoint)
    val options = RequestOptions.DEFAULT.toBuilder()
    // TODO: remove PERMISSIVE option after moving system index access to REST API call
    options.setWarningsHandler(WarningsHandler.PERMISSIVE)
    headers.forEach { options.addHeader(it.name, it.value) }
    request.options = options.build()
    if (entity != null) {
        request.entity = entity
    }
    return performRequest(request)
}

fun builder(): XContentBuilder {
    return XContentBuilder.builder(XContentType.JSON.xContent())
}

fun parser(xc: String): XContentParser {
    val parser = XContentType.JSON.xContent().createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, xc)
    parser.nextToken()
    return parser
}

fun xContentRegistry(): NamedXContentRegistry {
    return NamedXContentRegistry(
        listOf(
            SearchInput.XCONTENT_REGISTRY
        ) +
            SearchModule(Settings.EMPTY, false, emptyList()).namedXContents
    )
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import junit.framework.TestCase.assertNull
import org.apache.http.Header
import org.apache.http.HttpEntity
import org.opensearch.alerting.aggregation.bucketselectorext.BucketSelectorExtAggregationBuilder
import org.opensearch.alerting.aggregation.bucketselectorext.BucketSelectorExtFilter
import org.opensearch.alerting.core.model.ClusterMetricsInput
import org.opensearch.alerting.core.model.DocLevelMonitorInput
import org.opensearch.alerting.core.model.DocLevelQuery
import org.opensearch.alerting.core.model.Input
import org.opensearch.alerting.core.model.IntervalSchedule
import org.opensearch.alerting.core.model.Schedule
import org.opensearch.alerting.core.model.SearchInput
import org.opensearch.alerting.model.ActionExecutionResult
import org.opensearch.alerting.model.ActionRunResult
import org.opensearch.alerting.model.AggregationResultBucket
import org.opensearch.alerting.model.Alert
import org.opensearch.alerting.model.BucketLevelTrigger
import org.opensearch.alerting.model.BucketLevelTriggerRunResult
import org.opensearch.alerting.model.DocumentLevelTrigger
import org.opensearch.alerting.model.DocumentLevelTriggerRunResult
import org.opensearch.alerting.model.Finding
import org.opensearch.alerting.model.InputRunResults
import org.opensearch.alerting.model.Monitor
import org.opensearch.alerting.model.MonitorRunResult
import org.opensearch.alerting.model.QueryLevelTrigger
import org.opensearch.alerting.model.QueryLevelTriggerRunResult
import org.opensearch.alerting.model.Trigger
import org.opensearch.alerting.model.action.Action
import org.opensearch.alerting.model.action.ActionExecutionPolicy
import org.opensearch.alerting.model.action.ActionExecutionScope
import org.opensearch.alerting.model.action.AlertCategory
import org.opensearch.alerting.model.action.PerAlertActionScope
import org.opensearch.alerting.model.action.PerExecutionActionScope
import org.opensearch.alerting.model.action.Throttle
import org.opensearch.alerting.model.destination.email.EmailAccount
import org.opensearch.alerting.model.destination.email.EmailEntry
import org.opensearch.alerting.model.destination.email.EmailGroup
import org.opensearch.alerting.opensearchapi.string
import org.opensearch.alerting.util.getBucketKeysHash
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
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.authuser.User
import org.opensearch.index.query.QueryBuilders
import org.opensearch.script.Script
import org.opensearch.script.ScriptType
import org.opensearch.search.SearchModule
import org.opensearch.search.aggregations.bucket.terms.IncludeExclude
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.test.OpenSearchTestCase.randomBoolean
import org.opensearch.test.OpenSearchTestCase.randomInt
import org.opensearch.test.OpenSearchTestCase.randomIntBetween
import org.opensearch.test.rest.OpenSearchRestTestCase
import java.time.Instant
import java.time.temporal.ChronoUnit

fun randomQueryLevelMonitor(
    name: String = OpenSearchRestTestCase.randomAlphaOfLength(10),
    user: User = randomUser(),
    inputs: List<Input> = listOf(SearchInput(emptyList(), SearchSourceBuilder().query(QueryBuilders.matchAllQuery()))),
    schedule: Schedule = IntervalSchedule(interval = 5, unit = ChronoUnit.MINUTES),
    enabled: Boolean = randomBoolean(),
    triggers: List<Trigger> = (1..randomInt(10)).map { randomQueryLevelTrigger() },
    enabledTime: Instant? = if (enabled) Instant.now().truncatedTo(ChronoUnit.MILLIS) else null,
    lastUpdateTime: Instant = Instant.now().truncatedTo(ChronoUnit.MILLIS),
    withMetadata: Boolean = false
): Monitor {
    return Monitor(
        name = name, monitorType = Monitor.MonitorType.QUERY_LEVEL_MONITOR, enabled = enabled, inputs = inputs,
        schedule = schedule, triggers = triggers, enabledTime = enabledTime, lastUpdateTime = lastUpdateTime, user = user,
        uiMetadata = if (withMetadata) mapOf("foo" to "bar") else mapOf()
    )
}

// Monitor of older versions without security.
fun randomQueryLevelMonitorWithoutUser(
    name: String = OpenSearchRestTestCase.randomAlphaOfLength(10),
    inputs: List<Input> = listOf(SearchInput(emptyList(), SearchSourceBuilder().query(QueryBuilders.matchAllQuery()))),
    schedule: Schedule = IntervalSchedule(interval = 5, unit = ChronoUnit.MINUTES),
    enabled: Boolean = randomBoolean(),
    triggers: List<Trigger> = (1..randomInt(10)).map { randomQueryLevelTrigger() },
    enabledTime: Instant? = if (enabled) Instant.now().truncatedTo(ChronoUnit.MILLIS) else null,
    lastUpdateTime: Instant = Instant.now().truncatedTo(ChronoUnit.MILLIS),
    withMetadata: Boolean = false
): Monitor {
    return Monitor(
        name = name, monitorType = Monitor.MonitorType.QUERY_LEVEL_MONITOR, enabled = enabled, inputs = inputs,
        schedule = schedule, triggers = triggers, enabledTime = enabledTime, lastUpdateTime = lastUpdateTime, user = null,
        uiMetadata = if (withMetadata) mapOf("foo" to "bar") else mapOf()
    )
}

fun randomBucketLevelMonitor(
    name: String = OpenSearchRestTestCase.randomAlphaOfLength(10),
    user: User = randomUser(),
    inputs: List<Input> = listOf(
        SearchInput(
            emptyList(),
            SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
                .aggregation(TermsAggregationBuilder("test_agg").field("test_field"))
        )
    ),
    schedule: Schedule = IntervalSchedule(interval = 5, unit = ChronoUnit.MINUTES),
    enabled: Boolean = randomBoolean(),
    triggers: List<Trigger> = (1..randomInt(10)).map { randomBucketLevelTrigger() },
    enabledTime: Instant? = if (enabled) Instant.now().truncatedTo(ChronoUnit.MILLIS) else null,
    lastUpdateTime: Instant = Instant.now().truncatedTo(ChronoUnit.MILLIS),
    withMetadata: Boolean = false
): Monitor {
    return Monitor(
        name = name, monitorType = Monitor.MonitorType.BUCKET_LEVEL_MONITOR, enabled = enabled, inputs = inputs,
        schedule = schedule, triggers = triggers, enabledTime = enabledTime, lastUpdateTime = lastUpdateTime, user = user,
        uiMetadata = if (withMetadata) mapOf("foo" to "bar") else mapOf()
    )
}

fun randomClusterMetricsMonitor(
    name: String = OpenSearchRestTestCase.randomAlphaOfLength(10),
    user: User = randomUser(),
    inputs: List<Input> = listOf(randomClusterMetricsInput()),
    schedule: Schedule = IntervalSchedule(interval = 5, unit = ChronoUnit.MINUTES),
    enabled: Boolean = randomBoolean(),
    triggers: List<Trigger> = (1..randomInt(10)).map { randomQueryLevelTrigger() },
    enabledTime: Instant? = if (enabled) Instant.now().truncatedTo(ChronoUnit.MILLIS) else null,
    lastUpdateTime: Instant = Instant.now().truncatedTo(ChronoUnit.MILLIS),
    withMetadata: Boolean = false
): Monitor {
    return Monitor(
        name = name, monitorType = Monitor.MonitorType.CLUSTER_METRICS_MONITOR, enabled = enabled, inputs = inputs,
        schedule = schedule, triggers = triggers, enabledTime = enabledTime, lastUpdateTime = lastUpdateTime, user = user,
        uiMetadata = if (withMetadata) mapOf("foo" to "bar") else mapOf()
    )
}

fun randomDocumentLevelMonitor(
    name: String = OpenSearchRestTestCase.randomAlphaOfLength(10),
    user: User? = randomUser(),
    inputs: List<Input> = listOf(DocLevelMonitorInput("description", listOf("index"), emptyList())),
    schedule: Schedule = IntervalSchedule(interval = 5, unit = ChronoUnit.MINUTES),
    enabled: Boolean = randomBoolean(),
    triggers: List<Trigger> = (1..randomInt(10)).map { randomQueryLevelTrigger() },
    enabledTime: Instant? = if (enabled) Instant.now().truncatedTo(ChronoUnit.MILLIS) else null,
    lastUpdateTime: Instant = Instant.now().truncatedTo(ChronoUnit.MILLIS),
    withMetadata: Boolean = false
): Monitor {
    return Monitor(
        name = name, monitorType = Monitor.MonitorType.DOC_LEVEL_MONITOR, enabled = enabled, inputs = inputs,
        schedule = schedule, triggers = triggers, enabledTime = enabledTime, lastUpdateTime = lastUpdateTime, user = user,
        uiMetadata = if (withMetadata) mapOf("foo" to "bar") else mapOf()
    )
}

fun randomQueryLevelTrigger(
    id: String = UUIDs.base64UUID(),
    name: String = OpenSearchRestTestCase.randomAlphaOfLength(10),
    severity: String = "1",
    condition: Script = randomScript(),
    actions: List<Action> = mutableListOf(),
    destinationId: String = ""
): QueryLevelTrigger {
    return QueryLevelTrigger(
        id = id,
        name = name,
        severity = severity,
        condition = condition,
        actions = if (actions.isEmpty()) (0..randomInt(10)).map { randomAction(destinationId = destinationId) } else actions
    )
}

fun randomBucketLevelTrigger(
    id: String = UUIDs.base64UUID(),
    name: String = OpenSearchRestTestCase.randomAlphaOfLength(10),
    severity: String = "1",
    bucketSelector: BucketSelectorExtAggregationBuilder = randomBucketSelectorExtAggregationBuilder(name = id),
    actions: List<Action> = mutableListOf(),
    destinationId: String = ""
): BucketLevelTrigger {
    return BucketLevelTrigger(
        id = id,
        name = name,
        severity = severity,
        bucketSelector = bucketSelector,
        actions = if (actions.isEmpty()) randomActionsForBucketLevelTrigger(destinationId = destinationId) else actions
    )
}

fun randomActionsForBucketLevelTrigger(min: Int = 0, max: Int = 10, destinationId: String = ""): List<Action> =
    (min..randomInt(max)).map { randomActionWithPolicy(destinationId = destinationId) }

fun randomDocumentLevelTrigger(
    id: String = UUIDs.base64UUID(),
    name: String = OpenSearchRestTestCase.randomAlphaOfLength(10),
    severity: String = "1",
    condition: Script = randomScript(),
    actions: List<Action> = mutableListOf(),
    destinationId: String = ""
): DocumentLevelTrigger {
    return DocumentLevelTrigger(
        id = id,
        name = name,
        severity = severity,
        condition = condition,
        actions = if (actions.isEmpty() && destinationId.isNotBlank())
            (0..randomInt(10)).map { randomAction(destinationId = destinationId) }
        else actions
    )
}

fun randomBucketSelectorExtAggregationBuilder(
    name: String = OpenSearchRestTestCase.randomAlphaOfLength(10),
    bucketsPathsMap: MutableMap<String, String> = mutableMapOf("avg" to "10"),
    script: Script = randomBucketSelectorScript(params = bucketsPathsMap),
    parentBucketPath: String = "testPath",
    filter: BucketSelectorExtFilter = BucketSelectorExtFilter(IncludeExclude("foo*", "bar*"))
): BucketSelectorExtAggregationBuilder {
    return BucketSelectorExtAggregationBuilder(name, bucketsPathsMap, script, parentBucketPath, filter)
}

fun randomBucketSelectorScript(
    idOrCode: String = "params.avg >= 0",
    params: Map<String, String> = mutableMapOf("avg" to "10")
): Script {
    return Script(Script.DEFAULT_SCRIPT_TYPE, Script.DEFAULT_SCRIPT_LANG, idOrCode, emptyMap<String, String>(), params)
}

fun randomEmailAccount(
    salt: String = "",
    name: String = salt + OpenSearchRestTestCase.randomAlphaOfLength(10),
    email: String = salt + OpenSearchRestTestCase.randomAlphaOfLength(5) + "@email.com",
    host: String = salt + OpenSearchRestTestCase.randomAlphaOfLength(10),
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
    salt: String = "",
    name: String = salt + OpenSearchRestTestCase.randomAlphaOfLength(10),
    emails: List<EmailEntry> = (1..randomInt(10)).map {
        EmailEntry(email = salt + OpenSearchRestTestCase.randomAlphaOfLength(5) + "@email.com")
    }
): EmailGroup {
    return EmailGroup(name = name, emails = emails)
}

fun randomScript(source: String = "return " + OpenSearchRestTestCase.randomBoolean().toString()): Script = Script(source)

val ADMIN = "admin"
val ALERTING_BASE_URI = "/_plugins/_alerting/monitors"
val DESTINATION_BASE_URI = "/_plugins/_alerting/destinations"
val LEGACY_OPENDISTRO_ALERTING_BASE_URI = "/_opendistro/_alerting/monitors"
val LEGACY_OPENDISTRO_DESTINATION_BASE_URI = "/_opendistro/_alerting/destinations"
val ALWAYS_RUN = Script("return true")
val NEVER_RUN = Script("return false")
val DRYRUN_MONITOR = mapOf("dryrun" to "true")
val TEST_HR_INDEX = "hr_data"
val TEST_NON_HR_INDEX = "not_hr_data"
val TEST_HR_ROLE = "hr_role"
val TEST_HR_BACKEND_ROLE = "HR"
// Using a triple-quote string for the query so escaped quotes are kept as-is
// in the request made using triple-quote strings (i.e. createIndexRoleWithDocLevelSecurity).
// Removing the escape slash in the request causes the security API role request to fail with parsing exception.
val TERM_DLS_QUERY = """{\"term\": { \"accessible\": true}}"""

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
) = Action(name, destinationId, template, template, throttleEnabled, throttle, actionExecutionPolicy = null)

fun randomActionWithPolicy(
    name: String = OpenSearchRestTestCase.randomUnicodeOfLength(10),
    template: Script = randomTemplateScript("Hello World"),
    destinationId: String = "",
    throttleEnabled: Boolean = false,
    throttle: Throttle = randomThrottle(),
    actionExecutionPolicy: ActionExecutionPolicy? = randomActionExecutionPolicy()
): Action {
    return if (actionExecutionPolicy?.actionExecutionScope is PerExecutionActionScope) {
        // Return null for throttle when using PerExecutionActionScope since throttling is currently not supported for it
        Action(name, destinationId, template, template, throttleEnabled, null, actionExecutionPolicy = actionExecutionPolicy)
    } else {
        Action(name, destinationId, template, template, throttleEnabled, throttle, actionExecutionPolicy = actionExecutionPolicy)
    }
}

fun randomThrottle(
    value: Int = randomIntBetween(60, 120),
    unit: ChronoUnit = ChronoUnit.MINUTES
) = Throttle(value, unit)

fun randomActionExecutionPolicy(
    actionExecutionScope: ActionExecutionScope = randomActionExecutionScope()
) = ActionExecutionPolicy(actionExecutionScope)

fun randomActionExecutionScope(): ActionExecutionScope {
    return if (randomBoolean()) {
        val alertCategories = AlertCategory.values()
        PerAlertActionScope(actionableAlerts = (1..randomInt(alertCategories.size)).map { alertCategories[it - 1] }.toSet())
    } else {
        PerExecutionActionScope()
    }
}

fun randomAlert(monitor: Monitor = randomQueryLevelMonitor()): Alert {
    val trigger = randomQueryLevelTrigger()
    val actionExecutionResults = mutableListOf(randomActionExecutionResult(), randomActionExecutionResult())
    return Alert(
        monitor, trigger, Instant.now().truncatedTo(ChronoUnit.MILLIS), null,
        actionExecutionResults = actionExecutionResults
    )
}

fun randomDocLevelQuery(
    id: String = OpenSearchRestTestCase.randomAlphaOfLength(10),
    query: String = OpenSearchRestTestCase.randomAlphaOfLength(10),
    name: String = "${randomInt(5)}",
    tags: List<String> = mutableListOf(0..randomInt(10)).map { OpenSearchRestTestCase.randomAlphaOfLength(10) }
): DocLevelQuery {
    return DocLevelQuery(id = id, query = query, name = name, tags = tags)
}

fun randomDocLevelMonitorInput(
    description: String = OpenSearchRestTestCase.randomAlphaOfLength(randomInt(10)),
    indices: List<String> = listOf(1..randomInt(10)).map { OpenSearchRestTestCase.randomAlphaOfLength(10) },
    queries: List<DocLevelQuery> = listOf(1..randomInt(10)).map { randomDocLevelQuery() }
): DocLevelMonitorInput {
    return DocLevelMonitorInput(description = description, indices = indices, queries = queries)
}

fun randomFinding(
    id: String = OpenSearchRestTestCase.randomAlphaOfLength(10),
    relatedDocIds: List<String> = listOf(OpenSearchRestTestCase.randomAlphaOfLength(10)),
    monitorId: String = OpenSearchRestTestCase.randomAlphaOfLength(10),
    monitorName: String = OpenSearchRestTestCase.randomAlphaOfLength(10),
    index: String = OpenSearchRestTestCase.randomAlphaOfLength(10),
    docLevelQueries: List<DocLevelQuery> = listOf(randomDocLevelQuery()),
    timestamp: Instant = Instant.now()
): Finding {
    return Finding(
        id = id,
        relatedDocIds = relatedDocIds,
        monitorId = monitorId,
        monitorName = monitorName,
        index = index,
        docLevelQueries = docLevelQueries,
        timestamp = timestamp
    )
}

fun randomAlertWithAggregationResultBucket(monitor: Monitor = randomBucketLevelMonitor()): Alert {
    val trigger = randomBucketLevelTrigger()
    val actionExecutionResults = mutableListOf(randomActionExecutionResult(), randomActionExecutionResult())
    return Alert(
        monitor, trigger, Instant.now().truncatedTo(ChronoUnit.MILLIS), null,
        actionExecutionResults = actionExecutionResults,
        aggregationResultBucket = AggregationResultBucket(
            "parent_bucket_path_1",
            listOf("bucket_key_1"),
            mapOf("k1" to "val1", "k2" to "val2")
        )
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

fun randomQueryLevelMonitorRunResult(): MonitorRunResult<QueryLevelTriggerRunResult> {
    val triggerResults = mutableMapOf<String, QueryLevelTriggerRunResult>()
    val triggerRunResult = randomQueryLevelTriggerRunResult()
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

fun randomBucketLevelMonitorRunResult(): MonitorRunResult<BucketLevelTriggerRunResult> {
    val triggerResults = mutableMapOf<String, BucketLevelTriggerRunResult>()
    val triggerRunResult = randomBucketLevelTriggerRunResult()
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

fun randomDocumentLevelMonitorRunResult(): MonitorRunResult<DocumentLevelTriggerRunResult> {
    val triggerResults = mutableMapOf<String, DocumentLevelTriggerRunResult>()
    val triggerRunResult = randomDocumentLevelTriggerRunResult()
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

fun randomQueryLevelTriggerRunResult(): QueryLevelTriggerRunResult {
    val map = mutableMapOf<String, ActionRunResult>()
    map.plus(Pair("key1", randomActionRunResult()))
    map.plus(Pair("key2", randomActionRunResult()))
    return QueryLevelTriggerRunResult("trigger-name", true, null, map)
}

fun randomClusterMetricsInput(
    path: String = ClusterMetricsInput.ClusterMetricType.CLUSTER_HEALTH.defaultPath,
    pathParams: String = "",
    url: String = ""
): ClusterMetricsInput {
    return ClusterMetricsInput(path, pathParams, url)
}

fun randomBucketLevelTriggerRunResult(): BucketLevelTriggerRunResult {
    val map = mutableMapOf<String, ActionRunResult>()
    map.plus(Pair("key1", randomActionRunResult()))
    map.plus(Pair("key2", randomActionRunResult()))

    val aggBucket1 = AggregationResultBucket(
        "parent_bucket_path_1",
        listOf("bucket_key_1"),
        mapOf("k1" to "val1", "k2" to "val2")
    )
    val aggBucket2 = AggregationResultBucket(
        "parent_bucket_path_2",
        listOf("bucket_key_2"),
        mapOf("k1" to "val1", "k2" to "val2")
    )

    val actionResultsMap: MutableMap<String, MutableMap<String, ActionRunResult>> = mutableMapOf()
    actionResultsMap[aggBucket1.getBucketKeysHash()] = map
    actionResultsMap[aggBucket2.getBucketKeysHash()] = map

    return BucketLevelTriggerRunResult(
        "trigger-name", null,
        mapOf(
            aggBucket1.getBucketKeysHash() to aggBucket1,
            aggBucket2.getBucketKeysHash() to aggBucket2
        ),
        actionResultsMap
    )
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

fun Monitor.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun Monitor.toJsonStringWithUser(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContentWithUser(builder, ToXContent.EMPTY_PARAMS).string()
}

fun Alert.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun randomUser(): User {
    return User(
        OpenSearchRestTestCase.randomAlphaOfLength(10),
        listOf(
            OpenSearchRestTestCase.randomAlphaOfLength(10),
            OpenSearchRestTestCase.randomAlphaOfLength(10)
        ),
        listOf(OpenSearchRestTestCase.randomAlphaOfLength(10), ALL_ACCESS_ROLE),
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
            SearchInput.XCONTENT_REGISTRY,
            DocLevelMonitorInput.XCONTENT_REGISTRY,
            QueryLevelTrigger.XCONTENT_REGISTRY,
            BucketLevelTrigger.XCONTENT_REGISTRY,
            DocumentLevelTrigger.XCONTENT_REGISTRY
        ) + SearchModule(Settings.EMPTY, emptyList()).namedXContents
    )
}

fun assertUserNull(map: Map<String, Any?>) {
    val user = map["user"]
    assertNull("User is not null", user)
}

fun assertUserNull(monitor: Monitor) {
    assertNull("User is not null", monitor.user)
}

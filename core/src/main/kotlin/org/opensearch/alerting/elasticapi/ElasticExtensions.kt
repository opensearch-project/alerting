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

package org.opensearch.alerting.elasticapi

import kotlinx.coroutines.ThreadContextElement
import kotlinx.coroutines.delay
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.opensearch.OpenSearchException
import org.opensearch.action.ActionListener
import org.opensearch.action.bulk.BackoffPolicy
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.search.ShardSearchFailure
import org.opensearch.alerting.core.model.ScheduledJob
import org.opensearch.client.OpenSearchClient
import org.opensearch.common.bytes.BytesReference
import org.opensearch.common.settings.Settings
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.common.util.concurrent.ThreadContext.StoredContext
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParserUtils
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.InjectSecurity
import org.opensearch.commons.authuser.User
import org.opensearch.index.query.BoolQueryBuilder
import org.opensearch.index.query.QueryBuilders
import org.opensearch.rest.RestStatus
import org.opensearch.rest.RestStatus.BAD_GATEWAY
import org.opensearch.rest.RestStatus.GATEWAY_TIMEOUT
import org.opensearch.rest.RestStatus.SERVICE_UNAVAILABLE
import org.opensearch.search.builder.SearchSourceBuilder
import java.time.Instant
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

private val logger = LogManager.getLogger(ScheduledJob::javaClass)

/** Convert an object to maps and lists representation */
fun ToXContent.convertToMap(): Map<String, Any> {
    val bytesReference = XContentHelper.toXContent(this, XContentType.JSON, false)
    return XContentHelper.convertToMap(bytesReference, false, XContentType.JSON).v2()
}

/**
 * Backs off and retries a lambda that makes a request. This should not be called on any of the [standard][ThreadPool]
 * executors since those executors are not meant to be blocked by sleeping.
 */
fun <T> BackoffPolicy.retry(block: () -> T): T {
    val iter = iterator()
    do {
        try {
            return block()
        } catch (e: OpenSearchException) {
            if (iter.hasNext() && e.isRetriable()) {
                Thread.sleep(iter.next().millis)
            } else {
                throw e
            }
        }
    } while (true)
}

/**
 * Backs off and retries a lambda that makes a request. This retries on any Exception unless it detects the
 * Notification plugin is not installed. This should not be called on any of the [standard][ThreadPool] executors
 * since those executors are not meant to be blocked by sleeping.
 */
fun <T> BackoffPolicy.retryForNotification(block: () -> T): T {
    val iter = iterator()
    do {
        try {
            return block()
        } catch (e: java.lang.Exception) {
            val isMissingNotificationPlugin = e.message?.contains("failed to find action") ?: false
            if (isMissingNotificationPlugin) {
                throw OpenSearchException("Notification plugin is not installed. Please install the Notification plugin.", e)
            } else if (iter.hasNext()) {
                Thread.sleep(iter.next().millis)
            } else {
                throw e
            }
        }
    } while (true)
}

/**
 * Retries the given [block] of code as specified by the receiver [BackoffPolicy], if [block] throws an [OpenSearchException]
 * that is retriable (502, 503, 504).
 *
 * If all retries fail the final exception will be rethrown. Exceptions caught during intermediate retries are
 * logged as warnings to [logger]. Similar to [org.opensearch.action.bulk.Retry], except this retries on
 * 502, 503, 504 error codes as well as 429.
 *
 * @param logger - logger used to log intermediate failures
 * @param retryOn - any additional [RestStatus] values that should be retried
 * @param block - the block of code to retry. This should be a suspend function.
 */
suspend fun <T> BackoffPolicy.retry(
    logger: Logger,
    retryOn: List<RestStatus> = emptyList(),
    block: suspend () -> T
): T {
    val iter = iterator()
    do {
        try {
            return block()
        } catch (e: OpenSearchException) {
            if (iter.hasNext() && (e.isRetriable() || retryOn.contains(e.status()))) {
                val backoff = iter.next()
                logger.warn("Operation failed. Retrying in $backoff.", e)
                delay(backoff.millis)
            } else {
                throw e
            }
        }
    } while (true)
}

/**
 * Retries on 502, 503 and 504 per elastic client's behavior: https://github.com/elastic/elasticsearch-net/issues/2061
 * 429 must be retried manually as it's not clear if it's ok to retry for requests other than Bulk requests.
 */
fun OpenSearchException.isRetriable(): Boolean {
    return (status() in listOf(BAD_GATEWAY, SERVICE_UNAVAILABLE, GATEWAY_TIMEOUT))
}

fun SearchResponse.firstFailureOrNull(): ShardSearchFailure? {
    return shardFailures?.getOrNull(0)
}

fun XContentParser.instant(): Instant? {
    return when {
        currentToken() == XContentParser.Token.VALUE_NULL -> null
        currentToken().isValue -> Instant.ofEpochMilli(longValue())
        else -> {
            XContentParserUtils.throwUnknownToken(currentToken(), tokenLocation)
            null // unreachable
        }
    }
}

fun XContentBuilder.optionalTimeField(name: String, instant: Instant?): XContentBuilder {
    if (instant == null) {
        return nullField(name)
    }
    // second name as readableName should be different than first name
    return this.timeField(name, "${name}_in_millis", instant.toEpochMilli())
}

fun XContentBuilder.optionalUserField(name: String, user: User?): XContentBuilder {
    if (user == null) {
        return nullField(name)
    }
    return this.field(name, user)
}

fun addFilter(user: User, searchSourceBuilder: SearchSourceBuilder, fieldName: String) {
    val filterBackendRoles = QueryBuilders.termsQuery(fieldName, user.backendRoles)
    val queryBuilder = searchSourceBuilder.query() as BoolQueryBuilder
    searchSourceBuilder.query(queryBuilder.filter(filterBackendRoles))
}

/**
 * Extension function for ES 6.3 and above that duplicates the ES 6.2 XContentBuilder.string() method.
 */
fun XContentBuilder.string(): String = BytesReference.bytes(this).utf8ToString()

/**
 * Converts [OpenSearchClient] methods that take a callback into a kotlin suspending function.
 *
 * @param block - a block of code that is passed an [ActionListener] that should be passed to the OpenSearch client API.
 */
suspend fun <C : OpenSearchClient, T> C.suspendUntil(block: C.(ActionListener<T>) -> Unit): T =
    suspendCoroutine { cont ->
        block(object : ActionListener<T> {
            override fun onResponse(response: T) = cont.resume(response)

            override fun onFailure(e: Exception) = cont.resumeWithException(e)
        })
    }

/**
 * Store a [ThreadContext] and restore a [ThreadContext] when the coroutine resumes on a different thread.
 *
 * @param threadContext - a [ThreadContext] instance
 */
class ElasticThreadContextElement(private val threadContext: ThreadContext) : ThreadContextElement<Unit> {

    companion object Key : CoroutineContext.Key<ElasticThreadContextElement>
    private var context: StoredContext = threadContext.newStoredContext(true)

    override val key: CoroutineContext.Key<*>
        get() = Key

    override fun restoreThreadContext(context: CoroutineContext, oldState: Unit) {
        this.context = threadContext.stashContext()
    }

    override fun updateThreadContext(context: CoroutineContext) = this.context.close()
}

class InjectorContextElement(id: String, settings: Settings, threadContext: ThreadContext, private val roles: List<String>?) :
    ThreadContextElement<Unit> {

    companion object Key : CoroutineContext.Key<InjectorContextElement>
    override val key: CoroutineContext.Key<*>
        get() = Key

    var rolesInjectorHelper = InjectSecurity(id, settings, threadContext)

    override fun updateThreadContext(context: CoroutineContext) {
        rolesInjectorHelper.injectRoles(roles)
    }

    override fun restoreThreadContext(context: CoroutineContext, oldState: Unit) {
        rolesInjectorHelper.close()
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.opensearchapi

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ThreadContextElement
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import org.apache.logging.log4j.Logger
import org.opensearch.OpenSearchException
import org.opensearch.action.ActionListener
import org.opensearch.action.bulk.BackoffPolicy
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.search.ShardSearchFailure
import org.opensearch.client.OpenSearchClient
import org.opensearch.common.settings.Settings
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.InjectSecurity
import org.opensearch.commons.authuser.User
import org.opensearch.commons.notifications.NotificationsPluginInterface
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.index.query.BoolQueryBuilder
import org.opensearch.index.query.QueryBuilders
import org.opensearch.rest.RestStatus
import org.opensearch.rest.RestStatus.BAD_GATEWAY
import org.opensearch.rest.RestStatus.GATEWAY_TIMEOUT
import org.opensearch.rest.RestStatus.SERVICE_UNAVAILABLE
import org.opensearch.search.builder.SearchSourceBuilder
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

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
 * Notification plugin is not installed.
 *
 * @param logger - logger used to log intermediate failures
 * @param block - the block of code to retry. This should be a suspend function.
 */
suspend fun <T> BackoffPolicy.retryForNotification(
    logger: Logger,
    block: suspend () -> T
): T {
    val iter = iterator()
    do {
        try {
            return block()
        } catch (e: java.lang.Exception) {
            val isMissingNotificationPlugin = e.message?.contains("failed to find action") ?: false
            if (isMissingNotificationPlugin) {
                throw OpenSearchException("Notification plugin is not installed. Please install the Notification plugin.", e)
            } else if (iter.hasNext()) {
                val backoff = iter.next()
                logger.warn("Notification operation failed. Retrying in $backoff.", e)
                delay(backoff.millis)
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

fun addFilter(user: User, searchSourceBuilder: SearchSourceBuilder, fieldName: String) {
    val filterBackendRoles = QueryBuilders.termsQuery(fieldName, user.backendRoles)
    val queryBuilder = searchSourceBuilder.query() as BoolQueryBuilder
    searchSourceBuilder.query(queryBuilder.filter(filterBackendRoles))
}

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
 * Converts [NotificationsPluginInterface] methods that take a callback into a kotlin suspending function.
 *
 * @param block - a block of code that is passed an [ActionListener] that should be passed to the NotificationsPluginInterface API.
 */
suspend fun <T> NotificationsPluginInterface.suspendUntil(block: NotificationsPluginInterface.(ActionListener<T>) -> Unit): T =
    suspendCoroutine { cont ->
        block(object : ActionListener<T> {
            override fun onResponse(response: T) = cont.resume(response)

            override fun onFailure(e: Exception) = cont.resumeWithException(e)
        })
    }

class InjectorContextElement(
    id: String,
    settings: Settings,
    threadContext: ThreadContext,
    private val roles: List<String>?,
    private val user: User? = null
) : ThreadContextElement<Unit> {

    companion object Key : CoroutineContext.Key<InjectorContextElement>
    override val key: CoroutineContext.Key<*>
        get() = Key

    var rolesInjectorHelper = InjectSecurity(id, settings, threadContext)

    override fun updateThreadContext(context: CoroutineContext) {
        rolesInjectorHelper.injectRoles(roles)
        // This is from where plugins extract backend roles. It should be passed when calling APIs of other plugins
        rolesInjectorHelper.injectUserInfo(user)
    }

    override fun restoreThreadContext(context: CoroutineContext, oldState: Unit) {
        rolesInjectorHelper.close()
    }
}

suspend fun <T> withClosableContext(
    context: InjectorContextElement,
    block: suspend CoroutineScope.() -> T
): T {
    try {
        return withContext(context) { block() }
    } finally {
        context.rolesInjectorHelper.close()
    }
}

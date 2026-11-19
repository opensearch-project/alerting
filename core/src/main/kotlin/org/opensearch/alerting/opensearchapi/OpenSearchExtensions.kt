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
import org.opensearch.action.bulk.BackoffPolicy
import org.opensearch.action.search.SearchResponse
import org.opensearch.action.search.ShardSearchFailure
import org.opensearch.alerting.core.ppl.PPLPluginInterface
import org.opensearch.common.settings.Settings
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.InjectSecurity
import org.opensearch.commons.authuser.User
import org.opensearch.commons.notifications.NotificationsPluginInterface
import org.opensearch.core.action.ActionListener
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.rest.RestStatus.BAD_GATEWAY
import org.opensearch.core.rest.RestStatus.GATEWAY_TIMEOUT
import org.opensearch.core.rest.RestStatus.SERVICE_UNAVAILABLE
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.index.query.BoolQueryBuilder
import org.opensearch.index.query.QueryBuilders
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.transport.client.OpenSearchClient
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.coroutineContext
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

/**
 * Converts [PPLPluginInterface] methods that take a callback into a kotlin suspending function.
 *
 * @param block - a block of code that is passed an [ActionListener] that should be passed to the PPLPluginInterface API.
 */
suspend fun <T> PPLPluginInterface.suspendUntil(block: PPLPluginInterface.(ActionListener<T>) -> Unit): T =
    suspendCoroutine { cont ->
        block(object : ActionListener<T> {
            override fun onResponse(response: T) = cont.resume(response)

            override fun onFailure(e: Exception) = cont.resumeWithException(e)
        })
    }

/**
 * A [ThreadContextElement] that changes the OpenSearch thread context while a coroutine runs on a thread, and puts the
 * thread's own context back when the coroutine leaves it.
 *
 * With kotlinx-coroutines 1.1.1, a [withContext] call on the same dispatcher can resume its caller inside the frame that
 * ran the block, so the element is not restored until the caller next suspends. [withOwnContextElement] therefore also
 * restores the thread's own context when the block returns, and gives an outer block of the same kind its context back.
 * Changes the block makes to the thread context are lost at each suspension. Do not nest elements of different
 * subclasses: 1.1.1 restores them in the order it applied them.
 */
abstract class OwnContextElement(
    private val threadContext: ThreadContext,
    private val preserveResponseHeaders: Boolean = false
) : ThreadContextElement<ThreadContext.StoredContext> {

    /** The context this element replaced on a thread, and how many more times it is applied there, for a child coroutine. */
    private class Applied(var own: ThreadContext.StoredContext, var depth: Int = 0)

    // Empty once the thread has its own context back
    private val applied = ThreadLocal<Applied?>()

    /** Changes the context of the current thread for the coroutine. */
    protected abstract fun applyToThread()

    override fun updateThreadContext(context: CoroutineContext): ThreadContext.StoredContext {
        val current = applied.get()
        if (current != null) {
            // A child coroutine on the same thread inherits this element; the thread already holds its context
            current.depth++
            return current.own
        }
        val saved = threadContext.newStoredContext(preserveResponseHeaders)
        applied.set(Applied(saved))
        applyToThread()
        return saved
    }

    override fun restoreThreadContext(context: CoroutineContext, oldState: ThreadContext.StoredContext) {
        // Empty when the block already returned on this thread and put the context back itself
        val current = applied.get() ?: return
        if (current.depth > 0) {
            current.depth--
            return
        }
        applied.remove()
        current.own.restore()
    }

    /**
     * Puts the current thread back to what the caller of the block expects, if this element is still applied here.
     *
     * @param outer the element of the same kind that the caller runs under, if any
     */
    internal fun exitOnCurrentThread(outer: OwnContextElement?) {
        val current = applied.get() ?: return
        applied.remove()
        if (outer == null) {
            current.own.restore()
        } else {
            // The caller runs under the outer element, which now owns this thread's own context and puts it back
            outer.takeOver(current.own)
        }
    }

    private fun takeOver(own: ThreadContext.StoredContext) {
        val current = applied.get()
        if (current == null) applied.set(Applied(own)) else current.own = own
        applyToThread()
    }
}

/**
 * Runs [block] under [element], and puts the current thread back to the caller's context when the block returns.
 */
suspend fun <T> withOwnContextElement(element: OwnContextElement, block: suspend CoroutineScope.() -> T): T {
    val outer = coroutineContext.fold<OwnContextElement?>(null) { found, e -> if (e.key == element.key && e is OwnContextElement) e else found }
    try {
        return withContext(element) { block() }
    } finally {
        element.exitOnCurrentThread(outer)
    }
}

/**
 * Runs a coroutine with the roles and the user info of a monitor, workflow or user injected into the thread context.
 */
class InjectorContextElement(
    id: String,
    settings: Settings,
    threadContext: ThreadContext,
    private val roles: List<String>?,
    private val user: User? = null
) : OwnContextElement(threadContext, preserveResponseHeaders = true) {

    companion object Key : CoroutineContext.Key<InjectorContextElement>
    override val key: CoroutineContext.Key<*>
        get() = Key

    private val rolesInjectorHelper = InjectSecurity(id, settings, threadContext)

    override fun applyToThread() {
        rolesInjectorHelper.injectRoles(roles)
        // This is from where plugins extract backend roles. It should be passed when calling APIs of other plugins
        rolesInjectorHelper.injectUserInfo(user)
    }
}

/** Runs [block] under [context], as [withOwnContextElement] does. */
suspend fun <T> withClosableContext(
    context: InjectorContextElement,
    block: suspend CoroutineScope.() -> T
): T = withOwnContextElement(context, block)

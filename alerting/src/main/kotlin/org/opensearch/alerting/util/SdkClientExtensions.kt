/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import org.opensearch.action.index.IndexRequest
import org.opensearch.action.index.IndexResponse
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.remote.metadata.client.DeleteDataObjectRequest
import org.opensearch.remote.metadata.client.DeleteDataObjectResponse
import org.opensearch.remote.metadata.client.GetDataObjectRequest
import org.opensearch.remote.metadata.client.GetDataObjectResponse
import org.opensearch.remote.metadata.client.PutDataObjectRequest
import org.opensearch.remote.metadata.client.PutDataObjectResponse
import org.opensearch.remote.metadata.client.SdkClient
import org.opensearch.remote.metadata.client.SearchDataObjectRequest
import org.opensearch.remote.metadata.client.SearchDataObjectResponse
import org.opensearch.transport.client.Client

/**
 * Wrappers around [SdkClient] async methods that run the call under a stashed [ThreadContext].
 *
 * Under the resource-sharing framework, alerting writes to internal indices (e.g. `.opendistro-alerting-config`)
 * must run without the caller's transient auth so that [org.opensearch.security.filter.SecurityFilter] doesn't
 * reject them, while the persistent `OPENDISTRO_SECURITY_AUTHENTICATED_USER` header stays readable by
 * [org.opensearch.security.resources.ResourceIndexListener] to record `createdBy`.
 *
 * Each helper stashes only while the call is dispatched, and the calling thread gets its own context back before the
 * coroutine suspends. A stash closed after the suspension would run on whichever thread the coroutine resumes on, and
 * leave a context behind on both threads. The local cluster client sends the request on the calling thread, so its
 * requests carry the stashed context; a remote backend dispatches on its own executor, and the stash does not reach it.
 */

suspend fun SdkClient.putDataObjectStashed(
    request: PutDataObjectRequest,
    threadContext: ThreadContext,
): PutDataObjectResponse = threadContext.stashContext().use { this.putDataObjectAsync(request) }.await()

/**
 * Same as [putDataObjectStashed], but dispatched under [caller], the context captured on the request thread, so the
 * security plugin records the caller as the creator of a new resource. The calling thread gets its own context back
 * before the coroutine suspends. [indexWithContext] does the same for a [Client] write.
 */
suspend fun SdkClient.putDataObjectAsCaller(
    request: PutDataObjectRequest,
    threadContext: ThreadContext,
    caller: ThreadContext.StoredContext?,
): PutDataObjectResponse {
    val own = threadContext.newStoredContext(false)
    caller?.restore()
    val response = try {
        threadContext.stashContext().use { this.putDataObjectAsync(request) }
    } finally {
        own.restore()
    }
    return response.await()
}

suspend fun SdkClient.getDataObjectStashed(
    request: GetDataObjectRequest,
    threadContext: ThreadContext,
): GetDataObjectResponse = threadContext.stashContext().use { this.getDataObjectAsync(request) }.await()

suspend fun SdkClient.searchDataObjectStashed(
    request: SearchDataObjectRequest,
    threadContext: ThreadContext,
): SearchDataObjectResponse = threadContext.stashContext().use { this.searchDataObjectAsync(request) }.await()

suspend fun SdkClient.deleteDataObjectStashed(
    request: DeleteDataObjectRequest,
    threadContext: ThreadContext,
): DeleteDataObjectResponse = threadContext.stashContext().use { this.deleteDataObjectAsync(request) }.await()

/**
 * Executes [Client.index] under [stored], the caller's context, which is set only while the request is dispatched.
 *
 * The calling thread gets its own context back before the coroutine suspends, so no thread keeps the caller's
 * context, whichever thread the coroutine resumes on. The stash before dispatch keeps the caller's persistent
 * headers, which the security plugin reads to record the resource's creator. [putDataObjectAsCaller] does the same
 * for an [SdkClient] write.
 */
suspend fun Client.indexWithContext(stored: ThreadContext.StoredContext, request: IndexRequest): IndexResponse {
    val threadContext = threadPool().threadContext
    return suspendUntil { listener ->
        val own = threadContext.newStoredContext(false)
        stored.restore()
        try {
            threadContext.stashContext().use { index(request, listener) }
        } finally {
            own.restore()
        }
    }
}

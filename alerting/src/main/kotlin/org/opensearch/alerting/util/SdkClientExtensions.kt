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
import org.opensearch.transport.client.Client

/**
 * Wrappers around [SdkClient] async methods that run the call under a stashed [ThreadContext].
 *
 * Under the resource-sharing framework, alerting writes to internal indices (e.g. `.opendistro-alerting-config`)
 * must run without the caller's transient auth so that [org.opensearch.security.filter.SecurityFilter] doesn't
 * reject them, while the persistent `OPENDISTRO_SECURITY_AUTHENTICATED_USER` header stays readable by
 * [org.opensearch.security.resources.ResourceIndexListener] to record `createdBy`.
 *
 * Each helper stashes only while the call is dispatched. The local client sends the request on the calling thread,
 * so the request carries the stashed context, and the calling thread gets its own context back before the coroutine
 * suspends. A stash closed after the suspension would run on whichever thread the coroutine resumes on, and leave a
 * context behind on both threads.
 */

suspend fun SdkClient.putDataObjectStashed(
    request: PutDataObjectRequest,
    threadContext: ThreadContext,
): PutDataObjectResponse = threadContext.stashContext().use { this.putDataObjectAsync(request) }.await()

/**
 * Same as [putDataObjectStashed], but dispatched under [caller], the context captured on the request thread, so the
 * security plugin records the caller as the creator of a new resource. The calling thread gets its own context back
 * before the coroutine suspends.
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

suspend fun SdkClient.deleteDataObjectStashed(
    request: DeleteDataObjectRequest,
    threadContext: ThreadContext,
): DeleteDataObjectResponse = threadContext.stashContext().use { this.deleteDataObjectAsync(request) }.await()

/**
 * Executes [Client.index] with the caller's [ThreadContext] stashed just before dispatch; the stash
 * is closed via the completion callback so it works across the async / coroutine-resume boundary.
 * Prefer this over `client.suspendUntil { client.index(req, it) }` when the write must run under
 * the plugin (not the caller) but the surrounding coroutine flow must retain the caller's
 * persistent auth for the security plugin's [ResourceIndexListener].
 */
suspend fun Client.indexStashed(
    request: IndexRequest,
): IndexResponse {
    val stored = this.threadPool().threadContext.stashContext()
    return try {
        this.suspendUntil { this.index(request, it) }
    } finally {
        stored.close()
    }
}

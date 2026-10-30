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
 * Wrappers around [SdkClient] async methods that preserve the security plugin's ThreadContext
 * invariants across the async boundary.
 *
 * `sdkClient.xxxAsync(...)` completes on a pool thread whose [ThreadContext] does NOT inherit the
 * caller's stash. Under the resource-sharing framework, alerting writes to internal indices (e.g.
 * `.opendistro-alerting-config`) must run without the caller's transient auth so that
 * [org.opensearch.security.filter.SecurityFilter] doesn't reject them — while the persistent
 * `OPENDISTRO_SECURITY_AUTHENTICATED_USER` header must still be readable by
 * [org.opensearch.security.resources.ResourceIndexListener] to record `createdBy`.
 *
 * Each helper here stashes right before the sdk call and restores on the completion callback via
 * `whenComplete`, mirroring flow-framework / ml-commons. Callers should invoke these instead of
 * `sdkClient.xxxAsync(...).await()` directly.
 */

suspend fun SdkClient.putDataObjectStashed(
    request: PutDataObjectRequest,
    threadContext: ThreadContext,
): PutDataObjectResponse {
    val stored = threadContext.stashContext()
    return this.putDataObjectAsync(request)
        .whenComplete { _, _ -> stored.close() }
        .await()
}

suspend fun SdkClient.getDataObjectStashed(
    request: GetDataObjectRequest,
    threadContext: ThreadContext,
): GetDataObjectResponse {
    val stored = threadContext.stashContext()
    return this.getDataObjectAsync(request)
        .whenComplete { _, _ -> stored.close() }
        .await()
}

suspend fun SdkClient.deleteDataObjectStashed(
    request: DeleteDataObjectRequest,
    threadContext: ThreadContext,
): DeleteDataObjectResponse {
    val stored = threadContext.stashContext()
    return this.deleteDataObjectAsync(request)
        .whenComplete { _, _ -> stored.close() }
        .await()
}

/**
 * Executes [Client.index] under [stored], the caller's context, which is set only while the request is dispatched.
 *
 * The calling thread gets its own context back before the coroutine suspends, so no thread keeps the caller's
 * context, whichever thread the coroutine resumes on. The stash before dispatch keeps the caller's persistent
 * headers, which the security plugin reads to record the resource's creator.
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

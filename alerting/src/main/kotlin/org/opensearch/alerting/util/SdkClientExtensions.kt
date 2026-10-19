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
 * Stash only while dispatching the async call, then restore on that same thread before awaiting.
 * Restoring on a completion or coroutine-resume thread would leak context between requests.
 */

suspend fun SdkClient.putDataObjectStashed(
    request: PutDataObjectRequest,
    threadContext: ThreadContext,
    callerContext: ThreadContext.StoredContext? = null,
): PutDataObjectResponse {
    val future = threadContext.newStoredContext(false).use {
        callerContext?.restore()
        threadContext.stashContext().use { this.putDataObjectAsync(request) }
    }
    return future.await()
}

suspend fun SdkClient.getDataObjectStashed(
    request: GetDataObjectRequest,
    threadContext: ThreadContext,
): GetDataObjectResponse {
    val future = threadContext.stashContext().use { this.getDataObjectAsync(request) }
    return future.await()
}

suspend fun SdkClient.deleteDataObjectStashed(
    request: DeleteDataObjectRequest,
    threadContext: ThreadContext,
): DeleteDataObjectResponse {
    val future = threadContext.stashContext().use { this.deleteDataObjectAsync(request) }
    return future.await()
}

/**
 * Dispatches an internal write with the caller's persistent headers but without their transients.
 * Both context scopes close on the dispatching thread, before suspension.
 */
suspend fun Client.indexStashed(
    request: IndexRequest,
    callerContext: ThreadContext.StoredContext? = null,
): IndexResponse {
    val threadContext = this.threadPool().threadContext
    return this.suspendUntil { listener ->
        threadContext.newStoredContext(false).use {
            callerContext?.restore()
            threadContext.stashContext().use { this.index(request, listener) }
        }
    }
}

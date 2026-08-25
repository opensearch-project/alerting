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

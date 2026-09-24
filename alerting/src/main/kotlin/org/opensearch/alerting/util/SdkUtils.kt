/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import java.util.concurrent.CompletionException
import java.util.concurrent.CompletionStage
import java.util.concurrent.ExecutionException
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

/**
 * Converts a [CompletionStage] to a suspend function, allowing it to be used
 * inside coroutines without blocking the thread.
 *
 * A failed [CompletionStage] reports its error wrapped in a [CompletionException] (or
 * [ExecutionException]). If we propagated that wrapper as-is, downstream handlers such as
 * [org.opensearch.commons.alerting.util.AlertingException.wrap] — which type-switches on the
 * exception to derive the REST status — would see the generic wrapper and default to
 * 500 INTERNAL_SERVER_ERROR, masking the real status (e.g. a 409 CONFLICT from a
 * [org.opensearch.index.engine.VersionConflictEngineException]). Unwrap to the underlying cause so
 * the original exception type (and its status) survives the async boundary.
 */
suspend fun <T> CompletionStage<T>.await(): T = suspendCoroutine { cont ->
    this.whenComplete { result, error ->
        if (error != null) cont.resumeWithException(error.unwrapCompletion())
        else cont.resume(result)
    }
}

/**
 * Peels [CompletionException] / [ExecutionException] wrappers off a throwable to expose the
 * original cause. Returns the throwable unchanged if it is not a completion wrapper or has no cause.
 */
private fun Throwable.unwrapCompletion(): Throwable {
    var cause: Throwable = this
    while ((cause is CompletionException || cause is ExecutionException) && cause.cause != null) {
        cause = cause.cause!!
    }
    return cause
}

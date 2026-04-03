/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import java.util.concurrent.CompletionStage
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

/**
 * Converts a [CompletionStage] to a suspend function, allowing it to be used
 * inside coroutines without blocking the thread.
 */
suspend fun <T> CompletionStage<T>.await(): T = suspendCoroutine { cont ->
    this.whenComplete { result, error ->
        if (error != null) cont.resumeWithException(error)
        else cont.resume(result)
    }
}

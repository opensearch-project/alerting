/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import kotlinx.coroutines.suspendCancellableCoroutine
import org.opensearch.OpenSearchStatusException
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.index.engine.VersionConflictEngineException
import org.opensearch.remote.metadata.client.SdkClient
import org.opensearch.remote.metadata.common.SdkClientUtils
import java.util.function.BiConsumer
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

/**
 * Suspends until an SdkClient async operation completes, with parsing and exception handling.
 *
 * @param block A lambda that receives a [BiConsumer] to handle the [CompletionStage]'s result or exception.
 * @return The parsed result of type [R] (e.g., [IndexResponse], [GetResponse]) after converting the raw response.
 * @throws IllegalArgumentException If the raw response does not have a `parser()` method or cannot be parsed to [R].
 */
suspend inline fun <Raw : Any, reified R : Any> SdkClient.suspendUntil(
    crossinline block: SdkClient.(BiConsumer<Raw, Throwable?>) -> Unit
): R {
    try {
        val rawResponse: Raw = suspendCancellableCoroutine { cont ->
            block(
                BiConsumer<Raw, Throwable?> { result, exception ->
                    if (exception != null) {
                        cont.resumeWithException(SdkClientUtils.unwrapAndConvertToException(exception))
                    } else {
                        cont.resume(result)
                    }
                }
            )
        }
        val parser = rawResponse::class.java.getMethod("parser").invoke(rawResponse) as? XContentParser
            ?: throw IllegalArgumentException("Missing parser() on ${rawResponse::class.simpleName}")
        val parsed = R::class.java.getMethod("fromXContent", XContentParser::class.java)
            .invoke(null, parser) as? R
            ?: throw IllegalArgumentException("Failed to parse response into ${R::class.simpleName}")
        return parsed
    } catch (e: Throwable) {
        throw handleException(e)
    }
}

/**
 * Centralized Method for Handling different types of exceptions,
 * transforming them into appropriate OpenSearch exceptions
 *
 * @param exception The exception to process
 * @return The appropriate matching exception found
 */
fun handleException(exception: Throwable): Throwable {
    if (isVersionConflict(exception)) {
        return OpenSearchStatusException(
            exception.message ?: "Version conflict occurred",
            RestStatus.CONFLICT
        )
    }
    return exception
}

/**
 * Checks if an exception represents a version conflict.
 *
 * @param exception The exception to check.
 * @return True if the exception or its cause is a [VersionConflictEngineException], false otherwise.
 */
private fun isVersionConflict(exception: Throwable): Boolean {
    val cause = exception.cause ?: exception
    return cause is VersionConflictEngineException ||
        (cause is OpenSearchStatusException && cause.cause is VersionConflictEngineException)
}

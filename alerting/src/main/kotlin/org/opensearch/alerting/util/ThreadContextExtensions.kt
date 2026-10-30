/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.core.action.ActionListener
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

/**
 * Dispatches a listener-based call under [caller], the context captured on the request thread, and suspends until it
 * completes.
 *
 * The context is switched only while [dispatch] runs, so the calling thread gets its own context back before the
 * coroutine suspends, whichever thread it resumes on.
 */
suspend fun <T> ThreadContext.suspendAsCaller(caller: ThreadContext.StoredContext?, dispatch: (ActionListener<T>) -> Unit): T =
    suspendCoroutine { cont ->
        stashContext().use {
            caller?.restore()
            dispatch(
                object : ActionListener<T> {
                    override fun onResponse(response: T) = cont.resume(response)
                    override fun onFailure(e: Exception) = cont.resumeWithException(e)
                }
            )
        }
    }

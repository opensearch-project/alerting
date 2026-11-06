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
 * Dispatches a listener-based call under a stashed context and suspends until it completes.
 *
 * [prepare] runs after the stash and before [dispatch], to set the headers the call needs. The stash is closed right
 * after the dispatch, so the calling thread gets its own context back before the coroutine suspends, whichever thread
 * it resumes on.
 */
suspend fun <T> ThreadContext.suspendStashed(prepare: () -> Unit = {}, dispatch: (ActionListener<T>) -> Unit): T =
    suspendCoroutine { cont ->
        stashContext().use {
            prepare()
            dispatch(
                object : ActionListener<T> {
                    override fun onResponse(response: T) = cont.resume(response)
                    override fun onFailure(e: Exception) = cont.resumeWithException(e)
                }
            )
        }
    }

/**
 * Dispatches a listener-based call under [caller], the context captured on the request thread, and suspends until it
 * completes. The context is switched only while [dispatch] runs, as in [suspendStashed].
 */
suspend fun <T> ThreadContext.suspendAsCaller(caller: ThreadContext.StoredContext?, dispatch: (ActionListener<T>) -> Unit): T =
    suspendStashed({ caller?.restore() }, dispatch)

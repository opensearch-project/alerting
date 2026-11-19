/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ThreadContextElement
import kotlinx.coroutines.withContext
import org.opensearch.common.settings.Settings
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.commons.InjectSecurity
import org.opensearch.commons.authuser.User
import org.opensearch.core.action.ActionListener
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

/**
 * Dispatches a listener-based call under a stashed context and suspends until it completes.
 *
 * [prepare] runs after the stash and before [dispatch], to set the headers the call needs. The stash is closed right
 * after the dispatch, so the calling thread gets its own context back before the coroutine suspends, whichever thread
 * it resumes on. [dispatch] must send the request before it returns: a call it hands to another executor does not get
 * the stashed context.
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

/**
 * Runs a suspending [block] under a stashed context set up by [prepare].
 *
 * The context is applied to each thread while it runs the block, and each thread gets its own context back when the
 * block suspends or ends, so the stashed headers stay with the block whichever threads it runs on.
 */
suspend fun <T> ThreadContext.withStashedContext(prepare: () -> Unit = {}, block: suspend CoroutineScope.() -> T): T {
    val stashed = stashContext().use {
        prepare()
        newStoredContext(false)
    }
    return withContext(StoredContextElement(this, stashed)) { block() }
}

/**
 * Puts the roles and the user info that a monitor or workflow runs with on this context, as `InjectorContextElement`
 * does, for use in the prepare step of [withStashedContext].
 *
 * @param id the monitor or workflow the injection is logged for
 * @param settings the node settings, which decide whether a user or roles are injected
 * @param roles the roles to inject
 * @param user the user whose info is injected
 */
fun ThreadContext.injectRolesAndUserInfo(id: String, settings: Settings, roles: List<String>?, user: User?) {
    val injector = InjectSecurity(id, settings, this)
    injector.injectRoles(roles)
    injector.injectUserInfo(user)
}

/** Applies [stored] to the thread that runs the coroutine, and puts that thread's own context back when it stops. */
private class StoredContextElement(
    private val threadContext: ThreadContext,
    private val stored: ThreadContext.StoredContext
) : ThreadContextElement<ThreadContext.StoredContext> {

    companion object Key : CoroutineContext.Key<StoredContextElement>

    override val key: CoroutineContext.Key<*>
        get() = Key

    override fun updateThreadContext(context: CoroutineContext): ThreadContext.StoredContext {
        val own = threadContext.newStoredContext(false)
        stored.restore()
        return own
    }

    override fun restoreThreadContext(context: CoroutineContext, oldState: ThreadContext.StoredContext) = oldState.restore()
}

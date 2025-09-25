/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.alerting.util

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionType
import org.opensearch.common.CheckedRunnable
import org.opensearch.core.action.ActionListener
import org.opensearch.core.action.ActionResponse
import org.opensearch.identity.Subject
import org.opensearch.transport.client.Client
import org.opensearch.transport.client.FilterClient

/**
 * A special client for executing transport actions as this plugin's system subject.
 */
class PluginClient : FilterClient {
    private var subject: Subject? = null

    constructor(delegate: Client) : super(delegate)

    constructor(delegate: Client, subject: Subject) : super(delegate) {
        this.subject = subject
    }

    fun setSubject(subject: Subject) {
        this.subject = subject
    }

    override fun <Request : ActionRequest?, Response : ActionResponse?> doExecute(
        action: ActionType<Response?>?,
        request: Request?,
        listener: ActionListener<Response?>?
    ) {
        checkNotNull(subject) { "PluginClient is not initialized." }
        threadPool().getThreadContext().newStoredContext(false).use { ctx ->
            subject!!.runAs<RuntimeException?>(
                CheckedRunnable {
                    Companion.logger.info(
                        "Running transport action with subject: {}",
                        subject!!.getPrincipal().getName()
                    )
                    super.doExecute<Request?, Response?>(
                        action,
                        request,
                        ActionListener.runBefore<Response?>(listener, CheckedRunnable<RuntimeException> { ctx.restore() })
                    )
                }
            )
        }
    }

    companion object {
        private val logger: Logger = LogManager.getLogger(PluginClient::class.java)
    }
}

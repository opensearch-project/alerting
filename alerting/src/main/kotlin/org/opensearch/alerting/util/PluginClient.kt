/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionType
import org.opensearch.core.action.ActionListener
import org.opensearch.core.action.ActionResponse
import org.opensearch.identity.Subject
import org.opensearch.transport.client.Client
import org.opensearch.transport.client.FilterClient

/**
 * A special client for executing transport actions as this plugin's system subject.
 * Used to bypass user-level DLS on the resource-sharing index when the plugin needs
 * to perform internal reads (e.g., resolving subordinate resources to owning monitors).
 */
class PluginClient : FilterClient {

    private var subject: Subject? = null

    companion object {
        private val LOGGER: Logger = LogManager.getLogger(PluginClient::class.java)
    }

    constructor(delegate: Client) : super(delegate)

    constructor(delegate: Client, subject: Subject) : super(delegate) {
        this.subject = subject
    }

    fun setSubject(subject: Subject) {
        this.subject = subject
    }

    @Suppress("UNCHECKED_CAST")
    override fun <Request : ActionRequest, Response : ActionResponse> doExecute(
        action: ActionType<Response>,
        request: Request,
        listener: ActionListener<Response>
    ) {
        val currentSubject = subject
            ?: error("PluginClient is not initialized.")

        val storedContext = threadPool().threadContext.newStoredContext(false)

        try {
            currentSubject.runAs<Exception> {
                LOGGER.debug("Running transport action with subject: {}", currentSubject.principal.name)

                val wrappedListener = ActionListener.runBefore(listener) { storedContext.restore() }

                super.doExecute(action, request, wrappedListener)
            }
        } finally {
            storedContext.close()
        }
    }
}

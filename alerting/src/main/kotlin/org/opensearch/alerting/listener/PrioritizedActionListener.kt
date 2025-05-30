package org.opensearch.alerting.listener

import org.opensearch.core.action.ActionListener

interface PrioritizedActionListener<Response> : ActionListener<Response> {
    fun executeImmediately()
}
